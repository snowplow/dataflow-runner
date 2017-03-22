//
// Copyright (c) 2016-2017 Snowplow Analytics Ltd. All rights reserved.
//
// This program is licensed to you under the Apache License Version 2.0,
// and you may not use this file except in compliance with the Apache License Version 2.0.
// You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the Apache License Version 2.0 is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
//

package main

import (
	"errors"
	"math/rand"
	"strconv"
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/emr"
	"github.com/aws/aws-sdk-go/service/emr/emriface"
)

const (
	invalidStateSleepSeconds     = 30
	bootstrapFailureSleepSeconds = 300
)

// EmrCluster is used for starting and terminating clusters
type EmrCluster struct {
	Config ClusterConfig
	Svc    emriface.EMRAPI
}

// InitEmrCluster creates a new EmrCluster instance
func InitEmrCluster(clusterConfig ClusterConfig) (*EmrCluster, error) {
	creds, err := GetCredentialsProvider(
		clusterConfig.Credentials.AccessKeyId, clusterConfig.Credentials.SecretAccessKey)
	if err != nil {
		return nil, err
	}

	svc := emr.New(session.New(), &aws.Config{
		Region:      aws.String(clusterConfig.Region),
		Credentials: creds,
	})
	return &EmrCluster{
		Config: clusterConfig,
		Svc:    svc,
	}, nil
}

// TerminateJobFlow attempts to terminate a running cluster
func (ec EmrCluster) TerminateJobFlow(jobflowID string) error {
	terminateJobFlowsInput := emr.TerminateJobFlowsInput{
		JobFlowIds: []*string{aws.String(jobflowID)},
	}

	_, err := ec.Svc.TerminateJobFlows(&terminateJobFlowsInput)
	if err != nil {
		return err
	}

	log.Info("Terminating EMR cluster with jobflow id '" + jobflowID + "'...")

	_, err = ec.waitForState(jobflowID, "TERMINATED",
		[]string{"TERMINATED_WITH_ERRORS", "TERMINATED"})
	return err
}

// RunJobFlow builds the params config and launches an EMR cluster
func (ec EmrCluster) RunJobFlow() (string, error) {
	return ec.runJobFlow(bootstrapFailureSleepSeconds)
}

func (ec EmrCluster) runJobFlow(sleepTime int) (string, error) {
	params, err := ec.GetJobFlowInput()
	if err != nil {
		return "", err
	}

	var done = false
	var retry = 3
	var clusterState string
	var jobflowID string

	for done == false && retry > 0 {
		resp, err := ec.Svc.RunJobFlow(params)
		if err != nil {
			return "", err
		}

		log.Info("Launching EMR cluster with name '" + ec.Config.Name + "'...")

		clusterStatus, err := ec.waitForState(*resp.JobFlowId, "WAITING",
			[]string{"TERMINATED_WITH_ERRORS", "TERMINATED", "TERMINATING", "WAITING"})
		if err != nil {
			return "", err
		}

		if clusterStatus.StateChangeReason != nil &&
			clusterStatus.StateChangeReason.Code != nil &&
			*clusterStatus.StateChangeReason.Code == "BOOTSTRAP_FAILURE" {

			retry--

			timeout := rand.Intn(sleepTime)
			log.Error("Bootstrap failure detected, retrying in " + strconv.Itoa(timeout) + " seconds...")
			time.Sleep(time.Second * time.Duration(timeout))
		} else {
			done = true
		}

		clusterState = *clusterStatus.State
		jobflowID = *resp.JobFlowId
	}

	if retry <= 0 {
		return "", errors.New("could not start the cluster due to bootstrap failure")
	}

	if clusterState == "WAITING" {
		return jobflowID, nil
	}
	return "", errors.New("EMR cluster failed to launch with state " + clusterState)
}

// waitForState blocks waiting for the EMR cluster to enter a certain state or
// a failure exit state
func (ec EmrCluster) waitForState(jobflowID string, neededState string, exitStates []string) (*emr.ClusterStatus, error) {
	cluster := &emr.DescribeClusterInput{ClusterId: aws.String(jobflowID)}

	resp, err := ec.Svc.DescribeCluster(cluster)
	if err != nil {
		return nil, err
	}

	for !StringInSlice(*resp.Cluster.Status.State, exitStates) {
		log.Info("EMR cluster is in state " + *resp.Cluster.Status.State + " - need state " + neededState + ", checking again in " + strconv.Itoa(invalidStateSleepSeconds) + " seconds...")

		time.Sleep(time.Second * invalidStateSleepSeconds)

		resp, err = ec.Svc.DescribeCluster(cluster)
		if err != nil {
			return nil, err
		}
	}

	return resp.Cluster.Status, nil
}

// --- Parameter builders

// GetJobFlowInput parses the ClusterRecord and returns a
// RunJobFlowInput object which can launch an EMR cluster
func (ec EmrCluster) GetJobFlowInput() (*emr.RunJobFlowInput, error) {
	ec2 := ec.Config.Ec2

	ec2Subnet, placement, err := ec.GetLocation()
	if err != nil {
		return nil, err
	}

	// JobFlowInstancesConfig set
	instances := &emr.JobFlowInstancesConfig{
		Ec2KeyName:     aws.String(ec.Config.Ec2.KeyName),
		Ec2SubnetId:    aws.String(ec2Subnet),
		InstanceGroups: ec.GetInstanceGroups(),
		Placement: &emr.PlacementType{
			AvailabilityZone: aws.String(placement),
		},
		KeepJobFlowAliveWhenNoSteps: aws.Bool(true),
	}

	applications, err := ec.GetApplications()
	if err != nil {
		return nil, err
	}

	// RunJobFlowInput configs set
	params := &emr.RunJobFlowInput{
		Instances:         instances,
		Name:              aws.String(ec.Config.Name),
		JobFlowRole:       aws.String(ec.Config.Roles.Jobflow),
		ServiceRole:       aws.String(ec.Config.Roles.Service),
		LogUri:            aws.String(ec.Config.LogUri),
		Tags:              ec.GetTags(),
		BootstrapActions:  ec.GetBootstrapActions(),
		Configurations:    ec.GetConfigurations(),
		VisibleToAllUsers: aws.Bool(true),
		Applications:      applications,
	}

	// Check to see if version < 4.x
	majorVersion, err := ec.GetAmiVersionMajor()
	if err != nil {
		return nil, err
	}

	if majorVersion < 4 {
		params.AmiVersion = aws.String(ec2.AmiVersion)
	} else {
		params.ReleaseLabel = aws.String("emr-" + ec2.AmiVersion)
	}

	return params, nil
}

// GetLocation figures out where the EMR Cluster is going to be placed,
// either in a classic VPC or within a created subnet
func (ec EmrCluster) GetLocation() (string, string, error) {
	location := ec.Config.Ec2.Location

	ec2Subnet := ""
	placement := ""

	if location.Vpc != nil && location.Classic != nil {
		return "", "", errors.New("Only one of Availability Zone and Subnet id should be provided")
	} else if location.Vpc != nil {
		ec2Subnet = location.Vpc.SubnetId
	} else if location.Classic != nil {
		placement = location.Classic.AvailabilityZone
	} else {
		return "", "", errors.New("At least one of Availability Zone and Subnet id is required")
	}

	return ec2Subnet, placement, nil
}

// GetInstanceGroups builds the instance groups array
func (ec EmrCluster) GetInstanceGroups() []*emr.InstanceGroupConfig {
	instances := ec.Config.Ec2.Instances

	var instanceGroups = []*emr.InstanceGroupConfig{
		{
			InstanceCount: aws.Int64(1),
			InstanceRole:  aws.String("MASTER"),
			InstanceType:  aws.String(instances.Master.Type),
		},
		{
			InstanceCount: aws.Int64(instances.Core.Count),
			InstanceRole:  aws.String("CORE"),
			InstanceType:  aws.String(instances.Core.Type),
		},
		{
			InstanceCount: aws.Int64(instances.Task.Count),
			InstanceRole:  aws.String("TASK"),
			InstanceType:  aws.String(instances.Task.Type),
		},
	}

	// If task instance bid is provided setting the BidPrice  for the task instance
	if instances.Task.Bid != "" {
		instanceGroups[2].BidPrice = aws.String(instances.Task.Bid)

		// SPOT instance since a bid price parameter is mentioned
		instanceGroups[2].Market = aws.String("SPOT")
	}

	if instances.Master.EbsConfiguration != nil {
		instanceGroups[0].EbsConfiguration = GetEbsConfiguration(instances.Master.EbsConfiguration)
	}
	if instances.Core.EbsConfiguration != nil {
		instanceGroups[1].EbsConfiguration = GetEbsConfiguration(instances.Core.EbsConfiguration)
	}
	if instances.Task.EbsConfiguration != nil {
		instanceGroups[2].EbsConfiguration = GetEbsConfiguration(instances.Task.EbsConfiguration)
	}

	if instances.Task.Count > 0 && instances.Core.Count <= 0 {
		// Removing core config when there are no such instances
		instanceGroups = append(instanceGroups[0:1], instanceGroups[2])
	} else if instances.Core.Count > 0 && instances.Task.Count <= 0 {
		// Removing task config when there are no such instances
		instanceGroups = instanceGroups[0:2]
	} else if instances.Core.Count <= 0 && instances.Task.Count <= 0 {
		// Removing task and core configs when there are no such instances mentioned
		instanceGroups = instanceGroups[0:1]
	}

	return instanceGroups
}

// GetEbsConfiguration turns a EbsConfigurationRecord into an emr.EbsConfiguration
func GetEbsConfiguration(c *EbsConfigurationRecord) *emr.EbsConfiguration {
	configs := c.EbsBlockDeviceConfigs

	var emrConfigsArr []*emr.EbsBlockDeviceConfig

	if configs != nil && len(configs) > 0 {
		emrConfigsArr = make([]*emr.EbsBlockDeviceConfig, len(configs))

		for i, config := range configs {
			emrVolumeSpec := emr.VolumeSpecification{
				Iops:       aws.Int64(config.VolumeSpecification.Iops),
				SizeInGB:   aws.Int64(config.VolumeSpecification.SizeInGB),
				VolumeType: aws.String(config.VolumeSpecification.VolumeType),
			}

			emrConfig := emr.EbsBlockDeviceConfig{
				VolumesPerInstance:  aws.Int64(config.VolumesPerInstance),
				VolumeSpecification: &emrVolumeSpec,
			}

			emrConfigsArr[i] = &emrConfig
		}
	}

	return &emr.EbsConfiguration{
		EbsBlockDeviceConfigs: emrConfigsArr,
		EbsOptimized:          aws.Bool(c.EbsOptimized),
	}
}

// GetAmiVersionMajor returns the major AmiVersion
func (ec EmrCluster) GetAmiVersionMajor() (int, error) {
	return strconv.Atoi(string(ec.Config.Ec2.AmiVersion[0]))
}

// GetTags builds the tags array
func (ec EmrCluster) GetTags() []*emr.Tag {
	tags := ec.Config.Tags

	var emrTagsArr []*emr.Tag

	if tags != nil && len(tags) > 0 {
		emrTagsArr = make([]*emr.Tag, len(tags))

		for i, tag := range tags {
			emrTag := emr.Tag{
				Key:   aws.String(tag.Key),
				Value: aws.String(tag.Value),
			}

			emrTagsArr[i] = &emrTag
		}
	}

	return emrTagsArr
}

// GetBootstrapActions builds the bootstrap actions options
func (ec EmrCluster) GetBootstrapActions() []*emr.BootstrapActionConfig {
	bootstrapActions := ec.Config.BootstrapActionConfigs

	var emrBootstrapActionArr []*emr.BootstrapActionConfig

	if bootstrapActions != nil && len(bootstrapActions) > 0 {
		emrBootstrapActionArr = make([]*emr.BootstrapActionConfig, len(bootstrapActions))

		for i, bootstrapAction := range bootstrapActions {
			scriptBootstrapAction := bootstrapAction.ScriptBootstrapAction

			arguments := make([]*string, len(scriptBootstrapAction.Args))
			for j, argument := range scriptBootstrapAction.Args {
				arguments[j] = aws.String(argument)
			}

			emrScriptBootstrapAction := emr.ScriptBootstrapActionConfig{
				Args: arguments,
				Path: aws.String(scriptBootstrapAction.Path),
			}

			emrBootstrapAction := emr.BootstrapActionConfig{
				Name: aws.String(bootstrapAction.Name),
				ScriptBootstrapAction: &emrScriptBootstrapAction,
			}

			emrBootstrapActionArr[i] = &emrBootstrapAction
		}
	}

	return emrBootstrapActionArr
}

// GetConfigurations builds the configurations options
func (ec EmrCluster) GetConfigurations() []*emr.Configuration {
	configurations := ec.Config.Configurations

	var emrConfigurationArr []*emr.Configuration

	if configurations != nil && len(configurations) > 0 {
		emrConfigurationArr = make([]*emr.Configuration, len(configurations))

		for i, configuration := range configurations {
			propertyMap := make(map[string]*string)
			for k, v := range configuration.Properties {
				propertyMap[k] = aws.String(v)
			}

			emrConfiguration := emr.Configuration{
				Classification: aws.String(configuration.Classification),
				Properties:     propertyMap,
			}

			emrConfigurationArr[i] = &emrConfiguration
		}
	}

	return emrConfigurationArr
}

// GetApplications builds the applications options
func (ec EmrCluster) GetApplications() ([]*emr.Application, error) {
	applications := ec.Config.Applications

	var emrApplicationArr []*emr.Application
	allowedApps := []string{"Hadoop", "Hive", "Mahout", "Pig", "Spark"}

	if applications != nil && len(applications) > 0 {
		emrApplicationArr = make([]*emr.Application, len(applications))

		for i, application := range applications {
			if StringInSlice(application, allowedApps) {
				emrApplication := emr.Application{
					Name: aws.String(application),
				}

				emrApplicationArr[i] = &emrApplication
			} else {
				return nil, errors.New("Only " + strings.Join(allowedApps, ", ") +
					" are allowed applications")
			}
		}
	}

	return emrApplicationArr, nil
}
