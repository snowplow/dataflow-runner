//
// Copyright (c) 2016-2022 Snowplow Analytics Ltd. All rights reserved.
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
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/emr"
	"github.com/aws/aws-sdk-go-v2/service/emr/types"
	log "github.com/sirupsen/logrus"

	"github.com/snowplow-devops/go-retry"
)

const (
	invalidStateSleepSeconds     = 30
	bootstrapFailureSleepSeconds = 300
	clusterWaitMaxDuration       = 60 * time.Minute
)

// EMRAPI defines the interface for EMR operations (for mocking in tests)
type EMRAPI interface {
	RunJobFlow(ctx context.Context, params *emr.RunJobFlowInput, optFns ...func(*emr.Options)) (*emr.RunJobFlowOutput, error)
	TerminateJobFlows(ctx context.Context, params *emr.TerminateJobFlowsInput, optFns ...func(*emr.Options)) (*emr.TerminateJobFlowsOutput, error)
	DescribeCluster(ctx context.Context, params *emr.DescribeClusterInput, optFns ...func(*emr.Options)) (*emr.DescribeClusterOutput, error)
	AddJobFlowSteps(ctx context.Context, params *emr.AddJobFlowStepsInput, optFns ...func(*emr.Options)) (*emr.AddJobFlowStepsOutput, error)
	ListSteps(ctx context.Context, params *emr.ListStepsInput, optFns ...func(*emr.Options)) (*emr.ListStepsOutput, error)
	DescribeStep(ctx context.Context, params *emr.DescribeStepInput, optFns ...func(*emr.Options)) (*emr.DescribeStepOutput, error)
}

// EmrCluster is used for starting and terminating clusters
type EmrCluster struct {
	Config ClusterConfig
	Svc    EMRAPI
}

// InitEmrCluster creates a new EmrCluster instance
func InitEmrCluster(clusterConfig ClusterConfig) (*EmrCluster, error) {
	creds, err := GetCredentialsProvider(
		clusterConfig.Credentials.AccessKeyId, clusterConfig.Credentials.SecretAccessKey)
	if err != nil {
		return nil, err
	}

	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion(clusterConfig.Region),
		config.WithCredentialsProvider(creds),
	)
	if err != nil {
		return nil, err
	}

	svc := emr.NewFromConfig(cfg)
	return &EmrCluster{
		Config: clusterConfig,
		Svc:    svc,
	}, nil
}

// TerminateJobFlow attempts to terminate a running cluster
func (ec EmrCluster) TerminateJobFlow(jobflowID string) error {
	return ec.TerminateJobFlowWithContext(context.Background(), jobflowID)
}

// TerminateJobFlowWithContext attempts to terminate a running cluster with context support
func (ec EmrCluster) TerminateJobFlowWithContext(ctx context.Context, jobflowID string) error {
	terminateJobFlowsInput := emr.TerminateJobFlowsInput{
		JobFlowIds: []string{jobflowID},
	}

	_, err := retry.ExponentialWithInterface(3, time.Second, "emr.TerminateJobFlow", func() (interface{}, error) {
		return ec.Svc.TerminateJobFlows(ctx, &terminateJobFlowsInput)
	})
	if err != nil {
		return err
	}

	log.Infof("Terminating EMR cluster with jobflow id '%s'...", jobflowID)

	return ec.waitForClusterTerminated(ctx, jobflowID)
}

// RunJobFlow builds the params config and launches an EMR cluster
func (ec EmrCluster) RunJobFlow() (string, error) {
	return ec.RunJobFlowWithContext(context.Background())
}

// RunJobFlowWithContext builds the params config and launches an EMR cluster with context support
func (ec EmrCluster) RunJobFlowWithContext(ctx context.Context) (string, error) {
	return ec.runJobFlow(ctx, bootstrapFailureSleepSeconds)
}

func (ec EmrCluster) runJobFlow(ctx context.Context, sleepTime int) (string, error) {
	params, err := ec.GetJobFlowInput(true)
	if err != nil {
		return "", err
	}

	var done = false
	var retryCount = 3
	var clusterState string
	var jobflowID string
	var reasonCode, reasonMessage string

	for !done && retryCount > 0 {
		resp, err := retry.ExponentialWithInterface(3, time.Second, "emr.RunJobFlow", func() (interface{}, error) {
			return ec.Svc.RunJobFlow(ctx, params)
		})
		if err != nil {
			return "", err
		}

		log.Infof("Launching EMR cluster with name '%s'...", ec.Config.Name)

		runJobFlowOutput := resp.(*emr.RunJobFlowOutput)
		clusterStatus, err := ec.waitForClusterReady(ctx, *runJobFlowOutput.JobFlowId)
		if err != nil && clusterStatus == nil {
			return "", err
		}

		reasonCode, reasonMessage = clusterStateChangeReason(clusterStatus)
		if reasonCode != "" || reasonMessage != "" {
			log.Errorf("EMR cluster state change reason: code='%s' message=%q", reasonCode, reasonMessage)
		}

		if clusterStatus.StateChangeReason != nil &&
			clusterStatus.StateChangeReason.Code == types.ClusterStateChangeReasonCodeBootstrapFailure {

			retryCount--

			timeout := rand.Intn(sleepTime)
			log.Errorf("Bootstrap failure detected, retrying in %d seconds...", timeout)
			time.Sleep(time.Second * time.Duration(timeout))
		} else {
			done = true
		}

		clusterState = string(clusterStatus.State)
		jobflowID = *runJobFlowOutput.JobFlowId
	}

	if retryCount <= 0 {
		return "", fmt.Errorf("could not start the cluster due to bootstrap failure: code=%s message=%q", reasonCode, reasonMessage)
	}

	if clusterState == "WAITING" {
		return jobflowID, nil
	}
	if reasonCode != "" || reasonMessage != "" {
		return "", fmt.Errorf("EMR cluster failed to launch with state %s: code=%s message=%q", clusterState, reasonCode, reasonMessage)
	}
	return "", fmt.Errorf("EMR cluster failed to launch with state %s", clusterState)
}

// clusterStateChangeReason returns the StateChangeReason code and message, or empty strings if unset.
func clusterStateChangeReason(status *types.ClusterStatus) (string, string) {
	if status == nil || status.StateChangeReason == nil {
		return "", ""
	}
	code := string(status.StateChangeReason.Code)
	msg := ""
	if status.StateChangeReason.Message != nil {
		msg = *status.StateChangeReason.Message
	}
	return code, msg
}

// waitForClusterReady waits for the cluster to reach RUNNING or WAITING state using SDK v2 waiter.
// Returns the cluster status even on failure (needed for bootstrap failure detection).
func (ec EmrCluster) waitForClusterReady(ctx context.Context, jobflowID string) (*types.ClusterStatus, error) {
	input := &emr.DescribeClusterInput{ClusterId: aws.String(jobflowID)}

	// Validate cluster exists before starting the waiter
	resp, err := retry.ExponentialWithInterface(3, time.Second, "emr.DescribeCluster", func() (any, error) {
		return ec.Svc.DescribeCluster(ctx, input)
	})
	if err != nil {
		return nil, err
	}

	// Check if already in a terminal state
	output := resp.(*emr.DescribeClusterOutput)
	state := output.Cluster.Status.State
	if state == types.ClusterStateWaiting || state == types.ClusterStateRunning {
		return output.Cluster.Status, nil
	}
	if state == types.ClusterStateTerminated || state == types.ClusterStateTerminatedWithErrors || state == types.ClusterStateTerminating {
		return output.Cluster.Status, nil
	}

	waiter := emr.NewClusterRunningWaiter(ec.Svc, func(o *emr.ClusterRunningWaiterOptions) {
		o.MinDelay = invalidStateSleepSeconds * time.Second
		o.MaxDelay = invalidStateSleepSeconds * time.Second
	})

	waiterErr := waiter.Wait(ctx, input, clusterWaitMaxDuration)

	// Fetch final cluster status (needed for bootstrap failure detection)
	resp, err = ec.Svc.DescribeCluster(ctx, input)
	if err != nil {
		if waiterErr != nil {
			return nil, waiterErr
		}
		return nil, err
	}

	output = resp.(*emr.DescribeClusterOutput)
	return output.Cluster.Status, waiterErr
}

// waitForClusterTerminated waits for the cluster to terminate using SDK v2 waiter.
func (ec EmrCluster) waitForClusterTerminated(ctx context.Context, jobflowID string) error {
	input := &emr.DescribeClusterInput{ClusterId: aws.String(jobflowID)}

	// Validate cluster exists before starting the waiter
	resp, err := retry.ExponentialWithInterface(3, time.Second, "emr.DescribeCluster", func() (any, error) {
		return ec.Svc.DescribeCluster(ctx, input)
	})
	if err != nil {
		return err
	}

	// Check if already terminated
	output := resp.(*emr.DescribeClusterOutput)
	if output.Cluster.Status.State == types.ClusterStateTerminated ||
		output.Cluster.Status.State == types.ClusterStateTerminatedWithErrors {
		return nil
	}

	waiter := emr.NewClusterTerminatedWaiter(ec.Svc, func(o *emr.ClusterTerminatedWaiterOptions) {
		o.MinDelay = invalidStateSleepSeconds * time.Second
		o.MaxDelay = invalidStateSleepSeconds * time.Second
	})

	return waiter.Wait(ctx, input, clusterWaitMaxDuration)
}

// --- Parameter builders

// GetJobFlowInput parses the ClusterRecord and returns a
// RunJobFlowInput object which can launch an EMR cluster
func (ec EmrCluster) GetJobFlowInput(keepJobFlowAliveWhenNoSteps bool) (*emr.RunJobFlowInput, error) {
	ec2 := ec.Config.Ec2

	ec2Subnet, placement, err := ec.GetLocation()
	if err != nil {
		return nil, err
	}

	// JobFlowInstancesConfig set
	instances := &types.JobFlowInstancesConfig{
		Ec2KeyName:     aws.String(ec.Config.Ec2.KeyName),
		Ec2SubnetId:    aws.String(ec2Subnet),
		InstanceGroups: ec.GetInstanceGroups(),
		Placement: &types.PlacementType{
			AvailabilityZone: aws.String(placement),
		},
		KeepJobFlowAliveWhenNoSteps: aws.Bool(keepJobFlowAliveWhenNoSteps),
	}

	applications, err := ec.GetApplications()
	if err != nil {
		return nil, err
	}

	// RunJobFlowInput configs set
	params := &emr.RunJobFlowInput{
		Instances:             instances,
		Name:                  aws.String(ec.Config.Name),
		JobFlowRole:           aws.String(ec.Config.Roles.Jobflow),
		ServiceRole:           aws.String(ec.Config.Roles.Service),
		LogUri:                aws.String(ec.Config.LogUri),
		Tags:                  ec.GetTags(),
		BootstrapActions:      ec.GetBootstrapActions(),
		Configurations:        ec.GetConfigurations(),
		VisibleToAllUsers:     aws.Bool(true),
		Applications:          applications,
		SecurityConfiguration: aws.String(ec.Config.SecurityConfiguration),
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
		return "", "", fmt.Errorf("Only one of Availability Zone and Subnet id should be provided")
	} else if location.Vpc != nil {
		ec2Subnet = location.Vpc.SubnetId
	} else if location.Classic != nil {
		placement = location.Classic.AvailabilityZone
	} else {
		return "", "", fmt.Errorf("At least one of Availability Zone and Subnet id is required")
	}

	return ec2Subnet, placement, nil
}

// GetInstanceGroups builds the instance groups array
func (ec EmrCluster) GetInstanceGroups() []types.InstanceGroupConfig {
	instances := ec.Config.Ec2.Instances

	var instanceGroups = []types.InstanceGroupConfig{
		{
			InstanceCount: aws.Int32(1),
			InstanceRole:  types.InstanceRoleTypeMaster,
			InstanceType:  aws.String(instances.Master.Type),
		},
		{
			InstanceCount: aws.Int32(int32(instances.Core.Count)),
			InstanceRole:  types.InstanceRoleTypeCore,
			InstanceType:  aws.String(instances.Core.Type),
		},
		{
			InstanceCount: aws.Int32(int32(instances.Task.Count)),
			InstanceRole:  types.InstanceRoleTypeTask,
			InstanceType:  aws.String(instances.Task.Type),
		},
	}

	// If task instance bid is provided setting the BidPrice  for the task instance
	if instances.Task.Bid != "" {
		instanceGroups[2].BidPrice = aws.String(instances.Task.Bid)

		// SPOT instance since a bid price parameter is mentioned
		instanceGroups[2].Market = types.MarketTypeSpot
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

// GetEbsConfiguration turns a EbsConfigurationRecord into an types.EbsConfiguration
func GetEbsConfiguration(c *EbsConfigurationRecord) *types.EbsConfiguration {
	configs := c.EbsBlockDeviceConfigs

	var emrConfigsArr []types.EbsBlockDeviceConfig

	if configs != nil && len(configs) > 0 {
		emrConfigsArr = make([]types.EbsBlockDeviceConfig, len(configs))

		for i, config := range configs {
			emrVolumeSpec := &types.VolumeSpecification{
				SizeInGB:   aws.Int32(int32(config.VolumeSpecification.SizeInGB)),
				VolumeType: aws.String(config.VolumeSpecification.VolumeType),
			}
			if *emrVolumeSpec.VolumeType != "gp2" && *emrVolumeSpec.VolumeType != "gp3" {
				emrVolumeSpec.Iops = aws.Int32(int32(config.VolumeSpecification.Iops))
			}

			emrConfig := types.EbsBlockDeviceConfig{
				VolumesPerInstance:  aws.Int32(int32(config.VolumesPerInstance)),
				VolumeSpecification: emrVolumeSpec,
			}

			emrConfigsArr[i] = emrConfig
		}
	}

	return &types.EbsConfiguration{
		EbsBlockDeviceConfigs: emrConfigsArr,
		EbsOptimized:          aws.Bool(c.EbsOptimized),
	}
}

// GetAmiVersionMajor returns the major AmiVersion
func (ec EmrCluster) GetAmiVersionMajor() (int, error) {
	return strconv.Atoi(string(ec.Config.Ec2.AmiVersion[0]))
}

// GetTags builds the tags array
func (ec EmrCluster) GetTags() []types.Tag {
	tags := ec.Config.Tags

	var emrTagsArr []types.Tag

	if tags != nil && len(tags) > 0 {
		emrTagsArr = make([]types.Tag, len(tags))

		for i, tag := range tags {
			emrTag := types.Tag{
				Key:   aws.String(tag.Key),
				Value: aws.String(tag.Value),
			}

			emrTagsArr[i] = emrTag
		}
	}

	return emrTagsArr
}

// GetBootstrapActions builds the bootstrap actions options
func (ec EmrCluster) GetBootstrapActions() []types.BootstrapActionConfig {
	bootstrapActions := ec.Config.BootstrapActionConfigs

	var emrBootstrapActionArr []types.BootstrapActionConfig

	if bootstrapActions != nil && len(bootstrapActions) > 0 {
		emrBootstrapActionArr = make([]types.BootstrapActionConfig, len(bootstrapActions))

		for i, bootstrapAction := range bootstrapActions {
			scriptBootstrapAction := bootstrapAction.ScriptBootstrapAction

			arguments := make([]string, len(scriptBootstrapAction.Args))
			for j, argument := range scriptBootstrapAction.Args {
				arguments[j] = argument
			}

			emrScriptBootstrapAction := types.ScriptBootstrapActionConfig{
				Args: arguments,
				Path: aws.String(scriptBootstrapAction.Path),
			}

			emrBootstrapAction := types.BootstrapActionConfig{
				Name:                  aws.String(bootstrapAction.Name),
				ScriptBootstrapAction: &emrScriptBootstrapAction,
			}

			emrBootstrapActionArr[i] = emrBootstrapAction
		}
	}

	return emrBootstrapActionArr
}

// GetConfigurations builds the configurations options
func (ec EmrCluster) GetConfigurations() []types.Configuration {
	configurations := ec.Config.Configurations

	var emrConfigurationArr []types.Configuration

	if configurations != nil && len(configurations) > 0 {
		emrConfigurationArr = make([]types.Configuration, len(configurations))

		for i, configuration := range configurations {
			propertyMap := make(map[string]string)
			for k, v := range configuration.Properties {
				propertyMap[k] = v
			}

			emrConfiguration := types.Configuration{
				Classification: aws.String(configuration.Classification),
				Properties:     propertyMap,
			}

			emrConfigurationArr[i] = emrConfiguration
		}
	}

	return emrConfigurationArr
}

// GetApplications builds the applications options
func (ec EmrCluster) GetApplications() ([]types.Application, error) {
	applications := ec.Config.Applications

	var emrApplicationArr []types.Application
	if applications != nil && len(applications) > 0 {
		emrApplicationArr = make([]types.Application, len(applications))

		for i, application := range applications {
			emrApplication := types.Application{
				Name: aws.String(application),
			}
			emrApplicationArr[i] = emrApplication
		}
	}

	return emrApplicationArr, nil
}
