//
// Copyright (c) 2016-2018 Snowplow Analytics Ltd. All rights reserved.
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
	"strings"
	"testing"

	"errors"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/emr"
	"github.com/aws/aws-sdk-go/service/emr/emriface"
	"github.com/stretchr/testify/assert"
)

type mockEMRAPICluster struct {
	emriface.EMRAPI
}

func (m *mockEMRAPICluster) TerminateJobFlows(input *emr.TerminateJobFlowsInput) (*emr.TerminateJobFlowsOutput, error) {
	if !strings.HasPrefix(*input.JobFlowIds[0], "j-") {
		return nil, errors.New("TerminateJobFlows failed")
	}
	return &emr.TerminateJobFlowsOutput{}, nil
}

// Mock using the cluster id of input to set the cluster state
// ClusterId = "j-STARTING" will result in a cluster with the STARTING state
func (m *mockEMRAPICluster) DescribeCluster(input *emr.DescribeClusterInput) (*emr.DescribeClusterOutput, error) {
	if !strings.HasPrefix(*input.ClusterId, "j-") {
		return nil, errors.New("DescribeCluster failed")
	}
	var state string
	var states = []string{"STARTING", "BOOTSTRAPPING", "RUNNING", "WAITING", "TERMINATING",
		"TERMINATED", "TERMINATED_WITH_ERRORS"}
	for _, e := range states {
		if strings.Contains(*input.ClusterId, e) {
			state = e
			break
		}
	}
	if state == "" {
		return nil, errors.New("DescribeCluster failed")
	}
	if state == "TERMINATED" {
		return &emr.DescribeClusterOutput{
			Cluster: &emr.Cluster{
				Status: &emr.ClusterStatus{
					State: aws.String(state),
					StateChangeReason: &emr.ClusterStateChangeReason{
						Code: aws.String("BOOTSTRAP_FAILURE"),
					},
				},
			},
		}, nil
	}
	return &emr.DescribeClusterOutput{
		Cluster: &emr.Cluster{
			Status: &emr.ClusterStatus{
				State: aws.String(state),
			},
		},
	}, nil
}

func (m *mockEMRAPICluster) RunJobFlow(input *emr.RunJobFlowInput) (*emr.RunJobFlowOutput, error) {
	if *input.Name == "fail" {
		return nil, errors.New("RunJobFlow failed")
	}
	return &emr.RunJobFlowOutput{
		JobFlowId: aws.String("j-" + *input.Name),
	}, nil
}

func mockEmrCluster(clusterRecord ClusterConfig) *EmrCluster {
	return &EmrCluster{
		Config: clusterRecord,
		Svc:    &mockEMRAPICluster{},
	}
}

var CR, _ = InitConfigResolver()

func TestInitEmrCluster(t *testing.T) {
	assert := assert.New(t)

	record, _ := CR.ParseClusterRecord([]byte(ClusterRecord1), nil, "")

	ec, _ := InitEmrCluster(*record)
	assert.NotNil(ec)

	record.Credentials.SecretAccessKey = "hello"
	_, err := InitEmrCluster(*record)
	assert.NotNil(err)
	assert.Equal("access-key and secret-key must both be set to 'env', or neither", err.Error())

	record, _ = CR.ParseClusterRecord([]byte(ClusterRecord2), nil, "")

	ec, _ = InitEmrCluster(*record)
	assert.NotNil(ec)

	record.Credentials.SecretAccessKey = "hello"
	_, err = InitEmrCluster(*record)
	assert.NotNil(err)
	assert.Equal("access-key and secret-key must both be set to 'iam', or neither", err.Error())
}

func TestTerminateJobFlow_Fail(t *testing.T) {
	assert := assert.New(t)
	record, _ := CR.ParseClusterRecord([]byte(ClusterRecord1), nil, "")
	ec := mockEmrCluster(*record)

	// fails if TerminateJobFlows fails
	err := ec.TerminateJobFlow("hello")
	assert.NotNil(err)
	assert.Equal("TerminateJobFlows failed", err.Error())

	// fails if DescribeCluster fails
	err = ec.TerminateJobFlow("j-123")
	assert.NotNil(err)
	assert.Equal("DescribeCluster failed", err.Error())
}

func TestTerminateJobFlow_Success(t *testing.T) {
	record, _ := CR.ParseClusterRecord([]byte(ClusterRecord1), nil, "")
	ec := mockEmrCluster(*record)
	err := ec.TerminateJobFlow("j-TERMINATED")
	assert.Nil(t, err)
}

func TestRunJobFlow_Fail(t *testing.T) {
	assert := assert.New(t)
	record, _ := CR.ParseClusterRecord([]byte(ClusterRecord1), nil, "")

	// fails if GetJobFlowInput fails
	ec := mockEmrCluster(*record)
	_, err := ec.runJobFlow(3)
	assert.NotNil(err)
	assert.Equal("Only one of Availability Zone and Subnet id should be provided", err.Error())

	// fails if emr.RunJobFlow fails
	record.Name = "fail"
	record.Ec2.Location.Vpc = nil
	ec = mockEmrCluster(*record)
	_, err = ec.runJobFlow(3)
	assert.NotNil(err)
	assert.Equal("RunJobFlow failed", err.Error())

	// fails if DescribeCluster fails
	record.Name = "123"
	ec = mockEmrCluster(*record)
	_, err = ec.runJobFlow(3)
	assert.NotNil(err)
	assert.Equal("DescribeCluster failed", err.Error())

	// fails if 3 or more retries
	record.Name = "TERMINATED"
	ec = mockEmrCluster(*record)
	_, err = ec.runJobFlow(3)
	assert.NotNil(err)
	assert.Equal("could not start the cluster due to bootstrap failure", err.Error())

	// fails if the cluster state is not WAITING
	record.Name = "TERMINATING"
	ec = mockEmrCluster(*record)
	_, err = ec.runJobFlow(3)
	assert.NotNil(err)
	assert.Equal("EMR cluster failed to launch with state TERMINATING", err.Error())
}

func TestRunJobFlow_Success(t *testing.T) {
	record, _ := CR.ParseClusterRecord([]byte(ClusterRecord2), nil, "")
	record.Name = "WAITING"
	ec := mockEmrCluster(*record)
	id, _ := ec.runJobFlow(3)
	assert.Equal(t, "j-WAITING", id)
}

func TestGetJobFlowInput_Success(t *testing.T) {
	assert := assert.New(t)

	record, _ := CR.ParseClusterRecord([]byte(ClusterRecord2), nil, "")

	// Master, Core and Task Instances
	record.Ec2.Instances.Core.Count = 1
	record.Ec2.Instances.Task.Count = 1

	ec, _ := InitEmrCluster(*record)
	res, _ := ec.GetJobFlowInput()

	assert.Equal(3, len(res.Instances.InstanceGroups))

	// Master and Task Instances
	record.Ec2.Instances.Core.Count = 0
	record.Ec2.Instances.Task.Count = 1

	ec, _ = InitEmrCluster(*record)
	res, _ = ec.GetJobFlowInput()

	assert.Equal(2, len(res.Instances.InstanceGroups))

	// Master and Core Instances
	record.Ec2.Instances.Core.Count = 1
	record.Ec2.Instances.Task.Count = 0

	ec, _ = InitEmrCluster(*record)
	res, _ = ec.GetJobFlowInput()

	assert.Equal(2, len(res.Instances.InstanceGroups))

	// Master Instances Only
	record.Ec2.Instances.Core.Count = 0
	record.Ec2.Instances.Task.Count = 0

	ec, _ = InitEmrCluster(*record)
	res, _ = ec.GetJobFlowInput()

	assert.Equal(1, len(res.Instances.InstanceGroups))

	// Classic location

	record.Ec2.Location.Vpc = nil
	record.Ec2.Location.Classic = &ClassicRecord{AvailabilityZone: "us-east-1a"}

	ec, _ = InitEmrCluster(*record)
	res, _ = ec.GetJobFlowInput()

	assert.Equal("us-east-1a", *res.Instances.Placement.AvailabilityZone)
	assert.Equal("", *res.Instances.Ec2SubnetId)

	// EMR < 4

	record.Ec2.AmiVersion = "3.0.0"

	ec, _ = InitEmrCluster(*record)
	res, _ = ec.GetJobFlowInput()

	assert.Equal("3.0.0", *res.AmiVersion)

	// EMR >= 4

	record.Ec2.AmiVersion = "4.5.0"

	ec, _ = InitEmrCluster(*record)
	res, _ = ec.GetJobFlowInput()

	assert.Equal("emr-4.5.0", *res.ReleaseLabel)

	// EMR not a valid version string

	record.Ec2.AmiVersion = "hello"

	ec, _ = InitEmrCluster(*record)
	_, err := ec.GetJobFlowInput()

	assert.Equal("strconv.Atoi: parsing \"h\": invalid syntax", err.Error())
}

func TestGetJobFlowInput_Fail(t *testing.T) {
	assert := assert.New(t)

	record, _ := CR.ParseClusterRecord([]byte(ClusterRecord1), nil, "")

	// fails if GetLocation fails
	ec, _ := InitEmrCluster(*record)
	res, err := ec.GetJobFlowInput()
	assert.Nil(res)
	assert.NotNil(err)
	assert.Equal("Only one of Availability Zone and Subnet id should be provided", err.Error())

	record.Ec2.Location.Vpc = nil
	record.Ec2.Location.Classic = nil
	ec, _ = InitEmrCluster(*record)
	res, err = ec.GetJobFlowInput()
	assert.Nil(res)
	assert.NotNil(err)
	assert.Equal("At least one of Availability Zone and Subnet id is required", err.Error())

	// fails if GetApplications fails
	record, _ = CR.ParseClusterRecord([]byte(ClusterRecord2), nil, "")
	record.Applications = []string{"Snowplow"}
	ec, _ = InitEmrCluster(*record)

}

func TestGetInstanceGroups_NoEBS(t *testing.T) {
	assert := assert.New(t)

	record, _ := CR.ParseClusterRecord([]byte(ClusterRecord1), nil, "")
	ec, _ := InitEmrCluster(*record)
	groups := ec.GetInstanceGroups()
	assert.Len(groups, 3)
	expected := []*emr.InstanceGroupConfig{
		{
			InstanceCount: aws.Int64(1),
			InstanceRole:  aws.String("MASTER"),
			InstanceType:  aws.String("m1.medium"),
		},
		{
			InstanceCount: aws.Int64(3),
			InstanceRole:  aws.String("CORE"),
			InstanceType:  aws.String("c3.4xlarge"),
		},
		{
			InstanceCount: aws.Int64(1),
			InstanceRole:  aws.String("TASK"),
			InstanceType:  aws.String("m1.medium"),
			BidPrice:      aws.String("0.015"),
			Market:        aws.String("SPOT"),
		},
	}
	assert.Equal(expected[0], groups[0])
	assert.Equal(expected[1], groups[1])
	assert.Equal(expected[2], groups[2])
}

func TestGetInstanceGroups_WithEBS(t *testing.T) {
	assert := assert.New(t)

	record, _ := CR.ParseClusterRecord([]byte(ClusterRecordWithEBS), nil, "")
	ec, _ := InitEmrCluster(*record)
	groups := ec.GetInstanceGroups()
	assert.Len(groups, 3)
	expected := []*emr.InstanceGroupConfig{
		{
			InstanceCount: aws.Int64(1),
			InstanceRole:  aws.String("MASTER"),
			InstanceType:  aws.String("m1.medium"),
			EbsConfiguration: &emr.EbsConfiguration{
				EbsOptimized: aws.Bool(true),
				EbsBlockDeviceConfigs: []*emr.EbsBlockDeviceConfig{
					{
						VolumesPerInstance: aws.Int64(12),
						VolumeSpecification: &emr.VolumeSpecification{
							Iops:       aws.Int64(8),
							SizeInGB:   aws.Int64(10),
							VolumeType: aws.String("gp2"),
						},
					},
				},
			},
		},
		{
			InstanceCount: aws.Int64(3),
			InstanceRole:  aws.String("CORE"),
			InstanceType:  aws.String("c3.4xlarge"),
			EbsConfiguration: &emr.EbsConfiguration{
				EbsOptimized: aws.Bool(false),
				EbsBlockDeviceConfigs: []*emr.EbsBlockDeviceConfig{
					{
						VolumesPerInstance: aws.Int64(8),
						VolumeSpecification: &emr.VolumeSpecification{
							Iops:       aws.Int64(20),
							SizeInGB:   aws.Int64(4),
							VolumeType: aws.String("io1"),
						},
					},
				},
			},
		},
		{
			InstanceCount: aws.Int64(1),
			InstanceRole:  aws.String("TASK"),
			InstanceType:  aws.String("m1.medium"),
			BidPrice:      aws.String("0.015"),
			Market:        aws.String("SPOT"),
			EbsConfiguration: &emr.EbsConfiguration{
				EbsOptimized: aws.Bool(false),
				EbsBlockDeviceConfigs: []*emr.EbsBlockDeviceConfig{
					{
						VolumesPerInstance: aws.Int64(4),
						VolumeSpecification: &emr.VolumeSpecification{
							Iops:       aws.Int64(100),
							SizeInGB:   aws.Int64(6),
							VolumeType: aws.String("standard"),
						},
					},
				},
			},
		},
	}
	assert.Equal(expected[0], groups[0])
	assert.Equal(expected[1], groups[1])
	assert.Equal(expected[2], groups[2])
}

func TestGetTags_NoTags(t *testing.T) {
	record, _ := CR.ParseClusterRecord([]byte(ClusterRecord1), nil, "")
	ec, _ := InitEmrCluster(*record)
	assert.Nil(t, ec.GetTags())
}

func TestGetTags_WithTags(t *testing.T) {
	record, _ := CR.ParseClusterRecord([]byte(ClusterRecordWithTags), nil, "")
	ec, _ := InitEmrCluster(*record)
	tags := ec.GetTags()
	assert.Len(t, tags, 1)
	expected := &emr.Tag{
		Key:   aws.String("hello"),
		Value: aws.String("world"),
	}
	assert.Equal(t, expected, tags[0])
}

func TestGetBootstrapActions_NoActions(t *testing.T) {
	record, _ := CR.ParseClusterRecord([]byte(ClusterRecord1), nil, "")
	ec, _ := InitEmrCluster(*record)
	assert.Nil(t, ec.GetBootstrapActions())
}

func TestGetBootstrapActions_WithActions(t *testing.T) {
	record, _ := CR.ParseClusterRecord([]byte(ClusterRecordWithActions), nil, "")
	ec, _ := InitEmrCluster(*record)
	actions := ec.GetBootstrapActions()
	assert.Len(t, actions, 1)
	expected := &emr.BootstrapActionConfig{
		Name: aws.String("Bootstrap Action"),
		ScriptBootstrapAction: &emr.ScriptBootstrapActionConfig{
			Path: aws.String("s3://snowplow/script.sh"),
			Args: []*string{aws.String("1.5")},
		},
	}
	assert.Equal(t, expected, actions[0])
}

func TestGetConfigurations_NoConfigs(t *testing.T) {
	record, _ := CR.ParseClusterRecord([]byte(ClusterRecord1), nil, "")
	ec, _ := InitEmrCluster(*record)
	assert.Nil(t, ec.GetConfigurations())
}

func TestGetConfigurations_WithConfigs(t *testing.T) {
	record, _ := CR.ParseClusterRecord([]byte(ClusterRecordWithConfigs), nil, "")
	ec, _ := InitEmrCluster(*record)
	configs := ec.GetConfigurations()
	assert.Len(t, configs, 1)
	expected := &emr.Configuration{
		Classification: aws.String("c"),
		Properties:     map[string]*string{"key": aws.String("value")},
	}
	assert.Equal(t, expected, configs[0])
}

func TestGetApplications_NoApps(t *testing.T) {
	record, _ := CR.ParseClusterRecord([]byte(ClusterRecord1), nil, "")
	ec, _ := InitEmrCluster(*record)
	apps, err := ec.GetApplications()
	assert.Nil(t, apps)
	assert.Nil(t, err)
}

func TestGetApplications_WithApps(t *testing.T) {
	assert := assert.New(t)
	record, _ := CR.ParseClusterRecord([]byte(ClusterRecordWithApps), nil, "")
	ec, _ := InitEmrCluster(*record)
	apps, _ := ec.GetApplications()
	assert.Len(apps, 2)
	assert.Equal(aws.String("Hadoop"), apps[0].Name)
	assert.Equal(aws.String("Spark"), apps[1].Name)
}

func TestGetLocation_Fail(t *testing.T) {
	assert := assert.New(t)
	record, _ := CR.ParseClusterRecord([]byte(ClusterRecord1), nil, "")
	ec, _ := InitEmrCluster(*record)

	_, _, err := ec.GetLocation()
	assert.NotNil(err)
	assert.Equal("Only one of Availability Zone and Subnet id should be provided", err.Error())

	record.Ec2.Location.Classic = nil
	record.Ec2.Location.Vpc = nil
	_, _, err = ec.GetLocation()
	assert.NotNil(err)
	assert.Equal("At least one of Availability Zone and Subnet id is required", err.Error())
}

func TestGetLocation_Success(t *testing.T) {
	assert := assert.New(t)

	record, _ := CR.ParseClusterRecord([]byte(ClusterRecord2), nil, "")
	ec, _ := InitEmrCluster(*record)

	s, p, err := ec.GetLocation()
	assert.Nil(err)
	assert.Equal(s, "subnet-123456")
	assert.Equal(p, "")

	record.Ec2.Location.Classic = &ClassicRecord{
		AvailabilityZone: "eu-central-1",
	}
	record.Ec2.Location.Vpc = nil
	s, p, err = ec.GetLocation()
	assert.Nil(err)
	assert.Equal(s, "")
	assert.Equal(p, "eu-central-1")
}
