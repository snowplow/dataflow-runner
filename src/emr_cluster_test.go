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
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/emr"
	"github.com/aws/aws-sdk-go-v2/service/emr/types"
	"github.com/stretchr/testify/assert"
)

type mockEMRAPICluster struct{}

func (m *mockEMRAPICluster) TerminateJobFlows(ctx context.Context, input *emr.TerminateJobFlowsInput, optFns ...func(*emr.Options)) (*emr.TerminateJobFlowsOutput, error) {
	if !strings.HasPrefix(input.JobFlowIds[0], "j-") {
		return nil, errors.New("TerminateJobFlows failed")
	}
	return &emr.TerminateJobFlowsOutput{}, nil
}

// Mock using the cluster id of input to set the cluster state
// ClusterId = "j-STARTING" will result in a cluster with the STARTING state
func (m *mockEMRAPICluster) DescribeCluster(ctx context.Context, input *emr.DescribeClusterInput, optFns ...func(*emr.Options)) (*emr.DescribeClusterOutput, error) {
	if !strings.HasPrefix(*input.ClusterId, "j-") {
		return nil, errors.New("DescribeCluster failed")
	}
	var state types.ClusterState
	var states = []types.ClusterState{
		types.ClusterStateStarting,
		types.ClusterStateBootstrapping,
		types.ClusterStateRunning,
		types.ClusterStateWaiting,
		types.ClusterStateTerminating,
		types.ClusterStateTerminated,
		types.ClusterStateTerminatedWithErrors,
	}
	for _, e := range states {
		if strings.TrimPrefix(*input.ClusterId, "j-") == string(e) {
			state = e
			break
		}
	}
	if state == "" {
		return nil, errors.New("DescribeCluster failed")
	}
	if state == types.ClusterStateTerminated {
		return &emr.DescribeClusterOutput{
			Cluster: &types.Cluster{
				Status: &types.ClusterStatus{
					State: state,
					StateChangeReason: &types.ClusterStateChangeReason{
						Code:    types.ClusterStateChangeReasonCodeBootstrapFailure,
						Message: aws.String("Bootstrap action returned a non-zero return code"),
					},
				},
			},
		}, nil
	}
	if state == types.ClusterStateTerminatedWithErrors {
		return &emr.DescribeClusterOutput{
			Cluster: &types.Cluster{
				Status: &types.ClusterStatus{
					State: state,
					StateChangeReason: &types.ClusterStateChangeReason{
						Code:    types.ClusterStateChangeReasonCodeValidationError,
						Message: aws.String("On the master instance, application provisioning failed"),
					},
				},
			},
		}, nil
	}
	return &emr.DescribeClusterOutput{
		Cluster: &types.Cluster{
			Status: &types.ClusterStatus{
				State: state,
			},
		},
	}, nil
}

func (m *mockEMRAPICluster) RunJobFlow(ctx context.Context, input *emr.RunJobFlowInput, optFns ...func(*emr.Options)) (*emr.RunJobFlowOutput, error) {
	if *input.Name == "fail" {
		return nil, errors.New("RunJobFlow failed")
	}
	return &emr.RunJobFlowOutput{
		JobFlowId: aws.String("j-" + *input.Name),
	}, nil
}

func (m *mockEMRAPICluster) AddJobFlowSteps(ctx context.Context, input *emr.AddJobFlowStepsInput, optFns ...func(*emr.Options)) (*emr.AddJobFlowStepsOutput, error) {
	return nil, nil
}

func (m *mockEMRAPICluster) ListSteps(ctx context.Context, input *emr.ListStepsInput, optFns ...func(*emr.Options)) (*emr.ListStepsOutput, error) {
	return nil, nil
}

func (m *mockEMRAPICluster) DescribeStep(ctx context.Context, input *emr.DescribeStepInput, optFns ...func(*emr.Options)) (*emr.DescribeStepOutput, error) {
	return nil, nil
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
	assert.Equal("emr.TerminateJobFlow: TerminateJobFlows failed", err.Error())

	// fails if DescribeCluster fails
	err = ec.TerminateJobFlow("j-123")
	assert.NotNil(err)
	assert.Equal("emr.DescribeCluster: DescribeCluster failed", err.Error())
}

func TestTerminateJobFlow_Success(t *testing.T) {
	record, _ := CR.ParseClusterRecord([]byte(ClusterRecord1), nil, "")
	ec := mockEmrCluster(*record)
	err := ec.TerminateJobFlow("j-TERMINATED")
	assert.Nil(t, err)
}

func TestRunJobFlow_Fail(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()
	record, _ := CR.ParseClusterRecord([]byte(ClusterRecord1), nil, "")

	// fails if GetJobFlowInput fails
	ec := mockEmrCluster(*record)
	_, err := ec.runJobFlow(ctx, 3)
	assert.NotNil(err)
	assert.Equal("Only one of Availability Zone and Subnet id should be provided", err.Error())

	// fails if emr.RunJobFlow fails
	record.Name = "fail"
	record.Ec2.Location.Vpc = nil
	ec = mockEmrCluster(*record)
	_, err = ec.runJobFlow(ctx, 3)
	assert.NotNil(err)
	assert.Equal("emr.RunJobFlow: RunJobFlow failed", err.Error())

	// fails if DescribeCluster fails
	record.Name = "123"
	ec = mockEmrCluster(*record)
	_, err = ec.runJobFlow(ctx, 3)
	assert.NotNil(err)
	assert.Equal("emr.DescribeCluster: DescribeCluster failed", err.Error())

	// fails if 3 or more retries
	record.Name = "TERMINATED"
	ec = mockEmrCluster(*record)
	_, err = ec.runJobFlow(ctx, 3)
	assert.NotNil(err)
	assert.Equal("could not start the cluster due to bootstrap failure: code=BOOTSTRAP_FAILURE message=\"Bootstrap action returned a non-zero return code\"", err.Error())

	// fails if the cluster state is not WAITING
	record.Name = "TERMINATING"
	ec = mockEmrCluster(*record)
	_, err = ec.runJobFlow(ctx, 3)
	assert.NotNil(err)
	assert.Equal("EMR cluster failed to launch with state TERMINATING", err.Error())

	// surfaces the StateChangeReason code and message when present on a non-bootstrap failure
	record.Name = "TERMINATED_WITH_ERRORS"
	ec = mockEmrCluster(*record)
	_, err = ec.runJobFlow(ctx, 3)
	assert.NotNil(err)
	assert.Equal("EMR cluster failed to launch with state TERMINATED_WITH_ERRORS: code=VALIDATION_ERROR message=\"On the master instance, application provisioning failed\"", err.Error())
}

func TestRunJobFlow_Success(t *testing.T) {
	ctx := context.Background()
	record, _ := CR.ParseClusterRecord([]byte(ClusterRecord2), nil, "")
	record.Name = "WAITING"
	ec := mockEmrCluster(*record)
	id, _ := ec.runJobFlow(ctx, 3)
	assert.Equal(t, "j-WAITING", id)
}

func TestGetJobFlowInput_Success(t *testing.T) {
	assert := assert.New(t)

	record, _ := CR.ParseClusterRecord([]byte(ClusterRecord2), nil, "")

	// Master, Core and Task Instances
	record.Ec2.Instances.Core.Count = 1
	record.Ec2.Instances.Task.Count = 1

	ec, _ := InitEmrCluster(*record)
	res, _ := ec.GetJobFlowInput(true)

	assert.Equal(3, len(res.Instances.InstanceGroups))

	// Master and Task Instances
	record.Ec2.Instances.Core.Count = 0
	record.Ec2.Instances.Task.Count = 1

	ec, _ = InitEmrCluster(*record)
	res, _ = ec.GetJobFlowInput(true)

	assert.Equal(2, len(res.Instances.InstanceGroups))

	// Master and Core Instances
	record.Ec2.Instances.Core.Count = 1
	record.Ec2.Instances.Task.Count = 0

	ec, _ = InitEmrCluster(*record)
	res, _ = ec.GetJobFlowInput(true)

	assert.Equal(2, len(res.Instances.InstanceGroups))

	// Master Instances Only
	record.Ec2.Instances.Core.Count = 0
	record.Ec2.Instances.Task.Count = 0

	ec, _ = InitEmrCluster(*record)
	res, _ = ec.GetJobFlowInput(true)

	assert.Equal(1, len(res.Instances.InstanceGroups))

	// Classic location

	record.Ec2.Location.Vpc = nil
	record.Ec2.Location.Classic = &ClassicRecord{AvailabilityZone: "us-east-1a"}

	ec, _ = InitEmrCluster(*record)
	res, _ = ec.GetJobFlowInput(true)

	assert.Equal("us-east-1a", *res.Instances.Placement.AvailabilityZone)
	assert.Equal("", *res.Instances.Ec2SubnetId)

	// EMR < 4

	record.Ec2.AmiVersion = "3.0.0"

	ec, _ = InitEmrCluster(*record)
	res, _ = ec.GetJobFlowInput(true)

	assert.Equal("3.0.0", *res.AmiVersion)

	// EMR >= 4

	record.Ec2.AmiVersion = "4.5.0"

	ec, _ = InitEmrCluster(*record)
	res, _ = ec.GetJobFlowInput(true)

	assert.Equal("emr-4.5.0", *res.ReleaseLabel)

	// EMR not a valid version string

	record.Ec2.AmiVersion = "hello"

	ec, _ = InitEmrCluster(*record)
	_, err := ec.GetJobFlowInput(true)

	assert.Equal("strconv.Atoi: parsing \"h\": invalid syntax", err.Error())
}

func TestGetJobFlowInput_Fail(t *testing.T) {
	assert := assert.New(t)

	record, _ := CR.ParseClusterRecord([]byte(ClusterRecord1), nil, "")

	// fails if GetLocation fails
	ec, _ := InitEmrCluster(*record)
	res, err := ec.GetJobFlowInput(true)
	assert.Nil(res)
	assert.NotNil(err)
	assert.Equal("Only one of Availability Zone and Subnet id should be provided", err.Error())

	record.Ec2.Location.Vpc = nil
	record.Ec2.Location.Classic = nil
	ec, _ = InitEmrCluster(*record)
	res, err = ec.GetJobFlowInput(true)
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
	expected := []types.InstanceGroupConfig{
		{
			InstanceCount: aws.Int32(1),
			InstanceRole:  types.InstanceRoleTypeMaster,
			InstanceType:  aws.String("m1.medium"),
		},
		{
			InstanceCount: aws.Int32(3),
			InstanceRole:  types.InstanceRoleTypeCore,
			InstanceType:  aws.String("c3.4xlarge"),
		},
		{
			InstanceCount: aws.Int32(1),
			InstanceRole:  types.InstanceRoleTypeTask,
			InstanceType:  aws.String("m1.medium"),
			BidPrice:      aws.String("0.015"),
			Market:        types.MarketTypeSpot,
		},
	}
	assert.Equal(expected[0].InstanceCount, groups[0].InstanceCount)
	assert.Equal(expected[0].InstanceRole, groups[0].InstanceRole)
	assert.Equal(expected[0].InstanceType, groups[0].InstanceType)
	assert.Equal(expected[1].InstanceCount, groups[1].InstanceCount)
	assert.Equal(expected[1].InstanceRole, groups[1].InstanceRole)
	assert.Equal(expected[1].InstanceType, groups[1].InstanceType)
	assert.Equal(expected[2].InstanceCount, groups[2].InstanceCount)
	assert.Equal(expected[2].InstanceRole, groups[2].InstanceRole)
	assert.Equal(expected[2].InstanceType, groups[2].InstanceType)
	assert.Equal(expected[2].BidPrice, groups[2].BidPrice)
	assert.Equal(expected[2].Market, groups[2].Market)
}

func TestGetInstanceGroups_WithEBS(t *testing.T) {
	assert := assert.New(t)

	record, _ := CR.ParseClusterRecord([]byte(ClusterRecordWithEBS), nil, "")
	ec, _ := InitEmrCluster(*record)
	groups := ec.GetInstanceGroups()
	assert.Len(groups, 3)

	// Master instance
	assert.Equal(aws.Int32(1), groups[0].InstanceCount)
	assert.Equal(types.InstanceRoleTypeMaster, groups[0].InstanceRole)
	assert.Equal(aws.String("m1.medium"), groups[0].InstanceType)
	assert.NotNil(groups[0].EbsConfiguration)
	assert.Equal(aws.Bool(true), groups[0].EbsConfiguration.EbsOptimized)
	assert.Len(groups[0].EbsConfiguration.EbsBlockDeviceConfigs, 1)
	assert.Equal(aws.Int32(12), groups[0].EbsConfiguration.EbsBlockDeviceConfigs[0].VolumesPerInstance)
	assert.Equal(aws.Int32(10), groups[0].EbsConfiguration.EbsBlockDeviceConfigs[0].VolumeSpecification.SizeInGB)
	assert.Equal(aws.String("gp2"), groups[0].EbsConfiguration.EbsBlockDeviceConfigs[0].VolumeSpecification.VolumeType)

	// Core instance
	assert.Equal(aws.Int32(3), groups[1].InstanceCount)
	assert.Equal(types.InstanceRoleTypeCore, groups[1].InstanceRole)
	assert.Equal(aws.String("c3.4xlarge"), groups[1].InstanceType)
	assert.NotNil(groups[1].EbsConfiguration)
	assert.Equal(aws.Bool(false), groups[1].EbsConfiguration.EbsOptimized)
	assert.Len(groups[1].EbsConfiguration.EbsBlockDeviceConfigs, 1)
	assert.Equal(aws.Int32(8), groups[1].EbsConfiguration.EbsBlockDeviceConfigs[0].VolumesPerInstance)
	assert.Equal(aws.Int32(20), groups[1].EbsConfiguration.EbsBlockDeviceConfigs[0].VolumeSpecification.Iops)
	assert.Equal(aws.Int32(4), groups[1].EbsConfiguration.EbsBlockDeviceConfigs[0].VolumeSpecification.SizeInGB)
	assert.Equal(aws.String("io1"), groups[1].EbsConfiguration.EbsBlockDeviceConfigs[0].VolumeSpecification.VolumeType)

	// Task instance
	assert.Equal(aws.Int32(1), groups[2].InstanceCount)
	assert.Equal(types.InstanceRoleTypeTask, groups[2].InstanceRole)
	assert.Equal(aws.String("m1.medium"), groups[2].InstanceType)
	assert.Equal(aws.String("0.015"), groups[2].BidPrice)
	assert.Equal(types.MarketTypeSpot, groups[2].Market)
	assert.NotNil(groups[2].EbsConfiguration)
	assert.Equal(aws.Bool(false), groups[2].EbsConfiguration.EbsOptimized)
}

func TestGetInstanceGroups_WithGP3(t *testing.T) {
	assert := assert.New(t)

	record, _ := CR.ParseClusterRecord([]byte(ClusterRecordWithGP3), nil, "")
	ec, _ := InitEmrCluster(*record)
	groups := ec.GetInstanceGroups()
	assert.Len(groups, 3)

	// Master instance with gp3
	assert.Equal(aws.Int32(1), groups[0].InstanceCount)
	assert.Equal(types.InstanceRoleTypeMaster, groups[0].InstanceRole)
	assert.NotNil(groups[0].EbsConfiguration)
	assert.Equal(aws.Bool(true), groups[0].EbsConfiguration.EbsOptimized)
	assert.Len(groups[0].EbsConfiguration.EbsBlockDeviceConfigs, 1)
	assert.Equal(aws.String("gp3"), groups[0].EbsConfiguration.EbsBlockDeviceConfigs[0].VolumeSpecification.VolumeType)
	// gp3 should not have Iops set
	assert.Nil(groups[0].EbsConfiguration.EbsBlockDeviceConfigs[0].VolumeSpecification.Iops)
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
	expected := types.Tag{
		Key:   aws.String("hello"),
		Value: aws.String("world"),
	}
	assert.Equal(t, expected.Key, tags[0].Key)
	assert.Equal(t, expected.Value, tags[0].Value)
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
	assert.Equal(t, aws.String("Bootstrap Action"), actions[0].Name)
	assert.Equal(t, aws.String("s3://snowplow/script.sh"), actions[0].ScriptBootstrapAction.Path)
	assert.Equal(t, []string{"1.5"}, actions[0].ScriptBootstrapAction.Args)
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
	assert.Equal(t, aws.String("c"), configs[0].Classification)
	assert.Equal(t, map[string]string{"key": "value"}, configs[0].Properties)
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
