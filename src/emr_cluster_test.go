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
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsretry "github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/service/emr"
	"github.com/aws/aws-sdk-go-v2/service/emr/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
)

// TestMain collapses the DescribeCluster backoff for the whole package. Only the
// delays are shrunk, not the attempt counts: how long the waiting takes is
// nothing a test wants to sit through, but how many attempts each kind of error
// is worth is exactly what several of them assert.
func TestMain(m *testing.M) {
	emrLongRetryBaseDelay = time.Millisecond
	emrShortRetryBaseDelay = time.Millisecond
	os.Exit(m.Run())
}

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

// mockEMRAPISequence returns a scripted sequence of cluster states on successive
// DescribeCluster calls, enabling tests of the waiter path without sleeping.
type mockEMRAPISequence struct {
	sequence []types.ClusterState
	callNum  int
}

func (m *mockEMRAPISequence) DescribeCluster(ctx context.Context, input *emr.DescribeClusterInput, optFns ...func(*emr.Options)) (*emr.DescribeClusterOutput, error) {
	if m.callNum >= len(m.sequence) {
		return nil, errors.New("DescribeCluster sequence exhausted")
	}
	state := m.sequence[m.callNum]
	m.callNum++

	// Build the status based on the state
	status := &types.ClusterStatus{State: state}
	if state == types.ClusterStateTerminated {
		status.StateChangeReason = &types.ClusterStateChangeReason{
			Code:    types.ClusterStateChangeReasonCodeBootstrapFailure,
			Message: aws.String("Bootstrap action returned a non-zero return code"),
		}
	} else if state == types.ClusterStateTerminatedWithErrors {
		status.StateChangeReason = &types.ClusterStateChangeReason{
			Code:    types.ClusterStateChangeReasonCodeValidationError,
			Message: aws.String("On the master instance, application provisioning failed"),
		}
	}

	return &emr.DescribeClusterOutput{
		Cluster: &types.Cluster{
			Status: status,
		},
	}, nil
}

func (m *mockEMRAPISequence) RunJobFlow(ctx context.Context, input *emr.RunJobFlowInput, optFns ...func(*emr.Options)) (*emr.RunJobFlowOutput, error) {
	return nil, errors.New("RunJobFlow not supported by mockEMRAPISequence")
}

func (m *mockEMRAPISequence) TerminateJobFlows(ctx context.Context, input *emr.TerminateJobFlowsInput, optFns ...func(*emr.Options)) (*emr.TerminateJobFlowsOutput, error) {
	return nil, errors.New("TerminateJobFlows not supported by mockEMRAPISequence")
}

func (m *mockEMRAPISequence) AddJobFlowSteps(ctx context.Context, input *emr.AddJobFlowStepsInput, optFns ...func(*emr.Options)) (*emr.AddJobFlowStepsOutput, error) {
	return nil, errors.New("AddJobFlowSteps not supported by mockEMRAPISequence")
}

func (m *mockEMRAPISequence) ListSteps(ctx context.Context, input *emr.ListStepsInput, optFns ...func(*emr.Options)) (*emr.ListStepsOutput, error) {
	return nil, errors.New("ListSteps not supported by mockEMRAPISequence")
}

func (m *mockEMRAPISequence) DescribeStep(ctx context.Context, input *emr.DescribeStepInput, optFns ...func(*emr.Options)) (*emr.DescribeStepOutput, error) {
	return nil, errors.New("DescribeStep not supported by mockEMRAPISequence")
}

// describeResult is one scripted DescribeCluster outcome: either a status or an
// error, never both.
type describeResult struct {
	status *types.ClusterStatus
	err    error
}

func describeOK(state types.ClusterState) describeResult {
	return describeResult{status: &types.ClusterStatus{State: state}}
}

func describeOKWithReason(state types.ClusterState, code types.ClusterStateChangeReasonCode) describeResult {
	return describeResult{status: &types.ClusterStatus{
		State:             state,
		StateChangeReason: &types.ClusterStateChangeReason{Code: code, Message: aws.String("scripted")},
	}}
}

// describeThrottled is AWS asking us to slow down: worth waiting out.
func describeThrottled() describeResult {
	return describeResult{err: &smithy.GenericAPIError{
		Code:    "ThrottlingException",
		Message: "Rate exceeded",
	}}
}

// describeUnknownCluster is AWS telling us the jobflow ID does not exist, which
// is what a bad --emr-cluster on the command line produces. Waiting cannot make
// it true, so it must fail on the first response.
func describeUnknownCluster() describeResult {
	return describeResult{err: &smithy.GenericAPIError{
		Code:    "InvalidRequestException",
		Message: "Cluster id 'j-nope' is not valid",
	}}
}

// mockEMRAPIScript returns a scripted sequence of DescribeCluster outcomes so a
// test can place a failure at an exact point in a wait. The last entry repeats
// once the script runs out, which is how "and throttled from here on" is
// expressed.
type mockEMRAPIScript struct {
	script         []describeResult
	calls          int
	terminateCalls int
	// lastOptions is the per-call options the last DescribeCluster resolved to,
	// so tests can see which retry budget the call was made with.
	lastOptions emr.Options
}

func (m *mockEMRAPIScript) DescribeCluster(ctx context.Context, input *emr.DescribeClusterInput, optFns ...func(*emr.Options)) (*emr.DescribeClusterOutput, error) {
	m.lastOptions = emr.Options{}
	for _, fn := range optFns {
		fn(&m.lastOptions)
	}

	i := m.calls
	if i >= len(m.script) {
		i = len(m.script) - 1
	}
	m.calls++

	result := m.script[i]
	if result.err != nil {
		return nil, result.err
	}
	return &emr.DescribeClusterOutput{Cluster: &types.Cluster{Status: result.status}}, nil
}

func (m *mockEMRAPIScript) RunJobFlow(ctx context.Context, input *emr.RunJobFlowInput, optFns ...func(*emr.Options)) (*emr.RunJobFlowOutput, error) {
	return nil, errors.New("RunJobFlow not supported by mockEMRAPIScript")
}

func (m *mockEMRAPIScript) TerminateJobFlows(ctx context.Context, input *emr.TerminateJobFlowsInput, optFns ...func(*emr.Options)) (*emr.TerminateJobFlowsOutput, error) {
	m.terminateCalls++
	return &emr.TerminateJobFlowsOutput{}, nil
}

func (m *mockEMRAPIScript) AddJobFlowSteps(ctx context.Context, input *emr.AddJobFlowStepsInput, optFns ...func(*emr.Options)) (*emr.AddJobFlowStepsOutput, error) {
	return nil, errors.New("AddJobFlowSteps not supported by mockEMRAPIScript")
}

func (m *mockEMRAPIScript) ListSteps(ctx context.Context, input *emr.ListStepsInput, optFns ...func(*emr.Options)) (*emr.ListStepsOutput, error) {
	return nil, errors.New("ListSteps not supported by mockEMRAPIScript")
}

func (m *mockEMRAPIScript) DescribeStep(ctx context.Context, input *emr.DescribeStepInput, optFns ...func(*emr.Options)) (*emr.DescribeStepOutput, error) {
	return nil, errors.New("DescribeStep not supported by mockEMRAPIScript")
}

func scriptedEmrCluster(script ...describeResult) (*EmrCluster, *mockEMRAPIScript) {
	record, _ := CR.ParseClusterRecord([]byte(ClusterRecord1), nil, "")
	svc := &mockEMRAPIScript{script: script}
	return &EmrCluster{Config: *record, Svc: svc}, svc
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
	assert.Equal("emr.TerminateJobFlows: TerminateJobFlows failed", err.Error())

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

	// No "fails if DescribeCluster fails" case: waitForClusterReady no longer
	// checks the cluster exists before watching it, and the waiter treats a
	// failed describe as "not there yet" and polls again. A cluster that can
	// never be described now occupies the waiter until it times out, which is the
	// accepted cost of the jobflow ID always having come from RunJobFlow.

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

	// WAITING is what a stepless cluster should reach, but the running waiter
	// resolves on RUNNING too and a cluster observed there is perfectly usable.
	// Failing that one would report a healthy cluster as a failed launch.
	for _, state := range []string{"WAITING", "RUNNING"} {
		record.Name = state
		ec := mockEmrCluster(*record)
		id, err := ec.runJobFlow(ctx, 3)
		assert.Nil(t, err, state)
		assert.Equal(t, "j-"+state, id)
	}
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

func TestIsBootstrapFailure(t *testing.T) {
	assert := assert.New(t)

	// a nil status must not be treated as a bootstrap failure, and must not panic
	assert.False(isBootstrapFailure(nil))

	// a status with no reason at all
	assert.False(isBootstrapFailure(&types.ClusterStatus{
		State: types.ClusterStateTerminatedWithErrors,
	}))

	// a different reason code
	assert.False(isBootstrapFailure(&types.ClusterStatus{
		State: types.ClusterStateTerminatedWithErrors,
		StateChangeReason: &types.ClusterStateChangeReason{
			Code: types.ClusterStateChangeReasonCodeValidationError,
		},
	}))

	assert.True(isBootstrapFailure(&types.ClusterStatus{
		State: types.ClusterStateTerminatedWithErrors,
		StateChangeReason: &types.ClusterStateChangeReason{
			Code: types.ClusterStateChangeReasonCodeBootstrapFailure,
		},
	}))
}

func TestWaitForClusterFinished(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()
	record, _ := CR.ParseClusterRecord([]byte(ClusterRecord1), nil, "")
	ec := mockEmrCluster(*record)

	// a cleanly terminated cluster succeeds and hands back its status
	status, err := ec.waitForClusterFinished(ctx, "j-TERMINATED")
	assert.Nil(err)
	assert.NotNil(status)
	assert.Equal(types.ClusterStateTerminated, status.State)

	// TERMINATED_WITH_ERRORS is a failure here, unlike in waitForClusterTerminated,
	// and the status still comes back so the caller can classify it
	status, err = ec.waitForClusterFinished(ctx, "j-TERMINATED_WITH_ERRORS")
	assert.NotNil(err)
	assert.NotNil(status)
	assert.Equal(types.ClusterStateTerminatedWithErrors, status.State)
	assert.Equal(types.ClusterStateChangeReasonCodeValidationError, status.StateChangeReason.Code)

	// No undescribable-cluster case: waitForClusterFinished no longer checks the
	// cluster exists before watching it, so one that can never be described now
	// occupies the waiter rather than failing. waitForClusterTerminated, which
	// does still check, covers that below.
}

// TestWaitForClusterReady_AlreadyTerminated exercises the recovery describe. A
// failure acceptor makes the waiter return no output at all, so the status has
// to be fetched again before the caller can tell a bootstrap failure from
// anything else.
func TestWaitForClusterReady_AlreadyTerminated(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()

	ec, svc := scriptedEmrCluster(
		describeOKWithReason(types.ClusterStateTerminated,
			types.ClusterStateChangeReasonCodeBootstrapFailure),
	)

	status, err := ec.waitForClusterReady(ctx, "j-test")
	assert.NotNil(err, "the waiter reports the transition to a failure state")
	assert.NotNil(status, "but the status still comes back, for bootstrap detection")
	assert.True(isBootstrapFailure(status))
	assert.Equal(2, svc.calls, "one waiter poll, one recovery describe")
}

// TestWaitForClusterFinished_WithWaiter_Success tests the waiter path when the
// cluster is running and then terminates cleanly, ensuring waiter construction,
// waiter.Wait, and post-wait re-describe are exercised without sleeping.
func TestWaitForClusterFinished_WithWaiter_Success(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()
	record, _ := CR.ParseClusterRecord([]byte(ClusterRecord1), nil, "")

	ec := &EmrCluster{
		Config: *record,
		Svc: &mockEMRAPISequence{
			sequence: []types.ClusterState{
				types.ClusterStateTerminated,
				types.ClusterStateTerminated,
			},
		},
	}

	status, err := ec.waitForClusterFinished(ctx, "j-test")
	assert.Nil(err)
	assert.NotNil(status)
	assert.Equal(types.ClusterStateTerminated, status.State)
}

// TestWaitForClusterFinished_WithWaiter_Failure tests the waiter path when the
// cluster fails during execution, ensuring the caller receives both the error
// and the status for classification (the critical branch for transient retries).
//
// It also guards a property of the SDK that waitForClusterFinished now depends on
// for liveness: TERMINATED_WITH_ERRORS must be a failure acceptor. If a future
// SDK dropped it, the state would match no acceptor, the waiter would poll on for
// transientMaxWaitDuration, and this test would fail by timing out rather than by
// asserting — so a hang here means look at the acceptors first.
func TestWaitForClusterFinished_WithWaiter_Failure(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()
	record, _ := CR.ParseClusterRecord([]byte(ClusterRecord1), nil, "")

	ec := &EmrCluster{
		Config: *record,
		Svc: &mockEMRAPISequence{
			sequence: []types.ClusterState{
				types.ClusterStateTerminatedWithErrors,
				types.ClusterStateTerminatedWithErrors,
			},
		},
	}

	status, err := ec.waitForClusterFinished(ctx, "j-test")
	assert.NotNil(err)
	assert.NotNil(status)
	assert.Equal(types.ClusterStateTerminatedWithErrors, status.State)
	assert.Equal(types.ClusterStateChangeReasonCodeValidationError, status.StateChangeReason.Code)
}

// TestClusterPollDelaysAreJittered guards the property that makes a fleet of
// runners share the account's DescribeCluster budget: smithy only jitters a
// waiter's delay when the maximum exceeds the minimum, so collapsing either pair
// to a single value would silently put every runner back on the same fixed beat.
func TestClusterPollDelaysAreJittered(t *testing.T) {
	assert := assert.New(t)

	assert.Less(launchPollMinDelay, launchPollMaxDelay)
	assert.Less(jobPollMinDelay, jobPollMaxDelay)
}

// TestWaitForClusterReady_ThrottledOnceClusterIsUp covers the launch-phase half
// of the throttling failure: the cluster comes up, and every DescribeCluster
// after that point is throttled. Reading the status from the waiter's own output
// means there is no such call to throttle, so the launch is still reported as
// the success it was rather than abandoning a cluster that is running its steps.
func TestWaitForClusterReady_ThrottledOnceClusterIsUp(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()

	ec, svc := scriptedEmrCluster(
		describeOK(types.ClusterStateRunning), // the waiter resolves on its first poll
		describeThrottled(),                   // and everything after is throttled
	)

	status, err := ec.waitForClusterReady(ctx, "j-test")
	assert.Nil(err)
	assert.NotNil(status)
	assert.Equal(types.ClusterStateRunning, status.State)
	assert.Equal(1, svc.calls, "no DescribeCluster should be made after the waiter resolves")
}

// TestWaitForClusterFinished_ThrottledOnceTerminated covers the job-phase half:
// the cluster terminates cleanly and every DescribeCluster after that is
// throttled. This is the run that was being reported as failed.
func TestWaitForClusterFinished_ThrottledOnceTerminated(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()

	ec, svc := scriptedEmrCluster(
		describeOKWithReason(types.ClusterStateTerminated,
			types.ClusterStateChangeReasonCodeAllStepsCompleted), // the waiter resolves here
		describeThrottled(), // and everything after is throttled
	)

	status, err := ec.waitForClusterFinished(ctx, "j-test")
	assert.Nil(err)
	assert.NotNil(status)
	assert.Equal(types.ClusterStateTerminated, status.State)
	assert.Equal(types.ClusterStateChangeReasonCodeAllStepsCompleted, status.StateChangeReason.Code)
	assert.Equal(1, svc.calls, "no DescribeCluster should be made after the waiter resolves")
}

// TestWaitForClusterTerminated_RetriesThrottledDescribe checks the other half of
// the defence: the describes we do still make ride out a throttled response
// instead of failing the run on the first one.
func TestWaitForClusterTerminated_RetriesThrottledDescribe(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()

	ec, svc := scriptedEmrCluster(
		describeThrottled(),                      // pre-flight, first attempt
		describeOK(types.ClusterStateTerminated), // pre-flight, retried
	)

	assert.Nil(ec.waitForClusterTerminated(ctx, "j-test"))
	assert.Equal(2, svc.calls)
}

// TestWaitForClusterFinished_TerminatedWithoutReason covers a cluster that
// reaches TERMINATED before EMR has attached its StateChangeReason, where the
// describe that would recover it cannot get through.
//
// The caller cannot confirm a run complete without ALL_STEPS_COMPLETED, so this
// fails whatever we return. Two things still matter: the error says why the
// cluster could not be classified rather than only that it could not be, and the
// status the waiter did see comes back — discarding it makes the caller announce
// a cluster "left to run unwatched" that has already terminated.
func TestWaitForClusterFinished_TerminatedWithoutReason(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()

	ec, svc := scriptedEmrCluster(
		describeOK(types.ClusterStateTerminated), // the waiter resolves, but with no reason
		describeThrottled(),                      // so the reason cannot be recovered
	)

	status, err := ec.waitForClusterFinished(ctx, "j-test")
	assert.NotNil(err)
	assert.Contains(err.Error(), "ThrottlingException")
	assert.Greater(svc.calls, 1, "a missing reason is worth re-describing for")

	assert.NotNil(status, "the terminal state the waiter saw must not be discarded")
	assert.Equal(types.ClusterStateTerminated, status.State)
}

// TestWaitForClusterFinished_ReasonRecoveredByRedescribe is the same case when
// the re-describe does get through: the recovered reason wins.
func TestWaitForClusterFinished_ReasonRecoveredByRedescribe(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()

	ec, _ := scriptedEmrCluster(
		describeOK(types.ClusterStateTerminated), // the waiter resolves, but with no reason
		describeOKWithReason(types.ClusterStateTerminated,
			types.ClusterStateChangeReasonCodeAllStepsCompleted),
	)

	status, err := ec.waitForClusterFinished(ctx, "j-test")
	assert.Nil(err)
	assert.NotNil(status)
	assert.Equal(types.ClusterStateChangeReasonCodeAllStepsCompleted, status.StateChangeReason.Code)
}

// TestWaitForClusterTerminated_SustainedThrottlingStillFails checks that the
// tolerance has a limit: throttling that outlasts the retry budget, with no
// status ever observed, is reported rather than guessed at.
func TestWaitForClusterTerminated_SustainedThrottlingStillFails(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()

	ec, svc := scriptedEmrCluster(describeThrottled())

	err := ec.waitForClusterTerminated(ctx, "j-test")
	assert.NotNil(err)
	assert.Contains(err.Error(), "ThrottlingException")
	assert.Equal(emrLongRetryAttempts, svc.calls)
}

// TestTerminateJobFlowNoWait checks that abandoning a cluster does not wait for
// it to go. The callers on that path have already decided the run failed and are
// only stopping the billing; waiting would delay reporting the failure by up to
// clusterWaitMaxDuration to confirm something nobody is waiting to hear.
func TestTerminateJobFlowNoWait(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()

	ec, svc := scriptedEmrCluster(describeOK(types.ClusterStateStarting))

	assert.Nil(ec.TerminateJobFlowNoWait(ctx, "j-test"))
	assert.Equal(1, svc.terminateCalls)
	assert.Zero(svc.calls, "abandoning a cluster must not describe it afterwards")
}

// TestDescribeCarriesRaisedRetryBudget checks that the raised SDK retry budget
// reaches the describes that need it. Its counterpart is
// TestLaunchDoesNotCarryRaisedRetryBudget in transient_test.go: the budget is
// passed per call precisely so that it does not reach RunJobFlow.
func TestDescribeCarriesRaisedRetryBudget(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()

	ec, svc := scriptedEmrCluster(describeOK(types.ClusterStateTerminated))

	assert.Nil(ec.waitForClusterTerminated(ctx, "j-test"))
	assert.Equal(emrIdempotentRetryMaxAttempts, svc.lastOptions.RetryMaxAttempts)
}

// TestDescribeSurvivesTransientNonThrottleError covers the reason a non-throttle
// error gets a few attempts rather than none. EMR can briefly fail to recognise
// a jobflow ID that RunJobFlow has only just returned, and on the transient path
// treating that as fatal abandons a cluster that is about to run the playbook.
func TestDescribeSurvivesTransientNonThrottleError(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()

	ec, svc := scriptedEmrCluster(
		describeUnknownCluster(),
		describeOK(types.ClusterStateTerminated),
	)

	assert.Nil(ec.waitForClusterTerminated(ctx, "j-test"))
	assert.Equal(2, svc.calls)
}

// TestWaitForClusterFailsFastOnUnknownCluster covers the reason the existence
// check runs ahead of a waiter: a jobflow ID that does not exist should be
// reported in seconds, not waited on. Nothing about that answer improves with
// waiting, so unlike a throttle it gets only the short budget and never reaches
// the waiter.
//
// waitForClusterTerminated is the only one left with such a check, and the only
// one that needs it: `down` takes --emr-cluster from the operator, where the
// other two are only ever passed an ID RunJobFlow just issued.
func TestWaitForClusterFailsFastOnUnknownCluster(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()

	ec, svc := scriptedEmrCluster(describeUnknownCluster())

	err := ec.waitForClusterTerminated(ctx, "j-nope")
	assert.NotNil(err)
	assert.Contains(err.Error(), "is not valid")
	assert.Equal(emrShortRetryAttempts, svc.calls,
		"an unknown cluster must not consume the throttle budget")
	assert.Less(emrShortRetryAttempts, emrLongRetryAttempts)
}

// TestBlindTracker exercises the policy behind the blind-poll warning: quiet for
// the occasional failed describe, loud once enough consecutive polls have failed
// to be worth acting on, and not repeatedly in between.
//
// Counted in polls, so that the threshold means the same on the launch waiter at
// 30-90s and the job waiter at 60-300s. The times below are arbitrary — only the
// elapsed figure reported back to the operator depends on them.
func TestBlindTracker(t *testing.T) {
	assert := assert.New(t)

	var b blindTracker
	t0 := time.Unix(0, 0)
	at := func(minutes int) time.Time { return t0.Add(time.Duration(minutes) * time.Minute) }
	boom := errors.New("throttled")

	// polls short of the threshold say nothing — the waiter simply polls again
	for i := 1; i < blindWarnAfterPolls; i++ {
		assert.False(b.observe(at(i), boom, true).warn, "poll %d should be silent", i)
	}

	// the threshold poll does, and reports how long it has been going on
	state := b.observe(at(blindWarnAfterPolls), boom, true)
	assert.True(state.warn)
	assert.Equal(time.Duration(blindWarnAfterPolls-1)*time.Minute, state.blindFor)

	// then nothing again until the repeat interval, however long that takes
	for i := blindWarnAfterPolls + 1; i < blindWarnAfterPolls+blindWarnEveryPolls; i++ {
		assert.False(b.observe(at(i), boom, true).warn, "poll %d should be silent", i)
	}
	assert.True(b.observe(at(blindWarnAfterPolls+blindWarnEveryPolls), boom, true).warn)

	// recovery is worth one line, since a warning went out
	assert.True(b.observe(at(100), nil, false).recovered)

	// a success when nothing had gone wrong is not
	assert.False(b.observe(at(101), nil, false).recovered)

	// and the next failure starts a fresh count rather than resuming the old one
	assert.False(b.observe(at(102), boom, true).warn)
}

// TestBlindTrackerGivesUp covers the bound on unbroken blindness. The job wait is
// bounded in days, and past a certain point we are no longer waiting on an outage
// that might clear — the cluster has long since finished or died either way.
func TestBlindTrackerGivesUp(t *testing.T) {
	assert := assert.New(t)

	var b blindTracker
	t0 := time.Unix(0, 0)
	boom := errors.New("throttled")

	assert.False(b.observe(t0, boom, true).giveUp)
	assert.False(b.observe(t0.Add(maxBlindDuration-time.Second), boom, true).giveUp,
		"an outage that might still clear is worth waiting out")
	assert.True(b.observe(t0.Add(maxBlindDuration), boom, true).giveUp)

	// a spell that recovers leaves nothing behind to trip the next one
	b.observe(t0.Add(maxBlindDuration), nil, false)
	assert.False(b.observe(t0.Add(2*maxBlindDuration), boom, true).giveUp)
}

// TestWarnWhileBlindPassesTheDecisionThrough checks the wrapper only observes:
// the waiter's own verdict, including its error, must reach it unaltered.
func TestWarnWhileBlindPassesTheDecisionThrough(t *testing.T) {
	assert := assert.New(t)

	inner := func(ctx context.Context, in *emr.DescribeClusterInput, out *emr.DescribeClusterOutput, err error) (bool, error) {
		return true, errors.New("verdict")
	}

	throttled := &smithy.GenericAPIError{Code: "ThrottlingException", Message: "Rate exceeded"}
	retryable, err := warnWhileBlind("j-test", inner)(context.Background(), nil, nil, throttled)
	assert.True(retryable)
	assert.EqualError(err, "verdict")
}

// TestBlindTrackerGivesUpOnNonThrottleErrors covers the escape hatch. A waiter
// swallows API errors and polls again, which is what stops a throttle failing a
// run — but an error that will never clear, a role without DescribeCluster being
// the likely one, would otherwise hold the waiter for its whole bound.
func TestBlindTrackerGivesUpOnNonThrottleErrors(t *testing.T) {
	assert := assert.New(t)

	t0 := time.Unix(0, 0)
	at := func(seconds int) time.Time { return t0.Add(time.Duration(seconds) * time.Second) }
	denied := errors.New("api error AccessDeniedException")
	throttled := errors.New("throttled")

	var b blindTracker
	for i := 1; i < blindGiveUpAfterPolls; i++ {
		assert.False(b.observe(at(i), denied, false).giveUp,
			"poll %d is still within the window EMR needs to recognise a new jobflow ID", i)
	}
	assert.True(b.observe(at(blindGiveUpAfterPolls), denied, false).giveUp)

	// Throttling keeps the whole wait. A throttle among the failures also clears
	// the count, since it shows the API is answering.
	var c blindTracker
	for i := 1; i < blindGiveUpAfterPolls*10; i++ {
		assert.False(c.observe(at(i), throttled, true).giveUp, "poll %d", i)
	}

	var d blindTracker
	for i := 1; i < blindGiveUpAfterPolls; i++ {
		d.observe(at(i), denied, false)
	}
	d.observe(at(blindGiveUpAfterPolls), throttled, true)
	assert.False(d.observe(at(blindGiveUpAfterPolls+1), denied, false).giveUp,
		"an answer we could reach should reset the count of hopeless polls")
}

// TestSDKRetryAssumptions guards two properties of the SDK that the retry design
// depends on and that a version bump could quietly change. Both are argued for in
// comments; this is what would actually notice.
func TestSDKRetryAssumptions(t *testing.T) {
	assert := assert.New(t)

	// emrIdempotentRetryOptions raises MaxAttempts per call and expects the adaptive
	// rate limiter to survive it. It does only because AddWithMaxAttempts wraps
	// the retryer as a RetryerV2 rather than replacing it.
	wrapped := awsretry.AddWithMaxAttempts(awsretry.NewAdaptiveMode(), emrIdempotentRetryMaxAttempts)
	assert.Equal(emrIdempotentRetryMaxAttempts, wrapped.MaxAttempts())
	_, isV2 := wrapped.(aws.RetryerV2)
	assert.True(isV2, "the adaptive rate limiter is lost if the wrapper is not a RetryerV2")

	// An exhausted adaptive token bucket must sleep, not error. Were it to error,
	// the error is a bare fmt.Errorf that errorMightClear cannot recognise — which
	// since the waiters gained their give-up would abort a wait, not merely drop a
	// throttle onto the short budget.
	assert.False(awsretry.AdaptiveModeOptions{}.FailOnNoAttemptTokens)

	// errorMightClear decides whether a wait continues, so what it recognises is
	// load-bearing for liveness rather than just for patience. A throttle must
	// read as recoverable in both the shapes production produces: bare, and
	// wrapped in the MaxAttemptsError the SDK returns once its own retries are
	// spent. So must a dropped connection and a 5xx; an answer AWS actually gave
	// us must not.
	throttled := &smithy.GenericAPIError{Code: "ThrottlingException", Message: "Rate exceeded"}
	assert.True(errorMightClear(throttled), "bare throttle")
	assert.True(errorMightClear(&awsretry.MaxAttemptsError{Attempt: 3, Err: throttled}), "wrapped throttle")
	assert.True(errorMightClear(&smithy.GenericAPIError{Code: "RequestTimeout"}), "timeout")
	assert.False(errorMightClear(&smithy.GenericAPIError{Code: "AccessDeniedException"}), "denied")
	assert.False(errorMightClear(&smithy.GenericAPIError{Code: "InvalidRequestException"}), "unknown cluster")
}
