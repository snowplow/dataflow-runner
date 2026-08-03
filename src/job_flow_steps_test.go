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
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/emr"
	"github.com/aws/aws-sdk-go-v2/service/emr/types"
	"github.com/stretchr/testify/assert"
)

type mockEMRAPISteps struct{}

func (m *mockEMRAPISteps) AddJobFlowSteps(ctx context.Context, input *emr.AddJobFlowStepsInput, optFns ...func(*emr.Options)) (*emr.AddJobFlowStepsOutput, error) {
	if !strings.HasPrefix(*input.JobFlowId, "j-") {
		return nil, errors.New("AddJobFlowSteps failed")
	}
	return &emr.AddJobFlowStepsOutput{
		StepIds: []string{"1"},
	}, nil
}

// Mock using the cluster id of input to set the step State
// ClusterId = "j-PENDING" will result in a step with the PENDING state
func (m *mockEMRAPISteps) DescribeStep(ctx context.Context, input *emr.DescribeStepInput, optFns ...func(*emr.Options)) (*emr.DescribeStepOutput, error) {
	if !strings.HasPrefix(*input.ClusterId, "j-") {
		return nil, errors.New("DescribeStep failed")
	}
	var state types.StepState
	var states = []types.StepState{
		types.StepStatePending,
		types.StepStateCancelPending,
		types.StepStateRunning,
		types.StepStateCompleted,
		types.StepStateCancelled,
		types.StepStateFailed,
		types.StepStateInterrupted,
	}
	for _, e := range states {
		if strings.Contains(*input.ClusterId, string(e)) {
			state = e
			break
		}
	}
	if state == "" {
		return nil, errors.New("DescribeStep failed")
	}
	testTime := time.Date(2019, time.October, 10, 23, 0, 0, 0, time.UTC)
	return &emr.DescribeStepOutput{
		Step: &types.Step{
			Name: aws.String("step"),
			Id:   aws.String("step-id"),
			Status: &types.StepStatus{
				State: state,
				Timeline: &types.StepTimeline{
					StartDateTime: &testTime,
					EndDateTime:   &testTime,
				},
			},
		},
	}, nil
}

func (m *mockEMRAPISteps) RunJobFlow(ctx context.Context, input *emr.RunJobFlowInput, optFns ...func(*emr.Options)) (*emr.RunJobFlowOutput, error) {
	return nil, nil
}

func (m *mockEMRAPISteps) TerminateJobFlows(ctx context.Context, input *emr.TerminateJobFlowsInput, optFns ...func(*emr.Options)) (*emr.TerminateJobFlowsOutput, error) {
	return nil, nil
}

func (m *mockEMRAPISteps) DescribeCluster(ctx context.Context, input *emr.DescribeClusterInput, optFns ...func(*emr.Options)) (*emr.DescribeClusterOutput, error) {
	return nil, nil
}

func (m *mockEMRAPISteps) ListSteps(ctx context.Context, input *emr.ListStepsInput, optFns ...func(*emr.Options)) (*emr.ListStepsOutput, error) {
	return nil, nil
}

func mockJobFlowSteps(playbookConfig PlaybookConfig, jobflowID string) *JobFlowSteps {
	return &JobFlowSteps{
		Config:     playbookConfig,
		JobflowID:  jobflowID,
		IsBlocking: true,
		EmrSvc:     &mockEMRAPISteps{},
	}
}

func mockJobFlowStepsWithoutPlaybook(jobflowID string) *JobFlowSteps {
	record, _ := CR.ParsePlaybookRecord([]byte(PlaybookRecord1), nil, "")
	return mockJobFlowSteps(*record, jobflowID)
}

func TestInitJobFlowSteps(t *testing.T) {
	assert := assert.New(t)

	record, _ := CR.ParsePlaybookRecord([]byte(PlaybookRecord1), nil, "")

	jfs, _ := InitJobFlowSteps(*record, "j-id", true)
	assert.NotNil(jfs)

	record.Credentials.SecretAccessKey = "hello"
	_, err := InitJobFlowSteps(*record, "j-id", true)
	assert.NotNil(err)
	assert.Equal("access-key and secret-key must both be set to 'env', or neither", err.Error())

	record.Credentials.AccessKeyId = "iam"
	_, err = InitJobFlowSteps(*record, "j-id", true)
	assert.NotNil(err)
	assert.Equal("access-key and secret-key must both be set to 'iam', or neither", err.Error())

	record.Credentials.SecretAccessKey = "iam"
	jfs, _ = InitJobFlowSteps(*record, "j-id", true)
	assert.NotNil(jfs)
}

func TestAddJobFlowSteps_Fail(t *testing.T) {
	assert := assert.New(t)
	record, _ := CR.ParsePlaybookRecord([]byte(PlaybookRecord1), nil, "")
	jfs := mockJobFlowSteps(*record, "id")

	// fails if emr.AddJobFlowSteps fails
	_, err := jfs.AddJobFlowSteps()
	assert.NotNil(err)
	assert.Equal("emr.AddJobFlowSteps: AddJobFlowSteps failed", err.Error())

	// fails if DescribeStep fails
	jfs.JobflowID = "j-123"
	_, err = jfs.AddJobFlowSteps()
	assert.NotNil(err)
	assert.Equal("Couldn't retrieve step 1 state: emr.DescribeStep: DescribeStep failed", err.Error())

	// fails if the number of errors is > 0
	stepID := "step-id"
	jfs.JobflowID = "j-FAILED-gz"
	tmpDirInput := filepath.Join("tmp-gz", "log", jfs.JobflowID, "steps", stepID)
	os.MkdirAll(tmpDirInput, 0755)
	content := "test.gz"
	filename := "test"
	WriteGzFile(filename, tmpDirInput, content)
	_, err = jfs.AddJobFlowSteps()
	assert.NotNil(err)
	assert.Equal("1/1 steps failed to complete successfully", err.Error())

	// fails if GetJobFlowStepsInput fails
	jfs.Config.Steps = []*StepsRecord{}
	_, err = jfs.AddJobFlowSteps()
	assert.NotNil(err)
	assert.Equal("No steps found in config, nothing to add", err.Error())
}

func TestAddJobFlowSteps_Success(t *testing.T) {
	record, _ := CR.ParsePlaybookRecord([]byte(PlaybookRecord1), nil, "")
	jfs := mockJobFlowSteps(*record, "j-COMPLETED")
	_, err := jfs.AddJobFlowSteps()
	assert.Nil(t, err)
}

func TestGetJobFlowStepsInput_Success(t *testing.T) {
	assert := assert.New(t)

	record, _ := CR.ParsePlaybookRecord([]byte(PlaybookRecord1), nil, "")
	jfs, _ := InitJobFlowSteps(*record, "jobflow-id", true)

	assert.NotNil(jfs)

	res, err := jfs.GetJobFlowStepsInput()
	assert.NotNil(res)
	assert.Nil(err)
	assert.Equal("jobflow-id", *res.JobFlowId)
	assert.Equal(2, len(res.Steps))
}

func TestGetJobFlowStepsInput_Fail(t *testing.T) {
	assert := assert.New(t)

	record, _ := CR.ParsePlaybookRecord([]byte(PlaybookRecord1), nil, "")
	jfs, _ := InitJobFlowSteps(*record, "jobflow-id", true)

	assert.NotNil(jfs)

	jfs.Config.Steps[0].ActionOnFailure = "TERMINATE_CLUSTER"

	res, err := jfs.GetJobFlowStepsInput()
	assert.Nil(res)
	assert.NotNil(err)
	assert.Equal("Only the following failure actions are allowed 'CANCEL_AND_WAIT, CONTINUE' - to terminate use the 'down' command", err.Error())

	jfs.Config.Steps = nil

	res, err = jfs.GetJobFlowStepsInput()
	assert.Nil(res)
	assert.NotNil(err)
	assert.Equal("No steps found in config, nothing to add", err.Error())
}
func TestRetrieveStepsStates(t *testing.T) {
	assert := assert.New(t)

	jfs := mockJobFlowStepsWithoutPlaybook("j-COMPLETED")
	successCount, failureCount, failedStepsIds, infoLogs, errorLogs, err := jfs.RetrieveStepsStates([]string{"step-id"})
	assert.Equal(1, successCount)
	assert.Equal(0, failureCount)
	assert.NotNil(failedStepsIds)
	assert.Equal(0, len(failedStepsIds))
	assert.NotNil(infoLogs)
	assert.Equal([]string{"Step 'step' with id 'step-id' completed successfully - StartTime: 2019-10-10T23:00:00Z - EndTime: 2019-10-10T23:00:00Z"}, infoLogs)
	assert.NotNil(errorLogs)
	assert.Equal(0, len(errorLogs))
	assert.Nil(err)

	jfs = mockJobFlowStepsWithoutPlaybook("j-CANCELLED")
	successCount, failureCount, failedStepsIds, infoLogs, errorLogs, err = jfs.RetrieveStepsStates([]string{"step-id"})
	assert.Equal(0, successCount)
	assert.Equal(1, failureCount)
	assert.NotNil(failedStepsIds)
	assert.Equal(0, len(failedStepsIds))
	assert.NotNil(infoLogs)
	assert.Equal(0, len(infoLogs))
	assert.NotNil(errorLogs)
	assert.Equal([]string{"Step 'step' with id 'step-id' was CANCELLED"}, errorLogs)
	assert.Nil(err)
}

func TestRetrieveStepsStates_Fail(t *testing.T) {
	assert := assert.New(t)

	// fails if one DescribeStep fails
	jfs := mockJobFlowStepsWithoutPlaybook("j-NOTHING")
	successCount, failureCount, failedStepsIds, infoLogs, errorLogs, err := jfs.RetrieveStepsStates([]string{"step-id"})
	assert.Equal(0, successCount)
	assert.Equal(0, failureCount)
	assert.Nil(failedStepsIds)
	assert.Nil(infoLogs)
	assert.Nil(errorLogs)
	assert.NotNil(err)
	assert.Equal("Couldn't retrieve step step-id state: emr.DescribeStep: DescribeStep failed", err.Error())
}

func TestRetrieveStepState(t *testing.T) {
	assert := assert.New(t)
	stepID := "step-id"

	// log completed steps
	jfs := mockJobFlowStepsWithoutPlaybook("j-COMPLETED")
	state, logs, err := jfs.RetrieveStepState(stepID)
	assert.Equal(types.StepStateCompleted, state)
	assert.NotNil(logs)
	assert.Equal([]string{"Step 'step' with id 'step-id' completed successfully - StartTime: 2019-10-10T23:00:00Z - EndTime: 2019-10-10T23:00:00Z"}, logs)
	assert.Nil(err)

	// log cancelled steps
	jfs = mockJobFlowStepsWithoutPlaybook("j-CANCELLED")
	state, logs, err = jfs.RetrieveStepState(stepID)
	assert.Equal(types.StepStateCancelled, state)
	assert.NotNil(logs)
	assert.Equal([]string{"Step 'step' with id 'step-id' was CANCELLED"}, logs)
	assert.Nil(err)

	// outputs the failed step log
	jfs = mockJobFlowStepsWithoutPlaybook("j-FAILED")
	state, logs, err = jfs.RetrieveStepState(stepID)
	assert.Equal(types.StepStateFailed, state)
	assert.NotNil(logs)
	assert.Equal([]string{"Step 'step' with id 'step-id' was FAILED - StartTime: 2019-10-10T23:00:00Z - EndTime: 2019-10-10T23:00:00Z"}, logs)
	assert.Nil(err)

	// ignores steps that are running
	jfs = mockJobFlowStepsWithoutPlaybook("j-RUNNING")
	state, logs, err = jfs.RetrieveStepState(stepID)
	assert.Equal(types.StepStateRunning, state)
	assert.Equal([]string{}, logs)
	assert.Nil(err)
}

func TestRetrieveStepState_Fail(t *testing.T) {
	assert := assert.New(t)
	stepID := "step-id"

	// fails if DescribeStep fails
	jfs := mockJobFlowStepsWithoutPlaybook("j-nothing")
	state, logs, err := jfs.RetrieveStepState(stepID)
	assert.Equal(types.StepState(""), state)
	assert.Nil(logs)
	assert.NotNil(err)
	assert.Equal("Couldn't retrieve step step-id state: emr.DescribeStep: DescribeStep failed", err.Error())
}

// mockEMRAPIStepStates lists a fixed set of steps and reports each one's state by
// step ID, so a mixture of terminal and non-terminal steps can be exercised.
type mockEMRAPIStepStates struct {
	stepIDs []string
	states  map[string]types.StepState
}

func (m *mockEMRAPIStepStates) ListSteps(ctx context.Context, input *emr.ListStepsInput, optFns ...func(*emr.Options)) (*emr.ListStepsOutput, error) {
	if !strings.HasPrefix(*input.ClusterId, "j-") {
		return nil, errors.New("ListSteps failed")
	}
	steps := make([]types.StepSummary, 0, len(m.stepIDs))
	for _, id := range m.stepIDs {
		steps = append(steps, types.StepSummary{Id: aws.String(id)})
	}
	return &emr.ListStepsOutput{Steps: steps}, nil
}

func (m *mockEMRAPIStepStates) DescribeStep(ctx context.Context, input *emr.DescribeStepInput, optFns ...func(*emr.Options)) (*emr.DescribeStepOutput, error) {
	testTime := time.Date(2019, time.October, 10, 23, 0, 0, 0, time.UTC)
	return &emr.DescribeStepOutput{
		Step: &types.Step{
			Name: aws.String("step"),
			Id:   input.StepId,
			Status: &types.StepStatus{
				State: m.states[*input.StepId],
				Timeline: &types.StepTimeline{
					StartDateTime: &testTime,
					EndDateTime:   &testTime,
				},
			},
		},
	}, nil
}

func (m *mockEMRAPIStepStates) RunJobFlow(ctx context.Context, input *emr.RunJobFlowInput, optFns ...func(*emr.Options)) (*emr.RunJobFlowOutput, error) {
	return nil, nil
}

func (m *mockEMRAPIStepStates) TerminateJobFlows(ctx context.Context, input *emr.TerminateJobFlowsInput, optFns ...func(*emr.Options)) (*emr.TerminateJobFlowsOutput, error) {
	return nil, nil
}

func (m *mockEMRAPIStepStates) DescribeCluster(ctx context.Context, input *emr.DescribeClusterInput, optFns ...func(*emr.Options)) (*emr.DescribeClusterOutput, error) {
	return nil, nil
}

func (m *mockEMRAPIStepStates) AddJobFlowSteps(ctx context.Context, input *emr.AddJobFlowStepsInput, optFns ...func(*emr.Options)) (*emr.AddJobFlowStepsOutput, error) {
	return nil, nil
}

func TestFailedStepIDs(t *testing.T) {
	assert := assert.New(t)
	record, _ := CR.ParsePlaybookRecord([]byte(PlaybookRecord1), nil, "")

	// One step failed, one completed, and one is INTERRUPTED — which
	// GetFailedStepIDs would wait on forever, since it counts INTERRUPTED as
	// neither success nor failure. FailedStepIDs must return in a single pass
	// with just the failed step.
	svc := &mockEMRAPIStepStates{
		stepIDs: []string{"s-DONE", "s-FAIL", "s-STUCK"},
		states: map[string]types.StepState{
			"s-DONE":  types.StepStateCompleted,
			"s-FAIL":  types.StepStateFailed,
			"s-STUCK": types.StepStateInterrupted,
		},
	}
	jfs := &JobFlowSteps{Config: *record, JobflowID: "j-123", IsBlocking: true, EmrSvc: svc}

	failedStepIDs, err := jfs.FailedStepIDs()
	assert.Nil(err)
	assert.Equal([]string{"s-FAIL"}, failedStepIDs)

	// an undescribable cluster surfaces the error rather than an empty list
	jfs = &JobFlowSteps{Config: *record, JobflowID: "nope", IsBlocking: true, EmrSvc: svc}
	failedStepIDs, err = jfs.FailedStepIDs()
	assert.NotNil(err)
	assert.Nil(failedStepIDs)
}
