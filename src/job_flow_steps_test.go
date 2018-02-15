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
	"os"
	"path/filepath"
	"testing"

	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/emr"
	"github.com/aws/aws-sdk-go/service/emr/emriface"
	"github.com/stretchr/testify/assert"
)

type mockEMRAPISteps struct {
	emriface.EMRAPI
}

func (m *mockEMRAPISteps) AddJobFlowSteps(input *emr.AddJobFlowStepsInput) (*emr.AddJobFlowStepsOutput, error) {
	if !strings.HasPrefix(*input.JobFlowId, "j-") {
		return nil, errors.New("AddJobFlowSteps failed")
	}
	return &emr.AddJobFlowStepsOutput{
		StepIds: []*string{aws.String("1")},
	}, nil
}

// Mock using the cluster id of input to set the step State
// ClusterId = "j-PENDING" will result in a step with the PENDING state
func (m *mockEMRAPISteps) DescribeStep(input *emr.DescribeStepInput) (*emr.DescribeStepOutput, error) {
	if !strings.HasPrefix(*input.ClusterId, "j-") {
		return nil, errors.New("DescribeStep failed")
	}
	var state string
	var states = []string{"PENDING", "CANCEL_PENDING", "RUNNING", "COMPLETED", "CANCELLED",
		"FAILED", "INTERRUPTED"}
	for _, e := range states {
		if strings.Contains(*input.ClusterId, e) {
			state = e
			break
		}
	}
	if state == "" {
		return nil, errors.New("DescribeStep failed")
	}
	return &emr.DescribeStepOutput{
		Step: &emr.Step{
			Name: aws.String("step"),
			Id:   aws.String("step-id"),
			Status: &emr.StepStatus{
				State: aws.String(state),
			},
		},
	}, nil
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
	assert.Equal("AddJobFlowSteps failed", err.Error())

	// fails if DescribeStep fails
	jfs.JobflowID = "j-123"
	_, err = jfs.AddJobFlowSteps()
	assert.NotNil(err)
	assert.Equal("Couldn't retrieve step 1 state: DescribeStep failed", err.Error())

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
	ajfso := &emr.AddJobFlowStepsOutput{StepIds: []*string{aws.String("step-id")}}

	jfs := mockJobFlowStepsWithoutPlaybook("j-COMPLETED")
	successCount, failureCount, failedStepsIds, infoLogs, errorLogs, err := jfs.RetrieveStepsStates(ajfso)
	assert.Equal(1, successCount)
	assert.Equal(0, failureCount)
	assert.NotNil(failedStepsIds)
	assert.Equal(0, len(failedStepsIds))
	assert.NotNil(infoLogs)
	assert.Equal([]string{"Step 'step' with id 'step-id' completed successfully"}, infoLogs)
	assert.NotNil(errorLogs)
	assert.Equal(0, len(errorLogs))
	assert.Nil(err)

	jfs = mockJobFlowStepsWithoutPlaybook("j-CANCELLED")
	successCount, failureCount, failedStepsIds, infoLogs, errorLogs, err = jfs.RetrieveStepsStates(ajfso)
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
	ajfso := &emr.AddJobFlowStepsOutput{StepIds: []*string{aws.String("step-id")}}

	// fails if one DescribeStep fails
	jfs := mockJobFlowStepsWithoutPlaybook("j-NOTHING")
	successCount, failureCount, failedStepsIds, infoLogs, errorLogs, err := jfs.RetrieveStepsStates(ajfso)
	assert.Equal(0, successCount)
	assert.Equal(0, failureCount)
	assert.Nil(failedStepsIds)
	assert.Nil(infoLogs)
	assert.Nil(errorLogs)
	assert.NotNil(err)
	assert.Equal("Couldn't retrieve step step-id state: DescribeStep failed", err.Error())
}

func TestRetrieveStepState(t *testing.T) {
	assert := assert.New(t)
	stepID := "step-id"

	// log completed steps
	jfs := mockJobFlowStepsWithoutPlaybook("j-COMPLETED")
	state, logs, err := jfs.RetrieveStepState(stepID)
	assert.Equal("COMPLETED", state)
	assert.NotNil(logs)
	assert.Equal([]string{"Step 'step' with id 'step-id' completed successfully"}, logs)
	assert.Nil(err)

	// log cancelled steps
	jfs = mockJobFlowStepsWithoutPlaybook("j-CANCELLED")
	state, logs, err = jfs.RetrieveStepState(stepID)
	assert.Equal("CANCELLED", state)
	assert.NotNil(logs)
	assert.Equal([]string{"Step 'step' with id 'step-id' was CANCELLED"}, logs)
	assert.Nil(err)

	// outputs the failed step log
	jfs = mockJobFlowStepsWithoutPlaybook("j-FAILED")
	state, logs, err = jfs.RetrieveStepState(stepID)
	assert.Equal("FAILED", state)
	assert.NotNil(logs)
	assert.Equal([]string{"Step 'step' with id 'step-id' was FAILED"}, logs)
	assert.Nil(err)

	// ignores steps that are running
	jfs = mockJobFlowStepsWithoutPlaybook("j-RUNNING")
	state, logs, err = jfs.RetrieveStepState(stepID)
	assert.Equal("RUNNING", state)
	assert.Nil(logs)
	assert.Nil(err)
}

func TestRetrieveStepState_Fail(t *testing.T) {
	assert := assert.New(t)
	stepID := "step-id"

	// fails if DescribeStep fails
	jfs := mockJobFlowStepsWithoutPlaybook("j-nothing")
	state, logs, err := jfs.RetrieveStepState(stepID)
	assert.Equal("", state)
	assert.Nil(logs)
	assert.NotNil(err)
	assert.Equal("Couldn't retrieve step step-id state: DescribeStep failed", err.Error())
}
