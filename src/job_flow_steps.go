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
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/emr"
	"github.com/aws/aws-sdk-go-v2/service/emr/types"
	log "github.com/sirupsen/logrus"
	"github.com/snowplow-devops/go-retry"
)

// JobFlowSteps is used for adding steps to an existing cluster
type JobFlowSteps struct {
	Config     PlaybookConfig
	JobflowID  string
	IsBlocking bool
	EmrSvc     EMRAPI
}

// InitJobFlowSteps creates a new JobFlowSteps instance
func InitJobFlowSteps(playbookConfig PlaybookConfig, jobflowID string, isAsync bool) (*JobFlowSteps, error) {
	creds, err := GetCredentialsProvider(
		playbookConfig.Credentials.AccessKeyId, playbookConfig.Credentials.SecretAccessKey)
	if err != nil {
		return nil, err
	}

	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion(playbookConfig.Region),
		config.WithCredentialsProvider(creds),
	)
	if err != nil {
		return nil, err
	}

	emrSvc := emr.NewFromConfig(cfg)

	return &JobFlowSteps{
		Config:     playbookConfig,
		JobflowID:  jobflowID,
		IsBlocking: !isAsync,
		EmrSvc:     emrSvc,
	}, nil
}

func (jfs JobFlowSteps) GetFailedStepIDs() ([]string, error) {
	return jfs.GetFailedStepIDsWithContext(context.Background())
}

func (jfs JobFlowSteps) GetFailedStepIDsWithContext(ctx context.Context) ([]string, error) {
	stepIDs, err := jfs.GetStepIDsWithContext(ctx)
	if err != nil {
		return nil, err
	}

	done := false
	errorCount := 0
	failedStepsIDs := []string{}
	historicalInfoLogs := []string{}
	historicalErrorLogs := []string{}

	for !done && jfs.IsBlocking {
		successCount, errCount, fStepsIDs, infoLogs, errorLogs, err :=
			jfs.RetrieveStepsStatesWithContext(ctx, stepIDs)
		if err != nil {
			return nil, err
		}
		errorCount = errCount

		for _, l := range Diff(historicalInfoLogs, infoLogs) {
			log.Info(l)
		}
		for _, l := range Diff(historicalErrorLogs, errorLogs) {
			log.Error(l)
		}
		historicalInfoLogs = infoLogs
		historicalErrorLogs = errorLogs

		if (successCount + errorCount) == len(stepIDs) {
			done = true
			failedStepsIDs = fStepsIDs
		} else {
			time.Sleep(time.Second * 15)
			failedStepsIDs = []string{}
		}
	}

	if errorCount == 0 {
		return nil, nil
	}
	return failedStepsIDs, fmt.Errorf("%d/%d steps failed to complete successfully",
		errorCount, len(stepIDs))
}

// AddJobFlowSteps builds the parameters and then submits them to the running EMR cluster, returns
// the ids of the failed steps
func (jfs JobFlowSteps) AddJobFlowSteps() ([]string, error) {
	return jfs.AddJobFlowStepsWithContext(context.Background())
}

// AddJobFlowStepsWithContext builds the parameters and then submits them to the running EMR cluster with context support
func (jfs JobFlowSteps) AddJobFlowStepsWithContext(ctx context.Context) ([]string, error) {
	params, err := jfs.GetJobFlowStepsInput()
	if err != nil {
		return nil, err
	}

	done := false
	errorCount := 0
	failedStepsIDs := []string{}
	historicalInfoLogs := []string{}
	historicalErrorLogs := []string{}

	resp, err := retry.ExponentialWithInterface(3, time.Second, "emr.AddJobFlowSteps", func() (any, error) {
		return jfs.EmrSvc.AddJobFlowSteps(ctx, params)
	})
	if err != nil {
		return nil, err
	}

	addJobFlowStepsOutput := resp.(*emr.AddJobFlowStepsOutput)
	log.Infof("Successfully added %d steps to the EMR cluster with jobflow id '%s'...",
		len(jfs.Config.Steps), jfs.JobflowID)

	for !done && jfs.IsBlocking {
		successCount, errCount, fStepsIDs, infoLogs, errorLogs, err :=
			jfs.RetrieveStepsStatesWithContext(ctx, addJobFlowStepsOutput.StepIds)
		if err != nil {
			return nil, err
		}
		errorCount = errCount

		for _, l := range Diff(historicalInfoLogs, infoLogs) {
			log.Info(l)
		}
		for _, l := range Diff(historicalErrorLogs, errorLogs) {
			log.Error(l)
		}
		historicalInfoLogs = infoLogs
		historicalErrorLogs = errorLogs

		if (successCount + errorCount) == len(addJobFlowStepsOutput.StepIds) {
			done = true
			failedStepsIDs = fStepsIDs
		} else {
			time.Sleep(time.Second * 15)
			failedStepsIDs = []string{}
		}
	}

	if errorCount == 0 {
		return nil, nil
	}
	return failedStepsIDs, fmt.Errorf("%d/%d steps failed to complete successfully",
		errorCount, len(addJobFlowStepsOutput.StepIds))
}

func (jfs JobFlowSteps) GetStepIDs() ([]string, error) {
	return jfs.GetStepIDsWithContext(context.Background())
}

func (jfs JobFlowSteps) GetStepIDsWithContext(ctx context.Context) ([]string, error) {
	stepIDs := []string{}

	listStepsInput := &emr.ListStepsInput{
		ClusterId: aws.String(jfs.JobflowID),
	}

	resp, err := retry.ExponentialWithInterface(3, time.Second, "emr.ListSteps", func() (any, error) {
		return jfs.EmrSvc.ListSteps(ctx, listStepsInput)
	})
	if err != nil {
		return nil, err
	}

	listStepsOutput := resp.(*emr.ListStepsOutput)
	for _, step := range listStepsOutput.Steps {
		stepIDs = append(stepIDs, *step.Id)
	}

	return stepIDs, nil
}

// RetrieveStepsStates retrieves the states of all the steps for a job flow returning the state
// of every step as well as information about success or failure for each one
func (jfs JobFlowSteps) RetrieveStepsStates(stepIDs []string) (int, int, []string, []string, []string, error) {
	return jfs.RetrieveStepsStatesWithContext(context.Background(), stepIDs)
}

// RetrieveStepsStatesWithContext retrieves the states of all the steps with context support
func (jfs JobFlowSteps) RetrieveStepsStatesWithContext(ctx context.Context, stepIDs []string) (int, int, []string, []string, []string, error) {
	infoLogs := make([]string, 0)
	errorLogs := make([]string, 0)
	failedStepsIDs := make([]string, 0)
	successCount := 0
	errorCount := 0
	for _, stepID := range stepIDs {
		state, logs, err := jfs.RetrieveStepStateWithContext(ctx, stepID)
		if err != nil {
			return 0, 0, nil, nil, nil, err
		}
		if state == types.StepStateCompleted {
			infoLogs = append(infoLogs, logs...)
			successCount++
		}
		if state == types.StepStateFailed || state == types.StepStateCancelled {
			errorLogs = append(errorLogs, logs...)
			errorCount++
			if state == types.StepStateFailed {
				failedStepsIDs = append(failedStepsIDs, stepID)
			}
		}
	}
	return successCount, errorCount, failedStepsIDs, infoLogs, errorLogs, nil
}

// RetrieveStepState retrieves the state of a particular step, optionally retrieving the logs if
// it failed, also returns the step status
func (jfs JobFlowSteps) RetrieveStepState(stepID string) (types.StepState, []string, error) {
	return jfs.RetrieveStepStateWithContext(context.Background(), stepID)
}

// RetrieveStepStateWithContext retrieves the state of a particular step with context support
func (jfs JobFlowSteps) RetrieveStepStateWithContext(ctx context.Context, stepID string) (types.StepState, []string, error) {
	describeStepInput := &emr.DescribeStepInput{
		ClusterId: aws.String(jfs.JobflowID),
		StepId:    aws.String(stepID),
	}
	resp, err := retry.ExponentialWithInterface(3, time.Second, "emr.DescribeStep", func() (any, error) {
		return jfs.EmrSvc.DescribeStep(ctx, describeStepInput)
	})
	if err != nil {
		return "", nil, fmt.Errorf("Couldn't retrieve step %s state: %w", stepID, err)
	}

	dso := resp.(*emr.DescribeStepOutput)
	logs := make([]string, 0)
	logMessageHead := fmt.Sprintf("Step '%s' with id '%s'", *dso.Step.Name, *dso.Step.Id)

	switch dso.Step.Status.State {
	case types.StepStateCompleted:
		logs = append(logs, logMessageHead+" completed successfully"+jfs.CreateStepStartFinishTimeLog(dso))
	case types.StepStateFailed:
		logs = append(logs, logMessageHead+" was FAILED"+jfs.CreateStepStartFinishTimeLog(dso))
	case types.StepStateCancelled:
		logs = append(logs, logMessageHead+" was CANCELLED")
	}

	return dso.Step.Status.State, logs, nil
}

func (jfs JobFlowSteps) CreateStepStartFinishTimeLog(dso *emr.DescribeStepOutput) string {
	timeFormat := "2006-01-02T15:04:05Z"
	return fmt.Sprintf(" - StartTime: %s - EndTime: %s",
		dso.Step.Status.Timeline.StartDateTime.Format(timeFormat),
		dso.Step.Status.Timeline.EndDateTime.Format(timeFormat))
}

// GetJobFlowStepsInput parses the config given to it and
// returns the parameters needed to add steps to an EMR
// cluster
func (jfs JobFlowSteps) GetJobFlowStepsInput() (*emr.AddJobFlowStepsInput, error) {
	if len(jfs.Config.Steps) < 1 {
		return nil, fmt.Errorf("No steps found in config, nothing to add")
	}

	allowedActions := []string{"CANCEL_AND_WAIT", "CONTINUE"}

	steps := make([]types.StepConfig, len(jfs.Config.Steps))
	for i, step := range jfs.Config.Steps {
		arguments := make([]string, len(step.Arguments))
		copy(arguments, step.Arguments)

		hadoopJarStep := types.HadoopJarStepConfig{
			Jar:  aws.String(step.Jar),
			Args: arguments,
		}

		if !StringInSlice(step.ActionOnFailure, allowedActions) {
			return nil, fmt.Errorf("Only the following failure actions are allowed '%s' - to terminate use the 'down' command",
				strings.Join(allowedActions, ", "))
		}

		stepConfig := types.StepConfig{
			Name:            aws.String(step.Name),
			ActionOnFailure: types.ActionOnFailure(step.ActionOnFailure),
			HadoopJarStep:   &hadoopJarStep,
		}

		steps[i] = stepConfig
	}

	params := &emr.AddJobFlowStepsInput{
		JobFlowId: aws.String(jfs.JobflowID),
		Steps:     steps,
	}

	return params, nil
}
