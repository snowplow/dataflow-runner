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
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/emr"
	"github.com/aws/aws-sdk-go/service/emr/emriface"
	"github.com/hashicorp/errwrap"
	log "github.com/sirupsen/logrus"
	"github.com/snowplow-devops/go-retry"
)

// JobFlowSteps is used for adding steps to an existing cluster
type JobFlowSteps struct {
	Config     PlaybookConfig
	JobflowID  string
	IsBlocking bool
	EmrSvc     emriface.EMRAPI
}

// InitJobFlowSteps creates a new JobFlowSteps instance
func InitJobFlowSteps(playbookConfig PlaybookConfig, jobflowID string, isAsync bool) (*JobFlowSteps, error) {
	creds, err := GetCredentialsProvider(
		playbookConfig.Credentials.AccessKeyId, playbookConfig.Credentials.SecretAccessKey)
	if err != nil {
		return nil, err
	}

	emrSvc := emr.New(session.Must(session.NewSession()), &aws.Config{
		Region:      aws.String(playbookConfig.Region),
		Credentials: creds,
	})

	return &JobFlowSteps{
		Config:     playbookConfig,
		JobflowID:  jobflowID,
		IsBlocking: !isAsync,
		EmrSvc:     emrSvc,
	}, nil
}

func (jfs JobFlowSteps) GetFailedStepIDs() ([]string, error) {

	stepIDs, err := jfs.GetStepIDs()
	if err != nil {
		return nil, err
	}

	done := false
	errorCount := 0
	failedStepsIDs := []string{}
	historicalInfoLogs := []string{}
	historicalErrorLogs := []string{}

	for done == false && jfs.IsBlocking == true {
		successCount, errCount, fStepsIDs, infoLogs, errorLogs, err :=
			jfs.RetrieveStepsStates(stepIDs)
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
	return failedStepsIDs, errors.New("" + strconv.Itoa(errorCount) + "/" +
		strconv.Itoa(len(stepIDs)) + " steps failed to complete successfully")
}

// AddJobFlowSteps builds the parameters and then submits them to the running EMR cluster, returns
// the ids of the failed steps
func (jfs JobFlowSteps) AddJobFlowSteps() ([]string, error) {
	params, err := jfs.GetJobFlowStepsInput()
	if err != nil {
		return nil, err
	}

	done := false
	errorCount := 0
	failedStepsIDs := []string{}
	historicalInfoLogs := []string{}
	historicalErrorLogs := []string{}

	addJobFlowStepsOutput, err := retry.ExponentialWithInterface(3, time.Second, "emr.AddJobFlowSteps", func() (interface{}, error) {
		return jfs.EmrSvc.AddJobFlowSteps(params)
	})
	if err != nil {
		return nil, err
	}

	log.Info("Successfully added " + strconv.Itoa(len(jfs.Config.Steps)) +
		" steps to the EMR cluster with jobflow id '" + jfs.JobflowID + "'...")

	for done == false && jfs.IsBlocking == true {
		successCount, errCount, fStepsIDs, infoLogs, errorLogs, err :=
			jfs.RetrieveStepsStates(addJobFlowStepsOutput.(*emr.AddJobFlowStepsOutput).StepIds)
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

		if (successCount + errorCount) == len(addJobFlowStepsOutput.(*emr.AddJobFlowStepsOutput).StepIds) {
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
	return failedStepsIDs, errors.New("" + strconv.Itoa(errorCount) + "/" +
		strconv.Itoa(len(addJobFlowStepsOutput.(*emr.AddJobFlowStepsOutput).StepIds)) + " steps failed to complete successfully")
}

func (jfs JobFlowSteps) GetStepIDs() ([]*string, error) {

	stepIDs := []*string{}

	listStepsInput := &emr.ListStepsInput{
		ClusterId: aws.String(jfs.JobflowID),
	}

	listStepsOutput, err := retry.ExponentialWithInterface(3, time.Second, "emr.ListSteps", func() (interface{}, error) {
		return jfs.EmrSvc.ListSteps(listStepsInput)
	})
	if err != nil {
		return nil, err
	}

	for _, step := range listStepsOutput.(*emr.ListStepsOutput).Steps {
		stepIDs = append(stepIDs, step.Id)
	}

	return stepIDs, nil
}

// RetrieveStepsStates retrieves the states of all the steps for a job flow returning the state
// of every step as well as information about success or failure for each one
func (jfs JobFlowSteps) RetrieveStepsStates(stepIDs []*string) (int, int, []string, []string, []string, error) {
	infoLogs := make([]string, 0)
	errorLogs := make([]string, 0)
	failedStepsIDs := make([]string, 0)
	successCount := 0
	errorCount := 0
	for _, stepID := range stepIDs {
		state, logs, err := jfs.RetrieveStepState(*stepID)
		if err != nil {
			return 0, 0, nil, nil, nil, err
		}
		if state == "COMPLETED" {
			infoLogs = append(infoLogs, logs...)
			successCount++
		}
		if state == "FAILED" || state == "CANCELLED" {
			errorLogs = append(errorLogs, logs...)
			errorCount++
			if state == "FAILED" {
				failedStepsIDs = append(failedStepsIDs, *stepID)
			}
		}
	}
	return successCount, errorCount, failedStepsIDs, infoLogs, errorLogs, nil
}

// RetrieveStepState retrieves the state of a particular step, optionally retrieving the logs if
// it failed, also returns the step status
func (jfs JobFlowSteps) RetrieveStepState(stepID string) (string, []string, error) {
	describeStepInput := &emr.DescribeStepInput{
		ClusterId: aws.String(jfs.JobflowID),
		StepId:    aws.String(stepID),
	}
	dso, err := retry.ExponentialWithInterface(3, time.Second, "emr.DescribeStep", func() (interface{}, error) {
		return jfs.EmrSvc.DescribeStep(describeStepInput)
	})
	if err != nil {
		return "", nil, errwrap.Wrapf("Couldn't retrieve step "+stepID+" state: {{err}}", err)
	}

	logs := make([]string, 0)
	logMessageHead := "Step '" + *dso.(*emr.DescribeStepOutput).Step.Name + "' with id '" + *dso.(*emr.DescribeStepOutput).Step.Id
	if *dso.(*emr.DescribeStepOutput).Step.Status.State == "COMPLETED" {
		logs = append(logs, logMessageHead+"' completed successfully"+jfs.CreateStepStartFinishTimeLog(dso.(*emr.DescribeStepOutput)))
	} else if *dso.(*emr.DescribeStepOutput).Step.Status.State == "FAILED" {
		logs = append(logs, logMessageHead+"' was FAILED"+jfs.CreateStepStartFinishTimeLog(dso.(*emr.DescribeStepOutput)))
	} else if *dso.(*emr.DescribeStepOutput).Step.Status.State == "CANCELLED" {
		logs = append(logs, logMessageHead+"' was CANCELLED")
	}
	return *dso.(*emr.DescribeStepOutput).Step.Status.State, logs, nil
}

func (jfs JobFlowSteps) CreateStepStartFinishTimeLog(dso *emr.DescribeStepOutput) string {
	timeFormat := "2006-01-02T15:04:05Z"
	return " - StartTime: " + (*dso.Step.Status.Timeline.StartDateTime).Format(timeFormat) +
		" - EndTime: " + (*dso.Step.Status.Timeline.EndDateTime).Format(timeFormat)
}

// GetJobFlowStepsInput parses the config given to it and
// returns the parameters needed to add steps to an EMR
// cluster
func (jfs JobFlowSteps) GetJobFlowStepsInput() (*emr.AddJobFlowStepsInput, error) {
	if len(jfs.Config.Steps) < 1 {
		return nil, errors.New("No steps found in config, nothing to add")
	}

	allowedActions := []string{"CANCEL_AND_WAIT", "CONTINUE"}

	steps := make([]*emr.StepConfig, len(jfs.Config.Steps))
	for i, step := range jfs.Config.Steps {
		arguments := make([]*string, len(step.Arguments))
		for j, argument := range step.Arguments {
			arguments[j] = aws.String(argument)
		}

		hadoopJarStep := emr.HadoopJarStepConfig{
			Jar:  aws.String(step.Jar),
			Args: arguments,
		}

		if !StringInSlice(step.ActionOnFailure, allowedActions) {
			return nil, errors.New("Only the following failure actions are allowed '" +
				strings.Join(allowedActions, ", ") + "' - to terminate use the 'down' command")
		}

		stepConfig := emr.StepConfig{
			Name:            aws.String(step.Name),
			ActionOnFailure: aws.String(step.ActionOnFailure),
			HadoopJarStep:   &hadoopJarStep,
		}

		steps[i] = &stepConfig
	}

	params := &emr.AddJobFlowStepsInput{
		JobFlowId: aws.String(jfs.JobflowID),
		Steps:     steps,
	}

	return params, nil
}
