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
	"strconv"
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/emr"
	"github.com/aws/aws-sdk-go/service/emr/emriface"
	"github.com/hashicorp/errwrap"
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

	emrSvc := emr.New(session.New(), &aws.Config{
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

	addJobFlowStepsOutput, err := jfs.EmrSvc.AddJobFlowSteps(params)
	if err != nil {
		return nil, err
	}

	log.Info("Successfully added " + strconv.Itoa(len(jfs.Config.Steps)) +
		" steps to the EMR cluster with jobflow id '" + jfs.JobflowID + "'...")

	for done == false && jfs.IsBlocking == true {
		successCount, errCount, fStepsIDs, infoLogs, errorLogs, err :=
			jfs.RetrieveStepsStates(addJobFlowStepsOutput)
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
	return failedStepsIDs, errors.New("" + strconv.Itoa(errorCount) + "/" +
		strconv.Itoa(len(addJobFlowStepsOutput.StepIds)) + " steps failed to complete successfully")
}

// RetrieveStepsStates retrieves the states of all the steps for a job flow returning the state
// of every step as well as information about success or failure for each one
func (jfs JobFlowSteps) RetrieveStepsStates(addJobFlowStepsOutput *emr.AddJobFlowStepsOutput) (int, int, []string, []string, []string, error) {
	infoLogs := make([]string, 0)
	errorLogs := make([]string, 0)
	failedStepsIDs := make([]string, 0)
	successCount := 0
	errorCount := 0
	for _, stepID := range addJobFlowStepsOutput.StepIds {
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
	dso, err := jfs.EmrSvc.DescribeStep(describeStepInput)
	if err != nil {
		return "", nil, errwrap.Wrapf("Couldn't retrieve step "+stepID+" state: {{err}}", err)
	}

	if *dso.Step.Status.State == "COMPLETED" {
		infoLogs := []string{
			"Step '" + *dso.Step.Name + "' with id '" + *dso.Step.Id + "' completed successfully"}
		return *dso.Step.Status.State, infoLogs, nil
	} else if *dso.Step.Status.State == "CANCELLED" || *dso.Step.Status.State == "FAILED" {
		errorLogs := make([]string, 0)
		errorLogs = append(errorLogs,
			"Step '"+*dso.Step.Name+"' with id '"+*dso.Step.Id+"' was "+*dso.Step.Status.State)
		return *dso.Step.Status.State, errorLogs, nil
	}
	return *dso.Step.Status.State, nil, nil
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
