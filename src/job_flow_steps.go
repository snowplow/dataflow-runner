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
	log "github.com/Sirupsen/logrus"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/emr"
	"strconv"
	"strings"
	"time"
)

type JobFlowSteps struct {
	Config     PlaybookConfig
	JobflowId  string
	IsBlocking bool
}

// InitJobFlowSteps creates a new JobFlowSteps instance
func InitJobFlowSteps(playbookConfig PlaybookConfig, jobflowId string, isAsync bool) *JobFlowSteps {
	return &JobFlowSteps{
		Config:     playbookConfig,
		JobflowId:  jobflowId,
		IsBlocking: !isAsync,
	}
}

// AddJobFlowSteps builds the parameters and then submits them to
// the running EMR cluster
func (jfs JobFlowSteps) AddJobFlowSteps() error {
	params, err := jfs.GetJobFlowStepsInput()
	if err != nil {
		return err
	}

	creds, err := GetCredentialsProvider(jfs.Config.Credentials.AccessKeyId, jfs.Config.Credentials.SecretAccessKey)
	if err != nil {
		return err
	}

	done := false
	successCount := 0
	errorCount := 0

	svc := emr.New(session.New(), &aws.Config{Region: aws.String(jfs.Config.Region), Credentials: creds})

	resp, err := svc.AddJobFlowSteps(params)
	if err != nil {
		return err
	}

	log.Info("Successfully added " + strconv.Itoa(len(jfs.Config.Steps)) + " steps to the EMR cluster with jobflow id '" + jfs.JobflowId + "'...")

	for done == false && jfs.IsBlocking == true {

		for _, stepId := range resp.StepIds {
			params1 := &emr.DescribeStepInput{
				ClusterId: aws.String(jfs.JobflowId),
				StepId:    stepId,
			}

			req, resp1 := svc.DescribeStepRequest(params1)
			err1 := req.Send()

			if err1 == nil {
				if *resp1.Step.Status.State == "COMPLETED" {
					log.Info("Step '" + *resp1.Step.Name + "' with id '" + *resp1.Step.Id + "' completed successfully")
					successCount += 1
				} else if *resp1.Step.Status.State == "CANCELLED" || *resp1.Step.Status.State == "FAILED" {
					log.Error("Step '" + *resp1.Step.Name + "' with id '" + *resp1.Step.Id + "' was " + *resp1.Step.Status.State)
					errorCount += 1
				}
			}
		}

		if (successCount + errorCount) == len(resp.StepIds) {
			done = true
		} else {
			time.Sleep(time.Second * 15)
			successCount = 0
			errorCount = 0
		}
	}

	if errorCount == 0 {
		return nil
	} else {
		return errors.New("" + strconv.Itoa(errorCount) + "/" + strconv.Itoa(len(resp.StepIds)) + " steps failed to complete successfully")
	}
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
			return nil, errors.New("Only the following failure actions are allowed '" + strings.Join(allowedActions, ", ") + "' - to terminate use the 'down' command")
		}

		stepConfig := emr.StepConfig{
			Name:            aws.String(step.Name),
			ActionOnFailure: aws.String(step.ActionOnFailure),
			HadoopJarStep:   &hadoopJarStep,
		}

		steps[i] = &stepConfig
	}

	params := &emr.AddJobFlowStepsInput{
		JobFlowId: aws.String(jfs.JobflowId),
		Steps:     steps,
	}

	return params, nil
}
