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
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGetJobFlowStepsInput_Success(t *testing.T) {
	assert := assert.New(t)
	ar, _ := InitConfigResolver()

	record, _ := ar.ParsePlaybookRecord([]byte(PlaybookRecord_1), nil)
	jfs := InitJobFlowSteps(*record, "jobflow-id", true)

	assert.NotNil(jfs)

	res, err := jfs.GetJobFlowStepsInput()
	assert.NotNil(res)
	assert.Nil(err)
	assert.Equal("jobflow-id", *res.JobFlowId)
	assert.Equal(2, len(res.Steps))
}

func TestGetJobFlowStepsInput_Fail(t *testing.T) {
	assert := assert.New(t)
	ar, _ := InitConfigResolver()

	record, _ := ar.ParsePlaybookRecord([]byte(PlaybookRecord_1), nil)
	jfs := InitJobFlowSteps(*record, "jobflow-id", true)

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

func TestAddJobFlowSteps_Fail(t *testing.T) {
	assert := assert.New(t)
	ar, _ := InitConfigResolver()

	record, _ := ar.ParsePlaybookRecord([]byte(PlaybookRecord_1), nil)
	jfs := InitJobFlowSteps(*record, "jobflow-id", true)

	assert.NotNil(jfs)

	err := jfs.AddJobFlowSteps()

	assert.NotNil(err)
	assert.Equal("EnvAccessKeyNotFound: AWS_ACCESS_KEY_ID or AWS_ACCESS_KEY not found in environment", err.Error())

	jfs.Config.Credentials.SecretAccessKey = "hello"

	err = jfs.AddJobFlowSteps()

	assert.NotNil(err)
	assert.Equal("access-key and secret-key must both be set to 'env', or neither", err.Error())

	jfs.Config.Steps = []*StepsRecord{}

	err = jfs.AddJobFlowSteps()

	assert.NotNil(err)
	assert.Equal("No steps found in config, nothing to add", err.Error())
}
