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
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// --- Tests

func TestInitAvroResolver(t *testing.T) {
	assert := assert.New(t)

	ar, err := InitConfigResolver()
	assert.NotNil(ar)
	assert.Nil(err)
	assert.NotNil(ar.ClusterSchema)
	assert.NotNil(ar.PlaybookSchema)
}

func TestParseClusterRecord_Success(t *testing.T) {
	assert := assert.New(t)

	ar, _ := InitConfigResolver()
	res, err := ar.ParseClusterRecord([]byte(ClusterRecord1), nil)

	assert.NotNil(res)
	assert.Nil(err)
	assert.Equal("xxx", res.Name)
	assert.Equal("EMR_EC2_DefaultRole", res.Roles.Jobflow)
	assert.Equal("EMR_DefaultRole", res.Roles.Service)
	assert.Equal("4.5.0", res.Ec2.AmiVersion)
	assert.Equal("snowplow-yyy-key", res.Ec2.KeyName)
	assert.Equal("us-east-1a", res.Ec2.Location.Classic.AvailabilityZone)
	assert.Equal("subnet-123456", res.Ec2.Location.Vpc.SubnetId)
	assert.Equal("m1.medium", res.Ec2.Instances.Master.Type)
	assert.Equal("c3.4xlarge", res.Ec2.Instances.Core.Type)
	assert.Equal(int64(3), res.Ec2.Instances.Core.Count)
	assert.Equal("m1.medium", res.Ec2.Instances.Task.Type)
	assert.Equal(int64(0), res.Ec2.Instances.Task.Count)
	assert.Equal("0.015", res.Ec2.Instances.Task.Bid)

	res, err = ar.ParseClusterRecord([]byte(ClusterRecord2), nil)

	assert.NotNil(res)
	assert.Nil(err)
	assert.Equal("xxx", res.Name)
	assert.Equal("EMR_EC2_DefaultRole", res.Roles.Jobflow)
	assert.Equal("EMR_DefaultRole", res.Roles.Service)
	assert.Equal("4.5.0", res.Ec2.AmiVersion)
	assert.Equal("snowplow-yyy-key", res.Ec2.KeyName)
	assert.Nil(res.Ec2.Location.Classic)
	assert.Equal("subnet-123456", res.Ec2.Location.Vpc.SubnetId)
	assert.Equal("m1.medium", res.Ec2.Instances.Master.Type)
	assert.Equal("c3.4xlarge", res.Ec2.Instances.Core.Type)
	assert.Equal(int64(3), res.Ec2.Instances.Core.Count)
	assert.Equal("m1.medium", res.Ec2.Instances.Task.Type)
	assert.Equal(int64(0), res.Ec2.Instances.Task.Count)
	assert.Equal("0.015", res.Ec2.Instances.Task.Bid)
}

func TestParseClusterRecord_Fail(t *testing.T) {
	assert := assert.New(t)

	ar, _ := InitConfigResolver()
	res, err := ar.ParseClusterRecord([]byte("{"), nil)
	assert.Nil(res)
	assert.NotNil(err)
	assert.Equal("unexpected end of JSON input", err.Error())

	res, err = ar.ParseClusterRecord([]byte("{}"), nil)
	assert.NotNil(res)
	assert.Nil(err)

	res, err = ar.ParseClusterRecord([]byte(`{"schema":{},"data":"iglu:com.snowplowanalytics.dataflow-runner/Cluster/avro/1-0-0"}`), nil)
	assert.Nil(res)
	assert.NotNil(err)
	assert.Equal("json: cannot unmarshal object into Go struct field SelfDescribingRecord.Schema of type string", err.Error())

	res, err = ar.ParseClusterRecordFromFile("cluster_record.json", nil)
	assert.Nil(res)
	assert.NotNil(err)
	assert.Equal("open cluster_record.json: no such file or directory", err.Error())
}

func TestParsePlaybookRecord_Success(t *testing.T) {
	assert := assert.New(t)

	ar, _ := InitConfigResolver()
	res, err := ar.ParsePlaybookRecord([]byte(PlaybookRecord1), nil)

	assert.NotNil(res)
	assert.Nil(err)
	assert.Equal(2, len(res.Steps))

	for index, step := range res.Steps {
		if index == 0 {
			assert.Equal("CUSTOM_JAR", step.Type)
			assert.Equal("Combine Months", step.Name)
			assert.Equal("CANCEL_AND_WAIT", step.ActionOnFailure)
			assert.Equal("/usr/share/aws/emr/s3-dist-cp/lib/s3-dist-cp.jar", step.Jar)
			assert.Equal(4, len(step.Arguments))
		} else if index == 1 {
			assert.Equal("CUSTOM_JAR", step.Type)
			assert.Equal("Combine Months", step.Name)
			assert.Equal("CONTINUE", step.ActionOnFailure)
			assert.Equal("s3://snowplow-hosted-assets/3-enrich/hadoop-event-recovery/snowplow-hadoop-event-recovery-0.2.0.jar", step.Jar)
			assert.Equal(6, len(step.Arguments))
		}
	}
}

func TestParsePlaybookRecord_Fail(t *testing.T) {
	assert := assert.New(t)

	ar, _ := InitConfigResolver()
	res, err := ar.ParsePlaybookRecord([]byte("{"), nil)
	assert.Nil(res)
	assert.NotNil(err)
	assert.Equal("unexpected end of JSON input", err.Error())

	res, err = ar.ParsePlaybookRecord([]byte("{}"), nil)
	assert.NotNil(res)
	assert.Nil(err)

	res, err = ar.ParsePlaybookRecord([]byte(`{"schema":{},"data":"iglu:com.snowplowanalytics.dataflow-runner/Cluster/avro/1-0-0"}`), nil)
	assert.Nil(res)
	assert.NotNil(err)
	assert.Equal("json: cannot unmarshal object into Go struct field SelfDescribingRecord.Schema of type string", err.Error())

	res, err = ar.ParsePlaybookRecordFromFile("playbook_record.json", nil)
	assert.Nil(res)
	assert.NotNil(err)
	assert.Equal("open playbook_record.json: no such file or directory", err.Error())
}

func TestToSelfDescribingRecord(t *testing.T) {
	assert := assert.New(t)

	byteArr := []byte(`{"schema":"iglu:com.snowplowanalytics.dataflow-runner/Cluster/avro/1-0-0","data":{"key":"{{systemEnvs "TEST_ENV_VAR"}}","key2":"{{ .someVar}}","key3":"{{nowWithFormat "2006"}}"}}`)
	sdr, err := toSelfDescribingRecord(byteArr, nil)

	assert.Nil(sdr)
	assert.NotNil(err)
	assert.Equal("template: playbook:1: function \"systemEnvs\" not defined", err.Error())
}

func TestTemplateRawBytes(t *testing.T) {
	assert := assert.New(t)

	err := os.Setenv("TEST_ENV_VAR", "golangTestEnvVar")
	assert.Nil(err)

	varMap := map[string]interface{}{
		"someVar": "golangTestVar",
	}

	byteArr := []byte(`{"key":"{{systemEnv "TEST_ENV_VAR"}}","key2":"{{ .someVar}}","key3":"{{nowWithFormat "2006"}}"}`)
	templatedByteArr, err := templateRawBytes(byteArr, varMap)

	currYear := strconv.Itoa(time.Now().Year())

	assert.NotNil(templatedByteArr)
	assert.Nil(err)
	assert.Equal(`{"key":"golangTestEnvVar","key2":"golangTestVar","key3":"`+currYear+`"}`, string(templatedByteArr))

	byteArr = []byte(`{"key":"{{systemEnvs "TEST_ENV_VAR"}}"}`)
	templatedByteArr, err = templateRawBytes(byteArr, varMap)

	assert.Nil(templatedByteArr)
	assert.NotNil(err)
	assert.Equal("template: playbook:1: function \"systemEnvs\" not defined", err.Error())
}
