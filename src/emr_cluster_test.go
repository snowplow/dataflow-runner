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

func TestGetJobFlowInput_Success(t *testing.T) {
	assert := assert.New(t)
	ar, _ := InitConfigResolver()

	record, _ := ar.ParseClusterRecord([]byte(ClusterRecord_2), nil)

	// Master, Core and Task Instances
	record.Ec2.Instances.Core.Count = 1
	record.Ec2.Instances.Task.Count = 1

	ec := InitEmrCluster(*record)
	res, _ := ec.GetJobFlowInput()

	assert.Equal(3, len(res.Instances.InstanceGroups))

	// Master and Task Instances
	record.Ec2.Instances.Core.Count = 0
	record.Ec2.Instances.Task.Count = 1

	ec = InitEmrCluster(*record)
	res, _ = ec.GetJobFlowInput()

	assert.Equal(2, len(res.Instances.InstanceGroups))

	// Master and Core Instances
	record.Ec2.Instances.Core.Count = 1
	record.Ec2.Instances.Task.Count = 0

	ec = InitEmrCluster(*record)
	res, _ = ec.GetJobFlowInput()

	assert.Equal(2, len(res.Instances.InstanceGroups))

	// Master Instances Only
	record.Ec2.Instances.Core.Count = 0
	record.Ec2.Instances.Task.Count = 0

	ec = InitEmrCluster(*record)
	res, _ = ec.GetJobFlowInput()

	assert.Equal(1, len(res.Instances.InstanceGroups))

	// Classic location

	record.Ec2.Location.Vpc = nil
	record.Ec2.Location.Classic = &ClassicRecord{AvailabilityZone: "us-east-1a"}

	ec = InitEmrCluster(*record)
	res, _ = ec.GetJobFlowInput()

	assert.Equal("us-east-1a", *res.Instances.Placement.AvailabilityZone)
	assert.Equal("", *res.Instances.Ec2SubnetId)

	// EMR < 4

	record.Ec2.AmiVersion = "3.0.0"

	ec = InitEmrCluster(*record)
	res, _ = ec.GetJobFlowInput()

	assert.Equal("3.0.0", *res.AmiVersion)

	// EMR >= 4

	record.Ec2.AmiVersion = "4.5.0"

	ec = InitEmrCluster(*record)
	res, _ = ec.GetJobFlowInput()

	assert.Equal("emr-4.5.0", *res.ReleaseLabel)

	// EMR not a valid version string

	record.Ec2.AmiVersion = "hello"

	ec = InitEmrCluster(*record)
	_, err := ec.GetJobFlowInput()

	assert.Equal("strconv.ParseInt: parsing \"h\": invalid syntax", err.Error())
}

func TestGetJobFlowInput_Fail(t *testing.T) {
	assert := assert.New(t)
	ar, _ := InitConfigResolver()

	record, _ := ar.ParseClusterRecord([]byte(ClusterRecord_1), nil)
	ec := InitEmrCluster(*record)

	assert.NotNil(ec)

	res, err := ec.GetJobFlowInput()
	assert.Nil(res)
	assert.NotNil(err)
	assert.Equal("Only one of Availability Zone and Subnet id should be provided", err.Error())

	record.Ec2.Location.Vpc = nil
	record.Ec2.Location.Classic = nil

	ec = InitEmrCluster(*record)
	assert.NotNil(ec)

	res, err = ec.GetJobFlowInput()
	assert.Nil(res)
	assert.NotNil(err)
	assert.Equal("At least one of Availability Zone and Subnet id is required", err.Error())
}

func TestTerminateJobFlows_Fail(t *testing.T) {
	assert := assert.New(t)
	ar, _ := InitConfigResolver()

	record, _ := ar.ParseClusterRecord([]byte(ClusterRecord_1), nil)
	ec := InitEmrCluster(*record)

	assert.NotNil(ec)

	err := ec.TerminateJobFlows("hello")

	assert.NotNil(err)
	assert.Equal("EnvAccessKeyNotFound: AWS_ACCESS_KEY_ID or AWS_ACCESS_KEY not found in environment", err.Error())

	ec.Config.Credentials.SecretAccessKey = "hello"

	err = ec.TerminateJobFlows("hello")

	assert.NotNil(err)
	assert.Equal("access-key and secret-key must both be set to 'env', or neither", err.Error())
}

func TestRunJobFlow_Fail(t *testing.T) {
	assert := assert.New(t)
	ar, _ := InitConfigResolver()

	record, _ := ar.ParseClusterRecord([]byte(ClusterRecord_1), nil)
	ec := InitEmrCluster(*record)

	assert.NotNil(ec)

	jId, err := ec.RunJobFlow()

	assert.Equal("", jId)
	assert.NotNil(err)
	assert.Equal("Only one of Availability Zone and Subnet id should be provided", err.Error())

	ec.Config.Ec2.Location.Classic = nil

	jId, err = ec.RunJobFlow()

	assert.Equal("", jId)
	assert.NotNil(err)
	assert.Equal("EnvAccessKeyNotFound: AWS_ACCESS_KEY_ID or AWS_ACCESS_KEY not found in environment", err.Error())

	ec.Config.Credentials.SecretAccessKey = "hello"

	jId, err = ec.RunJobFlow()

	assert.Equal("", jId)
	assert.NotNil(err)
	assert.Equal("access-key and secret-key must both be set to 'env', or neither", err.Error())
}
