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
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/emr"
	"github.com/stretchr/testify/assert"
)

var CR, _ = InitConfigResolver()

func TestGetJobFlowInput_Success(t *testing.T) {
	assert := assert.New(t)

	record, _ := CR.ParseClusterRecord([]byte(ClusterRecord2), nil)

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

	assert.Equal("strconv.Atoi: parsing \"h\": invalid syntax", err.Error())
}

func TestGetJobFlowInput_Fail(t *testing.T) {
	assert := assert.New(t)

	record, _ := CR.ParseClusterRecord([]byte(ClusterRecord1), nil)

	// fails if GetLocation fails
	ec := InitEmrCluster(*record)
	res, err := ec.GetJobFlowInput()
	assert.Nil(res)
	assert.NotNil(err)
	assert.Equal("Only one of Availability Zone and Subnet id should be provided", err.Error())

	record.Ec2.Location.Vpc = nil
	record.Ec2.Location.Classic = nil
	ec = InitEmrCluster(*record)
	res, err = ec.GetJobFlowInput()
	assert.Nil(res)
	assert.NotNil(err)
	assert.Equal("At least one of Availability Zone and Subnet id is required", err.Error())

	// fails if GetApplications fails
	record, _ = CR.ParseClusterRecord([]byte(ClusterRecord2), nil)
	record.Applications = []string{"Snowplow"}
	ec, _ = InitEmrCluster(*record)

}

func TestTerminateJobFlows_Fail(t *testing.T) {
	assert := assert.New(t)

	record, _ := CR.ParseClusterRecord([]byte(ClusterRecord1), nil)
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

	record, _ := CR.ParseClusterRecord([]byte(ClusterRecord1), nil)
	ec := InitEmrCluster(*record)

	assert.NotNil(ec)

	jID, err := ec.RunJobFlow()

	assert.Equal("", jID)
	assert.NotNil(err)
	assert.Equal("Only one of Availability Zone and Subnet id should be provided", err.Error())

	ec.Config.Ec2.Location.Classic = nil

	jID, err = ec.RunJobFlow()

	assert.Equal("", jID)
	assert.NotNil(err)
	assert.Equal("EnvAccessKeyNotFound: AWS_ACCESS_KEY_ID or AWS_ACCESS_KEY not found in environment", err.Error())

	ec.Config.Credentials.SecretAccessKey = "hello"

	jID, err = ec.RunJobFlow()

	assert.Equal("", jID)
	assert.NotNil(err)
	assert.Equal("access-key and secret-key must both be set to 'env', or neither", err.Error())
}

func TestGetTags_NoTags(t *testing.T) {
	record, _ := CR.ParseClusterRecord([]byte(ClusterRecord1), nil)
	ec := InitEmrCluster(*record)
	assert.Nil(t, ec.GetTags())
}

func TestGetTags_WithTags(t *testing.T) {
	record, _ := CR.ParseClusterRecord([]byte(ClusterRecordWithTags), nil)
	ec := InitEmrCluster(*record)
	tags := ec.GetTags()
	assert.Len(t, tags, 1)
	expected := &emr.Tag{
		Key:   aws.String("hello"),
		Value: aws.String("world"),
	}
	assert.Equal(t, expected, tags[0])
}

func TestGetBootstrapActions_NoActions(t *testing.T) {
	record, _ := CR.ParseClusterRecord([]byte(ClusterRecord1), nil)
	ec := InitEmrCluster(*record)
	assert.Nil(t, ec.GetBootstrapActions())
}

func TestGetBootstrapActions_WithActions(t *testing.T) {
	record, _ := CR.ParseClusterRecord([]byte(ClusterRecordWithActions), nil)
	ec := InitEmrCluster(*record)
	actions := ec.GetBootstrapActions()
	assert.Len(t, actions, 1)
	expected := &emr.BootstrapActionConfig{
		Name: aws.String("Bootstrap Action"),
		ScriptBootstrapAction: &emr.ScriptBootstrapActionConfig{
			Path: aws.String("s3://snowplow/script.sh"),
			Args: []*string{aws.String("1.5")},
		},
	}
	assert.Equal(t, expected, actions[0])
}

func TestGetConfigurations_NoConfigs(t *testing.T) {
	record, _ := CR.ParseClusterRecord([]byte(ClusterRecord1), nil)
	ec := InitEmrCluster(*record)
	assert.Nil(t, ec.GetConfigurations())
}

func TestGetConfigurations_WithConfigs(t *testing.T) {
	record, _ := CR.ParseClusterRecord([]byte(ClusterRecordWithConfigs), nil)
	ec := InitEmrCluster(*record)
	configs := ec.GetConfigurations()
	assert.Len(t, configs, 1)
	expected := &emr.Configuration{
		Classification: aws.String("c"),
		Properties:     map[string]*string{"key": aws.String("value")},
	}
	assert.Equal(t, expected, configs[0])
}

func TestGetApplications_NoApps(t *testing.T) {
	record, _ := CR.ParseClusterRecord([]byte(ClusterRecord1), nil)
	ec := InitEmrCluster(*record)
	apps, err := ec.GetApplications()
	assert.Nil(t, apps)
	assert.Nil(t, err)
}

func TestGetApplications_WithApps(t *testing.T) {
	assert := assert.New(t)
	record, _ := CR.ParseClusterRecord([]byte(ClusterRecordWithApps), nil)
	ec := InitEmrCluster(*record)
	apps, _ := ec.GetApplications()
	assert.Len(apps, 2)
	assert.Equal(aws.String("Hadoop"), apps[0].Name)
	assert.Equal(aws.String("Spark"), apps[1].Name)

	// fails is the app is not allowed
	record.Applications = []string{"Snowplow"}
	ec, _ = InitEmrCluster(*record)
	_, err := ec.GetApplications()
	assert.NotNil(err)
	assert.Equal("Only Hadoop, Hive, Mahout, Pig, Spark are allowed applications", err.Error())
}
