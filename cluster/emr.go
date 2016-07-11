//
// Copyright (c) 2016 Snowplow Analytics Ltd. All rights reserved.
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
package cluster

import (
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/emr"
	"io/ioutil"
	"math/rand"
	"regexp"
	"time"
)

func ParseClusterAvro(filename string) ClusterRecord {

	clusterDetails, err := ioutil.ReadFile(filename)

	if err != nil {
		panic(err)
	}
	var clusterParams ClusterRecord
	err = json.Unmarshal(clusterDetails, &clusterParams)
	return clusterParams

}

func (clusterParams ClusterRecord) RunJobFlow(region string) {

	svc := emr.New(session.New(), &aws.Config{Region: aws.String(region)}) //getting emr instance specifying the region

	var instanceGroups = []*emr.InstanceGroupConfig{ //setting all the config for Instance Groups
		{
			InstanceCount: aws.Int64(1),
			InstanceRole:  aws.String("MASTER"),
			InstanceType:  aws.String(clusterParams.Data.Ec2.Instances.Master.Type),
		},
		{
			InstanceCount: aws.Int64(clusterParams.Data.Ec2.Instances.Core.Count),
			InstanceRole:  aws.String("CORE"),
			InstanceType:  aws.String(clusterParams.Data.Ec2.Instances.Core.Type),
		},
		{
			InstanceCount: aws.Int64(clusterParams.Data.Ec2.Instances.Task.Count),
			InstanceRole:  aws.String("TASK"),
			InstanceType:  aws.String(clusterParams.Data.Ec2.Instances.Task.Type),
		},
	}

	if clusterParams.Data.Ec2.Instances.Task.Bid != "" { //if task instance bid is provided setting the BidPrice  for the task instance

		instanceGroups[2].BidPrice = aws.String(clusterParams.Data.Ec2.Instances.Task.Bid)
		instanceGroups[2].Market = aws.String("SPOT") //SPOT instance since a bid price parameter is mentioned
	}

	if clusterParams.Data.Ec2.Instances.Task.Count > 0 && clusterParams.Data.Ec2.Instances.Core.Count <= 0 { //removing core config when there are no such instances

		instanceGroups = append(instanceGroups[0:1], instanceGroups[2])

	} else if clusterParams.Data.Ec2.Instances.Core.Count > 0 && clusterParams.Data.Ec2.Instances.Task.Count <= 0 { //removing task config when there are no such instances

		instanceGroups = instanceGroups[0:2]

	} else if clusterParams.Data.Ec2.Instances.Core.Count <= 0 && clusterParams.Data.Ec2.Instances.Task.Count <= 0 { //removing task and core configs when there are no such instances mentioned

		instanceGroups = instanceGroups[0:1]

	} else {

	}
	Placement := ""
	Ec2Subnet := ""
	if clusterParams.Data.Ec2.Location.Vpc != nil && clusterParams.Data.Ec2.Location.Classic != nil {
		fmt.Println("Only one of Availability Zone and Subnet id should be provided")
		return
	} else if clusterParams.Data.Ec2.Location.Vpc != nil {
		Ec2Subnet = clusterParams.Data.Ec2.Location.Vpc.SubnetId
	} else if clusterParams.Data.Ec2.Location.Classic != nil {
		Placement = clusterParams.Data.Ec2.Location.Classic.AvailabilityZone
	} else {
		fmt.Println("Atleast one of Availability Zone and Subnet id is needed")
		return
	}

	instances := &emr.JobFlowInstancesConfig{ //JobFlowInstancesConfig set
		Ec2KeyName:     aws.String(clusterParams.Data.Ec2.KeyName),
		Ec2SubnetId:    aws.String(Ec2Subnet),
		InstanceGroups: instanceGroups,
		Placement: &emr.PlacementType{
			AvailabilityZone: aws.String(Placement),
		},
	}

	params := &emr.RunJobFlowInput{ //RunJobFlowInput configs set
		Instances:   instances,
		Name:        aws.String(clusterParams.Data.Name),
		JobFlowRole: aws.String(clusterParams.Data.Roles.Jobflow),
		ServiceRole: aws.String(clusterParams.Data.Roles.Service),
	}

	match, _ := regexp.MatchString("[1-3]*", clusterParams.Data.Ec2.AmiVersion) //check to see if version < 4.x

	if match {
		params.AmiVersion = aws.String(clusterParams.Data.Ec2.AmiVersion) //if version < 4.x setting AmiVersion config
	} else {
		params.ReleaseLabel = aws.String("emr-" + clusterParams.Data.Ec2.AmiVersion) //else setting ReleaseLabel config
	}
	var done bool = false
	var retry int = 3
	var jobFlowId string
	for done == false && retry > 0 {
		resp, err := svc.RunJobFlow(params) //run Jobflow on emr
		if err != nil {
			fmt.Println(err.Error())
			return
		}

		cluster := &emr.DescribeClusterInput{ClusterId: aws.String(*resp.JobFlowId)}

		resp1, err := svc.DescribeCluster(cluster)

		if err != nil {
			fmt.Println(err.Error())
			return
		}

		for *resp1.Cluster.Status.State != "RUNNING" && *resp1.Cluster.Status.State != "TERMINATED_WITH_ERRORS" && *resp1.Cluster.Status.State != "TERMINATED" && *resp1.Cluster.Status.State != "TERMINATING" {
			time.Sleep(time.Second * 100)
			resp1, err = svc.DescribeCluster(cluster)

			if err != nil {
				fmt.Println(err.Error())
				return
			}
		}
		if *resp1.Cluster.Status.StateChangeReason.Code == "BOOTSTRAP_FAILURE" {
			retry -= 1
			var timeout = time.Duration(rand.Intn(600))
			time.Sleep(time.Second * timeout)
			fmt.Println("Bootstrap failure detected, retrying in" + string(timeout) + " seconds...")
		} else {
			done = true
		}
		jobFlowId = *resp.JobFlowId
	}
	if retry <= 0 {
		fmt.Println("Could not start the cluster due to bootstrap failure")
	} else {
		fmt.Println("JobFlow ID: " + jobFlowId) //JobFlowId printed out
	}
}
