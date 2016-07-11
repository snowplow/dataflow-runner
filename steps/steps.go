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
package steps

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/emr"
	"github.com/elodina/go-avro"
	"time"
)

func ParseAvro(filename string) PlaybookRecord {

	specificReader, err := avro.NewDataFileReader(filename, avro.NewSpecificDatumReader()) // get the avro specific reader
	if err != nil {
		panic(err)
	}
	obj := new(PlaybookRecord) //empty schema to populate
	ok, err := specificReader.Next(obj)
	if !ok {
		if err != nil {
			panic(err)
		}
	}
	return *obj

}

func (jobs PlaybookRecord) RunJobs(jobFlowId string, region string, time1 bool) {

	svc := emr.New(session.New(), &aws.Config{Region: aws.String(region)}) //getting emr instance specifying the region

	if len(jobs.Data.Steps) < 1 { // no job steps so quiting
		fmt.Println("no job details in file")
		return
	}
	arguments := []*string{}
	if len(jobs.Data.Steps[0].Arguments) < 1 { //  empty array since no arguments

	} else {
		arguments = []*string{ //setting first argument then appending the rest
			aws.String(jobs.Data.Steps[0].Arguments[0]),
		}

		for j := 1; j < len(jobs.Data.Steps[0].Arguments); j++ {
			next := []*string{
				aws.String(jobs.Data.Steps[0].Arguments[j]),
			}
			arguments = append(arguments, next[0])
		}
	}
	steps := []*emr.StepConfig{ //setting for first job
		{
			HadoopJarStep: &emr.HadoopJarStepConfig{
				Jar:  aws.String(jobs.Data.Steps[0].Jar),
				Args: arguments,
			},
			Name:            aws.String(jobs.Data.Steps[0].Name),
			ActionOnFailure: aws.String(jobs.Data.Steps[0].ActionOnFailure),
		},
	}

	for i := 1; i < len(jobs.Data.Steps); i++ {
		arguments := []*string{} //setting arguments for rest of the jobs
		if len(jobs.Data.Steps[0].Arguments) < 1 {

		} else {
			arguments = []*string{
				aws.String(jobs.Data.Steps[i].Arguments[0]),
			}
			for j := 1; j < len(jobs.Data.Steps[i].Arguments); j++ {
				next := []*string{
					aws.String(jobs.Data.Steps[i].Arguments[j]),
				}
				arguments = append(arguments, next[0])
			}
		}
		test := []*emr.StepConfig{ //setting configs for rest of the jobs
			{
				HadoopJarStep: &emr.HadoopJarStepConfig{
					Jar:  aws.String(jobs.Data.Steps[i].Jar),
					Args: arguments,
				},
				Name:            aws.String(jobs.Data.Steps[i].Name),
				ActionOnFailure: aws.String(jobs.Data.Steps[i].ActionOnFailure),
			},
		}
		steps = append(steps, test[0])
	}

	params := &emr.AddJobFlowStepsInput{
		JobFlowId: aws.String(jobFlowId), //setting clusterid or jobflowid
		Steps:     steps,                 // config for job details
	}

	resp, err := svc.AddJobFlowSteps(params) //aws call to add jobs

	if err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Println(resp)
	var done bool = false
	var count int = 0

	for done == false && time1 == true {

		for j := 0; j < len(resp.StepIds); j++ {
			params1 := &emr.DescribeStepInput{
				ClusterId: aws.String(jobFlowId),
				StepId:    resp.StepIds[j],
			}
			req, resp1 := svc.DescribeStepRequest(params1)
			err1 := req.Send()
			if err1 == nil {
				if *resp1.Step.Status.State == "COMPLETED" {
					count += 1
				} else if *resp1.Step.Status.State == "CANCELLED" || *resp1.Step.Status.State == "FAILED" {
					count += 1
					fmt.Println("job " + *resp1.Step.Id + " failed")
				} else {
				}

			}

		}
		if count == len(resp.StepIds) {
			done = true
		} else {
			time.Sleep(time.Second * 15)
			count = 0
		}

	}

}
