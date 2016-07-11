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
package main

import (
	"fmt"
	"github.com/spf13/cobra"
	"github.com/snowplow/dataflow-runner/cluster"
	"github.com/snowplow/dataflow-runner/steps"
)

const (
	CLI_VERSION = "0.1.0"
)

func main() {

	var region string
	var jobflowId string
	var time bool

	var cmdUp = &cobra.Command{
		Use:   "up",
		Short: "create a cluster",
		Long: `create a cluster using the details from the file
            `,
		Run: func(cmd *cobra.Command, args []string) {
			var clusterParams cluster.ClusterRecord = cluster.ParseClusterAvro(args[0])
			clusterParams.RunJobFlow(region)
		},
	}

	var cmdVersion = &cobra.Command{
		Use:   "version",
		Short: "version of dataflow runner",
		Long: `version of dataflow runner
            `,
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println("dataflow-runner: " + CLI_VERSION)
		},
	}

	var cmdRun = &cobra.Command{
		Use:   "run",
		Short: "run job steps",
		Long: `run jobsteps mentioned on the file on emr .
            `,
		Run: func(cmd *cobra.Command, args []string) {
			var jobs steps.PlaybookRecord = steps.ParseAvro(args[0])
			jobs.RunJobs(jobflowId, region, time)

		},
	}

	cmdUp.Flags().StringVarP(&region, "region", "r", "", "region of cluster")
	cmdRun.Flags().StringVarP(&region, "region", "r", "", "region of cluster")
	cmdRun.Flags().StringVarP(&jobflowId, "emr-cluster", "e", "", "cluster id")
	cmdRun.Flags().BoolVarP(&time, "time", "t", false, "blocking call")
	var rootCmd = &cobra.Command{Use: "app"}
	rootCmd.AddCommand(cmdUp, cmdRun, cmdVersion)

	rootCmd.Execute()

}
