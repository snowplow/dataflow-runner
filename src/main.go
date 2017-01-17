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

//+build !test

package main

import (
	"errors"
	log "github.com/Sirupsen/logrus"
	"gopkg.in/urfave/cli.v1"
	"os"
	"strings"
	"time"
)

const (
	APP_NAME      = "dataflow-runner"
	APP_USAGE     = "Run templatable playbooks of Hadoop/Spark/et al jobs on Amazon EMR"
	APP_COPYRIGHT = "(c) 2016-2017 Snowplow Analytics, LTD"
	CLI_VERSION   = "0.1.0"
	VAR_DELIM     = ","
)

func main() {
	app := cli.NewApp()

	SetLogLevel()

	app.Name = APP_NAME
	app.Usage = APP_USAGE
	app.Version = CLI_VERSION
	app.Copyright = APP_COPYRIGHT
	app.Compiled = time.Now()
	app.Authors = []cli.Author{
		cli.Author{
			Name:  "Joshua Beemster",
			Email: "support@snowplowanalytics.com",
		},
	}
	app.Commands = []cli.Command{
		{
			Name:  "up",
			Usage: "Launches a new EMR cluster",
			Flags: []cli.Flag{
				cli.StringFlag{Name: "emr-config", Usage: "EMR config path"},
				cli.StringFlag{Name: "vars", Usage: "Variables that will be used by the templater"},
			},
			Action: func(c *cli.Context) error {
				jobflowId, err := up(
					c.String("emr-config"),
					c.String("vars"),
				)
				checkErr(err)

				log.Info("EMR cluster launched successfully; Jobflow ID: " + jobflowId)
				return nil
			},
		},
		{
			Name:  "run",
			Usage: "Adds jobflow steps to a running EMR cluster",
			Flags: []cli.Flag{
				cli.StringFlag{Name: "emr-playbook", Usage: "Playbook path"},
				cli.StringFlag{Name: "emr-cluster", Usage: "Jobflow ID"},
				cli.BoolFlag{Name: "async", Usage: "Asynchronous execution of the jobflow steps"},
				cli.StringFlag{Name: "vars", Usage: "Variables that will be used by the templater"},
			},
			Action: func(c *cli.Context) error {
				err := run(
					c.String("emr-playbook"),
					c.String("emr-cluster"),
					c.Bool("async"),
					c.String("vars"),
				)
				checkErr(err)

				log.Info("All steps completed successfully")
				return nil
			},
		},
		{
			Name:  "down",
			Usage: "Terminates a running EMR cluster",
			Flags: []cli.Flag{
				cli.StringFlag{Name: "emr-config", Usage: "EMR config path"},
				cli.StringFlag{Name: "emr-cluster", Usage: "Jobflow ID"},
				cli.StringFlag{Name: "vars", Usage: "Variables that will be used by the templater"},
			},
			Action: func(c *cli.Context) error {
				err := down(
					c.String("emr-config"),
					c.String("emr-cluster"),
					c.String("vars"),
				)
				checkErr(err)

				log.Info("EMR cluster terminated successfully")
				return nil
			},
		},
		{
			Name:  "run-transient",
			Usage: "Launches, runs and then terminates an EMR cluster",
			Flags: []cli.Flag{
				cli.StringFlag{Name: "emr-config", Usage: "EMR config path"},
				cli.StringFlag{Name: "emr-playbook", Usage: "Playbook path"},
				cli.StringFlag{Name: "vars", Usage: "Variables that will be used by the templater"},
			},
			Action: func(c *cli.Context) error {
				emrConfig := c.String("emr-config")
				emrPlaybook := c.String("emr-playbook")
				vars := c.String("vars")

				jobflowId, err1 := up(emrConfig, vars)
				checkErr(err1)
				log.Info("EMR cluster launched successfully; Jobflow ID: " + jobflowId)

				err2 := run(emrPlaybook, jobflowId, false, vars)
				if err2 != nil {
					log.Error(err2.Error())
				} else {
					log.Info("All steps completed successfully")
				}

				err3 := down(emrConfig, jobflowId, vars)
				checkErr(err3)
				log.Info("EMR cluster terminated successfully")

				if err2 != nil {
					log.Error("Transient EMR run completed with errors")
					log.Fatal(err2.Error())
				} else {
					log.Info("Transient EMR run completed successfully")
				}

				return nil
			},
		},
	}

	app.Run(os.Args)
}

// --- Commands

// up launches a new EMR cluster
func up(emrConfig string, vars string) (string, error) {
	if emrConfig == "" {
		return "", errors.New("--emr-config needs to be specified")
	}

	varMap, err := varsToMap(vars)
	if err != nil {
		return "", err
	}

	ar := getNewConfigResolver()

	clusterRecord, err := ar.ParseClusterRecordFromFile(emrConfig, varMap)
	if err != nil {
		return "", err
	}

	ec := InitEmrCluster(*clusterRecord)
	jobflowId, err := ec.RunJobFlow()
	if err != nil {
		return "", err
	}

	return jobflowId, nil
}

// run adds steps to an EMR cluster
func run(emrPlaybook string, emrCluster string, async bool, vars string) error {
	if emrPlaybook == "" {
		return errors.New("--emr-playbook needs to be specified")
	}
	if emrCluster == "" {
		return errors.New("--emr-cluster needs to be specified")
	}

	varMap, err := varsToMap(vars)
	if err != nil {
		return err
	}

	ar := getNewConfigResolver()

	playbookRecord, err := ar.ParsePlaybookRecordFromFile(emrPlaybook, varMap)
	if err != nil {
		return err
	}

	jfs := InitJobFlowSteps(*playbookRecord, emrCluster, async)
	return jfs.AddJobFlowSteps()
}

// down terminates a running EMR cluster
func down(emrConfig string, emrCluster string, vars string) error {
	if emrConfig == "" {
		return errors.New("--emr-config needs to be specified")
	}
	if emrCluster == "" {
		return errors.New("--emr-cluster needs to be specified")
	}

	varMap, err := varsToMap(vars)
	if err != nil {
		return err
	}

	ar := getNewConfigResolver()

	clusterRecord, err := ar.ParseClusterRecordFromFile(emrConfig, varMap)
	if err != nil {
		return err
	}

	ec := InitEmrCluster(*clusterRecord)
	return ec.TerminateJobFlows(emrCluster)
}

// --- Helpers

// varsToMap converts the variables argument to a map of
// keys and values
func varsToMap(vars string) (map[string]interface{}, error) {
	if vars == "" {
		return map[string]interface{}{}, nil
	}

	varsArr := strings.Split(vars, VAR_DELIM)
	if len(varsArr)%2 != 0 {
		return nil, errors.New("--vars must have an even number of keys and values")
	}

	varsMap := make(map[string]interface{})
	for i := 0; i < len(varsArr); i += 2 {
		varsMap[varsArr[i]] = varsArr[i+1]
	}

	return varsMap, nil
}

// getNewConfigResolver gets a new ConfigResolver instance
func getNewConfigResolver() *ConfigResolver {
	ar, err := InitConfigResolver()
	checkErr(err)
	return ar
}

// checkErr is a utility function to exit the application on
// any errors being detected
func checkErr(err error) {
	if err != nil {
		log.Fatal(err.Error())
	}
}
