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
	"os"
	"strings"
	"time"

	"fmt"

	log "github.com/Sirupsen/logrus"
	"gopkg.in/urfave/cli.v1"
)

type lockError string

func (l lockError) Error() string { return string(l) }

const (
	appName                = "dataflow-runner"
	appUsage               = "Run templatable playbooks of Hadoop/Spark/et al jobs on Amazon EMR"
	appCopyright           = "(c) 2016-2017 Snowplow Analytics Ltd"
	cliVersion             = "0.3.0"
	varDelim               = ","
	fEmrConfig             = "emr-config"
	fEmrPlaybook           = "emr-playbook"
	fEmrCluster            = "emr-cluster"
	fVars                  = "vars"
	fAsync                 = "async"
	fLogLevel              = "log-level"
	fLock                  = "lock"
	fSoftLock              = "softLock"
	fConsul                = "consul"
	lockFileExistsExitCode = 17
	otherExitCode          = 1
)

func main() {
	app := cli.NewApp()

	var logLevel string
	logLevels := map[string]log.Level{
		"debug":   log.DebugLevel,
		"info":    log.InfoLevel,
		"warning": log.WarnLevel,
		"error":   log.ErrorLevel,
		"fatal":   log.FatalLevel,
		"panic":   log.PanicLevel,
	}
	logLevelKeys := getLogLevelKeys(logLevels)

	app.Name = appName
	app.Usage = appUsage
	app.Version = cliVersion
	app.Copyright = appCopyright
	app.Compiled = time.Now()
	app.Authors = []cli.Author{
		{
			Name:  "Joshua Beemster",
			Email: "support@snowplowanalytics.com",
		},
	}
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  fLogLevel,
			Value: "info",
			Usage: fmt.Sprintf("logging level, possible values are %s",
				strings.Join(logLevelKeys, ",")),
			Destination: &logLevel,
		},
	}
	app.Action = func(c *cli.Context) error {
		if level, ok := logLevels[logLevel]; ok {
			log.SetLevel(level)
		} else {
			return fmt.Errorf("Supported log levels are %s, provided %s",
				strings.Join(logLevelKeys, ","), logLevel)
		}
		cli.ShowAppHelp(c)
		return nil
	}
	app.Commands = []cli.Command{
		{
			Name:  "up",
			Usage: "Launches a new EMR cluster",
			Flags: []cli.Flag{
				getEmrConfigFlag(),
				getVarsFlag(),
			},
			Action: func(c *cli.Context) error {
				jobflowID, err := up(
					c.String(fEmrConfig),
					c.String(fVars),
				)
				if err != nil {
					return exitCodeError(err)
				}

				log.Info("EMR cluster launched successfully; Jobflow ID: " + jobflowID)
				return nil
			},
		},
		{
			Name:  "run",
			Usage: "Adds jobflow steps to a running EMR cluster",
			Flags: []cli.Flag{
				getEmrPlaybookFlag(),
				getEmrClusterFlag(),
				getAsyncFlag(),
				getLockFlag(),
				getSoftLockFlag(),
				getConsulFlag(),
				getVarsFlag(),
			},
			Action: func(c *cli.Context) error {
				async := c.Bool(fAsync)
				hardLock := c.String(fLock)
				softLock := c.String(fSoftLock)
				consul := c.String(fConsul)

				err := checkLockFlags(async, hardLock, softLock, consul)
				if err != nil {
					return exitCodeError(err)
				}

				lock, err := initLock(hardLock, softLock, consul)
				if err != nil {
					return exitCodeError(lockError(err.Error()))
				}

				err = run(
					c.String(fEmrPlaybook),
					c.String(fEmrCluster),
					async,
					c.String(fVars),
				)
				if err != nil {
					if lock != nil && softLock != "" {
						lock.Unlock()
					}
					return exitCodeError(err)
				} else if lock != nil {
					lock.Unlock()
				}

				log.Info("All steps completed successfully")

				return nil
			},
		},
		{
			Name:  "down",
			Usage: "Terminates a running EMR cluster",
			Flags: []cli.Flag{
				getEmrConfigFlag(),
				getEmrClusterFlag(),
				getVarsFlag(),
			},
			Action: func(c *cli.Context) error {
				err := down(
					c.String(fEmrConfig),
					c.String(fEmrCluster),
					c.String(fVars),
				)
				if err != nil {
					return exitCodeError(err)
				}

				log.Info("EMR cluster terminated successfully")
				return nil
			},
		},
		{
			Name:  "run-transient",
			Usage: "Launches, runs and then terminates an EMR cluster",
			Flags: []cli.Flag{
				getEmrConfigFlag(),
				getEmrPlaybookFlag(),
				getLockFlag(),
				getSoftLockFlag(),
				getConsulFlag(),
				getVarsFlag(),
			},
			Action: func(c *cli.Context) error {
				emrConfig := c.String(fEmrConfig)
				emrPlaybook := c.String(fEmrPlaybook)
				hardLock := c.String(fLock)
				softLock := c.String(fSoftLock)
				consul := c.String(fConsul)
				vars := c.String(fVars)

				err := checkLockFlags(false, hardLock, softLock, consul)
				if err != nil {
					return exitCodeError(err)
				}

				lock, err := initLock(hardLock, softLock, consul)
				if err != nil {
					return exitCodeError(lockError(err.Error()))
				}

				jobflowID, err1 := up(emrConfig, vars)
				if err1 != nil {
					if lock != nil && softLock != "" {
						lock.Unlock()
					}
					return exitCodeError(err1)
				}
				log.Info("EMR cluster launched successfully; Jobflow ID: " + jobflowID)
				err2 := run(emrPlaybook, jobflowID, false, vars)

				err3 := down(emrConfig, jobflowID, vars)
				if err3 != nil {
					if lock != nil && softLock != "" {
						lock.Unlock()
					}
					return exitCodeError(err3)
				}
				log.Info("EMR cluster terminated successfully")

				if err2 != nil {
					if lock != nil && softLock != "" {
						lock.Unlock()
					}
					log.Error("Transient EMR run completed with errors")
					return exitCodeError(err2)
				}

				log.Info("Transient EMR run completed successfully")

				if lock != nil {
					lock.Unlock()
				}

				return nil
			},
		},
	}

	app.Run(os.Args)
}

// --- CLI Flags

func getEmrConfigFlag() cli.StringFlag {
	return cli.StringFlag{Name: fEmrConfig, Usage: "EMR config path"}
}

func getEmrPlaybookFlag() cli.StringFlag {
	return cli.StringFlag{Name: fEmrPlaybook, Usage: "Playbook path"}
}

func getEmrClusterFlag() cli.StringFlag {
	return cli.StringFlag{Name: fEmrCluster, Usage: "Jobflow ID"}
}

func getVarsFlag() cli.StringFlag {
	return cli.StringFlag{Name: fVars, Usage: "Variables that will be used by the templater"}
}

func getAsyncFlag() cli.BoolFlag {
	return cli.BoolFlag{Name: fAsync, Usage: "Asynchronous execution of the jobflow steps"}
}

func getLockFlag() cli.StringFlag {
	usage := "Path to the lock held for the duration of the jobflow steps. This is materialized" +
		" by a file or a KV entry in Consul depending on the --" + fConsul + "flag."
	return cli.StringFlag{
		Name:  fLock,
		Usage: usage,
	}
}

func getSoftLockFlag() cli.StringFlag {
	usage := "Path to the lock held for the duration of the jobflow steps. This is materialized" +
		" by a file or a KV entry in Consul depending on the --" + fConsul + "flag. Released no" +
		" matter if the operation failed or succeeded."
	return cli.StringFlag{
		Name:  fSoftLock,
		Usage: usage,
	}
}

func getConsulFlag() cli.StringFlag {
	return cli.StringFlag{
		Name:  fConsul,
		Usage: "Address of the Consul server used for distributed locking for the duration of the jobflow steps",
	}
}

// --- Commands

// up launches a new EMR cluster
func up(emrConfig string, vars string) (string, error) {
	if emrConfig == "" {
		return "", flagToError(fEmrConfig)
	}

	varMap, err := varsToMap(vars)
	if err != nil {
		return "", err
	}

	ar, err := InitConfigResolver()
	if err != nil {
		return "", err
	}

	clusterRecord, err := ar.ParseClusterRecordFromFile(emrConfig, varMap)
	if err != nil {
		return "", err
	}

	ec, err := InitEmrCluster(*clusterRecord)
	if err != nil {
		return "", err
	}
	jobflowID, err := ec.RunJobFlow()
	if err != nil {
		return "", err
	}

	return jobflowID, nil
}

// run adds steps to an EMR cluster
func run(emrPlaybook, emrCluster string, async bool, vars string) error {
	if emrPlaybook == "" {
		return flagToError(fEmrPlaybook)
	}
	if emrCluster == "" {
		return flagToError(fEmrCluster)
	}

	varMap, err := varsToMap(vars)
	if err != nil {
		return err
	}

	ar, err := InitConfigResolver()
	if err != nil {
		return err
	}

	playbookRecord, err := ar.ParsePlaybookRecordFromFile(emrPlaybook, varMap)
	if err != nil {
		return err
	}

	jfs, err := InitJobFlowSteps(*playbookRecord, emrCluster, async)
	if err != nil {
		return err
	}

	return jfs.AddJobFlowSteps()
}

// down terminates a running EMR cluster
func down(emrConfig string, emrCluster string, vars string) error {
	if emrConfig == "" {
		return flagToError(fEmrConfig)
	}
	if emrCluster == "" {
		return flagToError(fEmrCluster)
	}

	varMap, err := varsToMap(vars)
	if err != nil {
		return err
	}

	ar, err := InitConfigResolver()
	if err != nil {
		return err
	}

	clusterRecord, err := ar.ParseClusterRecordFromFile(emrConfig, varMap)
	if err != nil {
		return err
	}

	ec, err := InitEmrCluster(*clusterRecord)
	if err != nil {
		return err
	}
	return ec.TerminateJobFlow(emrCluster)
}

// --- Helpers

// flagToError returns a generic error for a missing flag
func flagToError(flag string) error {
	return errors.New("--" + flag + " needs to be specified")
}

// checkLockFlags checks the validity of the lock-related flags
func checkLockFlags(async bool, hardLock, softLock, consul string) error {
	if consul != "" && hardLock == "" && softLock == "" {
		return errors.New(
			"--" + fLock + " or --" + fSoftLock + " is needed to make use of --" + fConsul)
	}
	if hardLock != "" && softLock != "" {
		return errors.New("--" + fLock + " and --" + fSoftLock + " are mutually exclusive")
	}
	if async && (hardLock != "" || softLock != "") {
		return errors.New(
			"--" + fAsync + " and --" + fLock + " or --" + fSoftLock + " are not compatible")
	}
	return nil
}

// varsToMap converts the variables argument to a map of
// keys and values
func varsToMap(vars string) (map[string]interface{}, error) {
	if vars == "" {
		return map[string]interface{}{}, nil
	}

	varsArr := strings.Split(vars, varDelim)
	if len(varsArr)%2 != 0 {
		return nil, errors.New("--" + fVars + " must have an even number of keys and values")
	}

	varsMap := make(map[string]interface{})
	for i := 0; i < len(varsArr); i += 2 {
		varsMap[varsArr[i]] = varsArr[i+1]
	}

	return varsMap, nil
}

// exitCodeError turns an error into an exit code aware error
func exitCodeError(err error) error {
	switch err.(type) {
	case lockError:
		log.Warn(err.Error())
		return cli.NewExitError(err.Error(), lockFileExistsExitCode)
	default:
		log.Error(err.Error())
		return cli.NewExitError(err.Error(), otherExitCode)
	}
}

// getLogLevelKeys builds an array of the available log levels
func getLogLevelKeys(logLevels map[string]log.Level) []string {
	keys := make([]string, 0, len(logLevels))
	for k := range logLevels {
		keys = append(keys, k)
	}
	return keys
}

// initLock tries to init a lock
func initLock(hardLock, softLock, consul string) (Lock, error) {
	var lock Lock
	var err error
	if hardLock != "" || softLock != "" {
		lock, err = GetLock(hardLock+softLock, consul)
		if err != nil {
			return nil, err
		}
		err = lock.TryLock()
		if err != nil {
			return nil, err
		}
	}
	return lock, nil
}
