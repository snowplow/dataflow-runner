//
// Copyright (c) 2016-2022 Snowplow Analytics Ltd. All rights reserved.
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

//go:build !test
// +build !test

package main

import (
	"errors"
	"os"
	"strconv"
	"strings"
	"time"

	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/emr"
	log "github.com/sirupsen/logrus"
	"gopkg.in/urfave/cli.v1"

	"github.com/getsentry/sentry-go"
)

const (
	appName          = "dataflow-runner"
	appUsage         = "Run templatable playbooks of Hadoop/Spark/et al jobs on Amazon EMR"
	appCopyright     = "(c) 2016-2022 Snowplow Analytics Ltd"
	cliVersion       = "0.7.1"
	varDelim         = ","
	fEmrConfig       = "emr-config"
	fEmrPlaybook     = "emr-playbook"
	fEmrCluster      = "emr-cluster"
	fVars            = "vars"
	fAsync           = "async"
	fLogFailedSteps  = "log-failed-steps"
	fLogLevel        = "log-level"
	fLock            = "lock"
	fSoftLock        = "softLock"
	fConsul          = "consul"
	fSentry          = "sentry"
	lockHeldExitCode = 17
	otherExitCode    = 1
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
				getSentryFlag(),
			},
			Action: func(c *cli.Context) error {
				sentry := c.String(fSentry)
				sentryEnabled := len(sentry) > 0

				if sentryEnabled {
					err := initializeSentry(sentry)
					if err != nil {
						return cli.NewExitError(err, otherExitCode)
					}
				}

				jobflowID, err := up(
					c.String(fEmrConfig),
					c.String(fVars),
				)
				if err != nil {
					return exitCodeError(sentryEnabled, err)
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
				getLogFailedStepsFlag(),
				getAsyncFlag(),
				getLockFlag(),
				getSoftLockFlag(),
				getConsulFlag(),
				getVarsFlag(),
				getSentryFlag(),
			},
			Action: func(c *cli.Context) error {
				emrPlaybook := c.String(fEmrPlaybook)
				jobflowID := c.String(fEmrCluster)
				logFailedSteps := c.Bool(fLogFailedSteps)
				async := c.Bool(fAsync)
				hardLock := c.String(fLock)
				softLock := c.String(fSoftLock)
				consul := c.String(fConsul)
				vars := c.String(fVars)
				sentry := c.String(fSentry)
				sentryEnabled := len(sentry) > 0

				if sentryEnabled {
					err := initializeSentry(sentry)
					if err != nil {
						return cli.NewExitError(err, otherExitCode)
					}
				}

				err := checkLockFlags(async, hardLock, softLock, consul)
				if err != nil {
					return exitCodeError(sentryEnabled, err)
				}

				lock, err := initLock(hardLock, softLock, consul)
				if err != nil {
					return exitCodeError(sentryEnabled, err)
				}

				failedStepsIDs, err := run(emrPlaybook, jobflowID, async, vars)

				if logFailedSteps && len(failedStepsIDs) > 0 {
					// Here we can't leverage the time spent downing the cluster to make sure log files have
					// been rotated. As a result, we just sleep.
					sleep := 300
					log.Info("Sleeping for " + strconv.Itoa(sleep) +
						" seconds waiting for the logs to be rotated")
					time.Sleep(time.Second * time.Duration(sleep))
					displayFailedStepsLogs(failedStepsIDs, emrPlaybook, jobflowID, vars)
				}

				if err != nil {
					if lock != nil && softLock != "" {
						lock.Unlock()
					}
					return exitCodeError(sentryEnabled, err)
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
				getSentryFlag(),
			},
			Action: func(c *cli.Context) error {
				sentry := c.String(fSentry)
				sentryEnabled := len(sentry) > 0

				if sentryEnabled {
					err := initializeSentry(sentry)
					if err != nil {
						return cli.NewExitError(err, otherExitCode)
					}
				}

				err := down(
					c.String(fEmrConfig),
					c.String(fEmrCluster),
					c.String(fVars),
				)
				if err != nil {
					return exitCodeError(sentryEnabled, err)
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
				getLogFailedStepsFlag(),
				getLockFlag(),
				getSoftLockFlag(),
				getConsulFlag(),
				getVarsFlag(),
				getSentryFlag(),
			},
			Action: func(c *cli.Context) error {
				emrConfig := c.String(fEmrConfig)
				emrPlaybook := c.String(fEmrPlaybook)
				logFailedSteps := c.Bool(fLogFailedSteps)
				hardLock := c.String(fLock)
				softLock := c.String(fSoftLock)
				consul := c.String(fConsul)
				vars := c.String(fVars)
				sentry := c.String(fSentry)
				sentryEnabled := len(sentry) > 0

				if sentryEnabled {
					err := initializeSentry(sentry)
					if err != nil {
						return cli.NewExitError(err, otherExitCode)
					}
				}

				clusterRecord, err := parseClusterRecord(emrConfig, vars)
				if err != nil {
					return exitCodeError(sentryEnabled, err)
				}

				playbookRecord, err := parsePlaybookRecord(emrPlaybook, vars)
				if err != nil {
					return exitCodeError(sentryEnabled, err)
				}

				err = checkLockFlags(false, hardLock, softLock, consul)
				if err != nil {
					return exitCodeError(sentryEnabled, err)
				}

				lock, err := initLock(hardLock, softLock, consul)
				if err != nil {
					return exitCodeError(sentryEnabled, err)
				}

				emrCluster, err := InitEmrCluster(*clusterRecord)
				if err != nil {
					if lock != nil && softLock != "" {
						lock.Unlock()
					}
					return exitCodeError(sentryEnabled, err)
				}

				jobFlowSteps, err := runJobFlowWithSteps(emrCluster, playbookRecord)
				if err != nil {
					if lock != nil && softLock != "" {
						lock.Unlock()
					}
					return exitCodeError(sentryEnabled, err)
				}

				log.Info("Transient EMR run with jobflow ID [" + jobFlowSteps.JobflowID + "] started successfully")

				log.Info("Waiting until cluster is terminated...")
				err = emrCluster.Svc.WaitUntilClusterTerminatedWithContext(
					aws.BackgroundContext(),
					&emr.DescribeClusterInput{
						ClusterId: aws.String(jobFlowSteps.JobflowID),
					},
					request.WithWaiterDelay(request.ConstantWaiterDelay(45*time.Second)),
					func(w *request.Waiter) {
						w.MaxAttempts = 26880
					},
				)
				if err != nil {
					if lock != nil && softLock != "" {
						lock.Unlock()
					}
					return exitCodeError(sentryEnabled, err)
				}

				log.Info("EMR cluster with ID [" + jobFlowSteps.JobflowID + "] is terminated successfully")

				failedStepIDs, err := jobFlowSteps.GetFailedStepIDs()

				if logFailedSteps && len(failedStepIDs) > 0 {
					displayFailedStepsLogs(failedStepIDs, emrPlaybook, jobFlowSteps.JobflowID, vars)
				}

				if err != nil {
					if lock != nil && softLock != "" {
						lock.Unlock()
					}
					return exitCodeError(sentryEnabled, err)
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

func getLogFailedStepsFlag() cli.BoolFlag {
	return cli.BoolFlag{
		Name:  fLogFailedSteps,
		Usage: "Whether or not to retrieve and display the logs for any failed step",
	}
}

func getLockFlag() cli.StringFlag {
	usage := "Path to the lock held for the duration of the jobflow steps. This is materialized" +
		" by a file or a KV entry in Consul depending on the --" + fConsul + " flag."
	return cli.StringFlag{
		Name:  fLock,
		Usage: usage,
	}
}

func getSoftLockFlag() cli.StringFlag {
	usage := "Path to the lock held for the duration of the jobflow steps. This is materialized" +
		" by a file or a KV entry in Consul depending on the --" + fConsul + " flag. Released no" +
		" matter if the operation failed or succeeded."
	return cli.StringFlag{
		Name:  fSoftLock,
		Usage: usage,
	}
}

func getConsulFlag() cli.StringFlag {
	return cli.StringFlag{
		Name:  fConsul,
		Usage: "Address of the Consul server used for distributed locking",
	}
}

func getSentryFlag() cli.StringFlag {
	return cli.StringFlag{
		Name:  fSentry,
		Usage: "The Sentry DSN to send errors to",
	}
}

// --- Commands

// up launches a new EMR cluster
func up(emrConfig string, vars string) (string, error) {
	clusterRecord, err := parseClusterRecord(emrConfig, vars)
	if err != nil {
		return "", err
	}

	return upWithConfig(clusterRecord)
}

func upWithConfig(clusterRecord *ClusterConfig) (string, error) {
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

func runJobFlowWithSteps(emrCluster *EmrCluster, playbookRecord *PlaybookConfig) (*JobFlowSteps, error) {

	jobFlowInput, err := emrCluster.GetJobFlowInput(false)
	if err != nil {
		return nil, err
	}

	jobFlowSteps, err := InitJobFlowSteps(*playbookRecord, "", false)
	if err != nil {
		return nil, err
	}

	addJobFlowStepsInput, err := jobFlowSteps.GetJobFlowStepsInput()
	if err != nil {
		return nil, err
	}

	jobFlowInput.Steps = addJobFlowStepsInput.Steps

	jobFlowOutput, err := emrCluster.Svc.RunJobFlow(jobFlowInput)
	if err != nil {
		return nil, err
	}

	jobFlowSteps.JobflowID = *jobFlowOutput.JobFlowId

	return jobFlowSteps, nil
}

// log the failed steps by printing out the different log files for each failed step
func displayFailedStepsLogs(failedStepsIDs []string, emrPlaybook, jobflowID, vars string) {
	playbookRecord, err := parsePlaybookRecord(emrPlaybook, vars)
	if err != nil {
		log.Error("Couldn't parse playbook record: " + err.Error())
	}
	logsDownloader, err := InitLogsDownloader(
		playbookRecord.Credentials.AccessKeyId,
		playbookRecord.Credentials.SecretAccessKey,
		playbookRecord.Region,
		jobflowID,
	)
	if err != nil {
		log.Error("Couldn't retrieve failed steps' logs: " + err.Error())
	}
	for _, stepID := range failedStepsIDs {
		logs, err := logsDownloader.GetStepLogs(stepID)
		if err != nil {
			log.Error("Couldn't retrieve logs for step " + stepID + ": " + err.Error())
		}
		for filename, content := range logs {
			log.Info("Content of log file '" + filename + "' for step " + stepID + ":")
			log.Info(content)
		}
	}
}

// run adds steps to an EMR cluster and return the failed steps' IDs
func run(emrPlaybook, emrCluster string, async bool, vars string) ([]string, error) {
	playbookRecord, err := parsePlaybookRecord(emrPlaybook, vars)
	if err != nil {
		return nil, err
	}

	return runWithConfig(playbookRecord, emrCluster, async)
}

func runWithConfig(playbookRecord *PlaybookConfig, emrCluster string, async bool) ([]string, error) {
	jfs, err := InitJobFlowSteps(*playbookRecord, emrCluster, async)
	if err != nil {
		return nil, err
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

	return downWithConfig(clusterRecord, emrCluster)
}

func downWithConfig(clusterRecord *ClusterConfig, emrCluster string) error {
	ec, err := InitEmrCluster(*clusterRecord)
	if err != nil {
		return err
	}
	return ec.TerminateJobFlow(emrCluster)
}

// --- Helpers

func initializeSentry(dsn string) error {
	log.Info("Initializing sentry with dsn: " + dsn)
	return sentry.Init(sentry.ClientOptions{
		Dsn:     dsn,
		Release: cliVersion,
	})
}

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
func exitCodeError(sentryEnabled bool, err error) error {
	if sentryEnabled {
		sentry.CaptureException(err)
		sentry.Flush(time.Second * 5)
	}

	switch err.(type) {
	case LockHeldError:
		log.Warn(err.Error())
		return cli.NewExitError(err.Error(), lockHeldExitCode)
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
		err := lock.TryLock()
		if err != nil {
			return nil, err
		}
	}
	return lock, nil
}

// parses a playbook record
func parsePlaybookRecord(emrPlaybook, vars string) (*PlaybookConfig, error) {
	if emrPlaybook == "" {
		return nil, flagToError(fEmrPlaybook)
	}

	varMap, err := varsToMap(vars)
	if err != nil {
		return nil, err
	}

	ar, err := InitConfigResolver()
	if err != nil {
		return nil, err
	}

	return ar.ParsePlaybookRecordFromFile(emrPlaybook, varMap)
}

// parses a cluster record
func parseClusterRecord(emrConfig, vars string) (*ClusterConfig, error) {
	if emrConfig == "" {
		return nil, flagToError(fEmrConfig)
	}

	varMap, err := varsToMap(vars)
	if err != nil {
		return nil, err
	}

	ar, err := InitConfigResolver()
	if err != nil {
		return nil, err
	}

	return ar.ParseClusterRecordFromFile(emrConfig, varMap)
}
