//
// Copyright (c) 2016-2026 Snowplow Analytics Ltd. All rights reserved.
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
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/emr"
	"github.com/aws/aws-sdk-go-v2/service/emr/types"
	log "github.com/sirupsen/logrus"

	"github.com/snowplow-devops/go-retry"
)

const (
	// transientBootstrapJitterSeconds bounds the random pause between attempts.
	// There is no floor: a relaunch already takes minutes (teardown,
	// re-provisioning, bootstrap), so the jitter serves only to stop a cohort of
	// runners that failed on a shared dependency relaunching in lockstep. `up`
	// uses a wider bound, bootstrapFailureSleepSeconds.
	transientBootstrapJitterSeconds = 30
)

// launchTransientCluster launches a cluster with the playbook's steps attached and
// returns the new jobflow ID.
//
// The steps go in the RunJobFlow call rather than a later AddJobFlowSteps: the
// cluster then self-terminates once they finish, so no live cluster is left
// behind if this process dies.
func launchTransientCluster(ctx context.Context, ec *EmrCluster, jfs *JobFlowSteps) (string, error) {
	jobFlowInput, err := ec.GetJobFlowInput(false)
	if err != nil {
		return "", err
	}

	stepsInput, err := jfs.GetJobFlowStepsInput()
	if err != nil {
		return "", err
	}
	jobFlowInput.Steps = stepsInput.Steps

	resp, err := retry.ExponentialWithInterface(3, time.Second, "emr.RunJobFlow", func() (any, error) {
		return ec.Svc.RunJobFlow(ctx, jobFlowInput)
	})
	if err != nil {
		return "", err
	}

	return *resp.(*emr.RunJobFlowOutput).JobFlowId, nil
}

// warnClusterLeftUnwatched records that we have stopped watching a transient
// cluster we could not read. It is not terminated — see runTransientAttempt for
// why — so the only trace of it is this line and the jobflow ID it names.
func warnClusterLeftUnwatched(jobflowID string) {
	log.Warnf("EMR cluster %s could not be observed and is being left to run unwatched; "+
		"it has no steps left to submit and will terminate itself", jobflowID)
}

// clusterFailureError builds the error for a cluster the launch wait saw
// terminate, naming the state change reason. It avoids claiming the cluster
// failed to launch: a short job can pass through RUNNING between two polls, so
// this is also reached for a run whose steps ran and failed.
//
// This is the sole owner of logging that reason: callers must not log it
// themselves, or an attempt that is about to be retried logs the same event
// twice.
func clusterFailureError(jobflowID string, status *types.ClusterStatus) error {
	code, message := clusterStateChangeReason(status)
	log.Errorf("EMR cluster state change reason: code='%s' message=%q", code, message)
	return fmt.Errorf("EMR cluster %s failed with state %s: code=%s message=%q",
		jobflowID, status.State, code, message)
}

// runTransientAttempt performs one transient run: launch a cluster with the
// playbook's steps attached, wait for it to come up, then wait for it to finish.
//
// The two waits are separate because a single terminate-wait conflates "the
// cluster never came up" with "the job failed", gives the launch no bound of its
// own, and leaves the outcome dependent on which terminal state EMR picks.
//
// A non-nil status is returned only when the cluster failed to come up, so the
// caller can decide whether to retry. Past launch the status is always nil: a
// step may have run, so re-submitting the playbook would no longer be safe.
func runTransientAttempt(ctx context.Context, ec *EmrCluster, jfs *JobFlowSteps) (*types.ClusterStatus, error) {
	jobflowID, err := launchTransientCluster(ctx, ec, jfs)
	if err != nil {
		return nil, err
	}
	jfs.JobflowID = jobflowID

	log.Infof("Transient EMR run with jobflow ID [%s] started successfully", jobflowID)
	log.Info("Waiting until cluster is running...")

	launchStatus, launchErr := ec.waitForClusterReady(ctx, jobflowID)
	if launchStatus == nil {
		// The cluster could not be read at all, so the failure cannot be
		// classified, and it must never be retried: retrying re-submits the steps.
		//
		// The cluster is deliberately left alone. It is tempting to terminate it —
		// we are about to stop watching something that keeps billing — but nothing
		// here rules out its steps already running. Not observing RUNNING is a
		// statement about our visibility, not about the cluster, and by the time
		// the reads have exhausted their budget a cluster launched minutes ago may
		// well be mid-step. Killing it then leaves half-written output for the next
		// run to reason about, which is worse than an unwatched cluster that
		// finishes the playbook and self-terminates on its own.
		//
		// The launch-timeout branch below does terminate, and safely, because there
		// we have a status and can see the cluster never came up.
		warnClusterLeftUnwatched(jobflowID)
		return nil, launchErr
	}

	if !clusterIsUp(launchStatus) {
		if !clusterIsTerminalish(launchStatus) {
			// The launch wait timed out while the cluster was still coming up:
			// waitForClusterReady returns a non-terminal status alongside its
			// error. Left alone the cluster keeps billing, and may still come up
			// and run the steps after we have reported failure. No step can have
			// run yet, so terminating is safe. The no-wait form, since this path
			// has already spent clusterWaitMaxDuration in the launch waiter and
			// should not spend another watching the teardown — though a throttled
			// terminate can now retry for minutes before reporting that it failed,
			// so "no wait" bounds the success case rather than every case.
			if termErr := ec.TerminateJobFlowNoWait(ctx, jobflowID); termErr != nil {
				log.Warnf("Failed to terminate EMR cluster %s after launch timeout: %v", jobflowID, termErr)
			}
			if launchErr == nil {
				// A non-terminal, non-up status is only expected alongside a
				// launch error; never report success for a cluster that never
				// came up.
				launchErr = fmt.Errorf("EMR cluster %s did not come up in time (state %s)",
					jobflowID, launchStatus.State)
			}
			return nil, launchErr
		}

		// The cluster is on its way out, or already gone. A status observed at
		// TERMINATING may not carry its StateChangeReason yet, so wait for the
		// terminal state and classify from that.
		settled, settledErr := ec.waitForClusterFinished(ctx, jobflowID)

		// Only ever an upgrade. Take the settled status when it carries the reason
		// the launch status lacked, or when neither has one and it at least names
		// the state the cluster actually reached — never the reverse.
		//
		// waitForClusterFinished can hand back a reasonless TERMINATED when its own
		// recovery describe was throttled out, and a bootstrap failure often
		// presents as plain TERMINATED, which is that waiter's success acceptor.
		// Overwriting unconditionally would replace a BOOTSTRAP_FAILURE with
		// nothing, and the retry decision below keys on exactly that.
		if settled != nil && (settled.StateChangeReason != nil || launchStatus.StateChangeReason == nil) {
			launchStatus = settled
		}

		// A bare terminal state is not enough to call this success: EMR reports
		// plain TERMINATED even when the failure happened during bootstrap, so
		// waitForClusterFinished's nil error cannot be trusted on its own.
		//
		// Reaching here does not mean no step ran. A short job can pass through
		// RUNNING between two polls, so this also catches runs whose steps ran
		// and failed. Only the reason code distinguishes them, which is why the
		// retry decision keys on BOOTSTRAP_FAILURE alone.
		code, message := clusterStateChangeReason(launchStatus)
		switch {
		case code == string(types.ClusterStateChangeReasonCodeAllStepsCompleted):
			// The cluster passed through RUNNING between two polls and the run
			// has already finished cleanly. Nothing failed.
			log.Infof("EMR cluster with ID [%s] is terminated successfully", jobflowID)
			return nil, nil
		case code != "" || message != "":
			return launchStatus, clusterFailureError(jobflowID, launchStatus)
		default:
			// No reason ever surfaced: the failure cannot be classified, so it
			// must never be retried.
			if settledErr == nil {
				settledErr = fmt.Errorf("EMR cluster %s failed with state %s and no state change reason",
					jobflowID, launchStatus.State)
			}
			return nil, settledErr
		}
	}

	log.Info("Waiting until cluster is terminated...")

	finalStatus, err := ec.waitForClusterFinished(ctx, jobflowID)
	if err != nil {
		code, message := clusterStateChangeReason(finalStatus)
		if code != "" || message != "" {
			log.Errorf("EMR cluster state change reason: code='%s' message=%q", code, message)
			return nil, fmt.Errorf("EMR cluster %s terminated with errors: code=%s message=%q",
				jobflowID, code, message)
		}
		if finalStatus == nil {
			// No status at all: the run is reported failed without our knowing what
			// the cluster did, and it may still be working through the playbook. Say
			// so, as the launch path does — otherwise the only record of a cluster
			// nobody is watching is an error that does not mention it.
			warnClusterLeftUnwatched(jobflowID)
		}
		return nil, err
	}

	// As in the settle branch above, a nil error only means the cluster reached
	// TERMINATED: EMR reports plain TERMINATED even when a step failed. Every
	// outcome here returns a nil status — a step may have run, so none of it is
	// retryable.
	code, message := clusterStateChangeReason(finalStatus)
	switch {
	case code == string(types.ClusterStateChangeReasonCodeAllStepsCompleted):
		log.Infof("EMR cluster with ID [%s] is terminated successfully", jobflowID)
		return nil, nil
	case code != "" || message != "":
		log.Errorf("EMR cluster state change reason: code='%s' message=%q", code, message)
		return nil, fmt.Errorf("EMR cluster %s terminated with state %s: code=%s message=%q",
			jobflowID, finalStatus.State, code, message)
	default:
		// No reason ever surfaced: the run cannot be confirmed complete.
		return nil, fmt.Errorf("EMR cluster %s terminated with state %s and no state change reason, so the run cannot be confirmed complete",
			jobflowID, finalStatus.State)
	}
}

// runTransientJobFlow performs a transient run, relaunching from scratch when the
// cluster fails to bootstrap.
//
// Only BOOTSTRAP_FAILURE is retried: bootstrap actions run before any step, so no
// step can have run and re-submitting the playbook cannot re-process data.
// Reasons like INTERNAL_ERROR or INSTANCE_FAILURE can strike part-way through a
// step, so those — and any failure that cannot be classified — are never
// retried.
func runTransientJobFlow(ctx context.Context, ec *EmrCluster, jfs *JobFlowSteps) error {
	return runTransientJobFlowWithJitter(ctx, ec, jfs, transientBootstrapJitterSeconds)
}

// runTransientJobFlowWithJitter is runTransientJobFlow with the jitter bound
// injected, so tests do not have to sleep. jitterSeconds must be at least 1.
func runTransientJobFlowWithJitter(ctx context.Context, ec *EmrCluster, jfs *JobFlowSteps, jitterSeconds int) error {
	var code, message string

	for attempt := 1; attempt <= bootstrapRetryAttempts; attempt++ {
		launchStatus, err := runTransientAttempt(ctx, ec, jfs)
		if !isBootstrapFailure(launchStatus) {
			// Only a bootstrap failure is retryable. Everything else — success, a
			// job-phase failure, a non-bootstrap launch failure, a launch that
			// timed out, one that could not be classified — stands as it is.
			return err
		}

		code, message = clusterStateChangeReason(launchStatus)

		if attempt == bootstrapRetryAttempts {
			break
		}

		pause := rand.Intn(jitterSeconds)
		log.Warnf("Bootstrap failure detected on attempt %d of %d, retrying in %d seconds...",
			attempt, bootstrapRetryAttempts, pause)
		time.Sleep(time.Second * time.Duration(pause))
	}

	return fmt.Errorf("could not start the cluster due to bootstrap failure after %d attempts: code=%s message=%q",
		bootstrapRetryAttempts, code, message)
}
