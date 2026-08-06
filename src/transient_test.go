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
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/emr"
	"github.com/aws/aws-sdk-go-v2/service/emr/types"
	"github.com/stretchr/testify/assert"
)

// mockEMRAPITransient scripts DescribeCluster responses so that multi-attempt
// behaviour can be tested. attempts[i] is the ordered list of statuses returned
// during the (i+1)th launch; the last entry repeats if more describes arrive than
// were scripted, so extra polls do not panic the test.
//
// Every scripted status should be one the waiters resolve on immediately
// (RUNNING, WAITING, TERMINATING, TERMINATED, TERMINATED_WITH_ERRORS) so the
// tests never actually sleep on a poll interval.
type mockEMRAPITransient struct {
	attempts        [][]*types.ClusterStatus
	runJobFlowCalls int
	describeIdx     int
	terminateCalls  int
	// lastRunJobFlowOptions is the per-call options the last RunJobFlow resolved
	// to, so a test can see which retry budget the launch was made with.
	lastRunJobFlowOptions emr.Options
	// describeErr, when set, fails every DescribeCluster, standing in for a
	// cluster whose state we cannot read at all.
	describeErr error
}

func (m *mockEMRAPITransient) RunJobFlow(ctx context.Context, input *emr.RunJobFlowInput, optFns ...func(*emr.Options)) (*emr.RunJobFlowOutput, error) {
	m.runJobFlowCalls++
	m.describeIdx = 0

	m.lastRunJobFlowOptions = emr.Options{}
	for _, fn := range optFns {
		fn(&m.lastRunJobFlowOptions)
	}

	return &emr.RunJobFlowOutput{JobFlowId: aws.String("j-transient")}, nil
}

func (m *mockEMRAPITransient) DescribeCluster(ctx context.Context, input *emr.DescribeClusterInput, optFns ...func(*emr.Options)) (*emr.DescribeClusterOutput, error) {
	if m.describeErr != nil {
		return nil, m.describeErr
	}

	script := m.attempts[m.runJobFlowCalls-1]
	i := m.describeIdx
	if i >= len(script) {
		i = len(script) - 1
	}
	m.describeIdx++
	return &emr.DescribeClusterOutput{Cluster: &types.Cluster{Status: script[i]}}, nil
}

func (m *mockEMRAPITransient) TerminateJobFlows(ctx context.Context, input *emr.TerminateJobFlowsInput, optFns ...func(*emr.Options)) (*emr.TerminateJobFlowsOutput, error) {
	m.terminateCalls++
	return &emr.TerminateJobFlowsOutput{}, nil
}

func (m *mockEMRAPITransient) AddJobFlowSteps(ctx context.Context, input *emr.AddJobFlowStepsInput, optFns ...func(*emr.Options)) (*emr.AddJobFlowStepsOutput, error) {
	return nil, nil
}

func (m *mockEMRAPITransient) ListSteps(ctx context.Context, input *emr.ListStepsInput, optFns ...func(*emr.Options)) (*emr.ListStepsOutput, error) {
	return nil, nil
}

func (m *mockEMRAPITransient) DescribeStep(ctx context.Context, input *emr.DescribeStepInput, optFns ...func(*emr.Options)) (*emr.DescribeStepOutput, error) {
	return nil, nil
}

// statusWithReason builds a cluster status carrying a state change reason.
func statusWithReason(state types.ClusterState, code types.ClusterStateChangeReasonCode, message string) *types.ClusterStatus {
	return &types.ClusterStatus{
		State: state,
		StateChangeReason: &types.ClusterStateChangeReason{
			Code:    code,
			Message: aws.String(message),
		},
	}
}

// bareStatus builds a cluster status with no state change reason, as can be seen
// at TERMINATING before the reason is populated.
func bareStatus(state types.ClusterState) *types.ClusterStatus {
	return &types.ClusterStatus{State: state}
}

// transientFixture wires a mock EMR client into both an EmrCluster and a
// JobFlowSteps, using cluster fixture 2 because fixture 1 sets both a VPC subnet
// and an availability zone, which GetJobFlowInput rejects.
func transientFixture(attempts [][]*types.ClusterStatus) (*EmrCluster, *JobFlowSteps, *mockEMRAPITransient) {
	svc := &mockEMRAPITransient{attempts: attempts}

	clusterRecord, _ := CR.ParseClusterRecord([]byte(ClusterRecord2), nil, "")
	playbookRecord, _ := CR.ParsePlaybookRecord([]byte(PlaybookRecord1), nil, "")

	ec := &EmrCluster{Config: *clusterRecord, Svc: svc}
	jfs := &JobFlowSteps{Config: *playbookRecord, IsBlocking: true, EmrSvc: svc}

	return ec, jfs, svc
}

func TestRunTransientAttempt_Success(t *testing.T) {
	assert := assert.New(t)
	ec, jfs, svc := transientFixture([][]*types.ClusterStatus{
		{
			bareStatus(types.ClusterStateRunning),
			statusWithReason(types.ClusterStateTerminated, types.ClusterStateChangeReasonCodeAllStepsCompleted, "Steps completed"),
		},
	})

	launchStatus, err := runTransientAttempt(context.Background(), ec, jfs)
	assert.Nil(err)
	assert.Nil(launchStatus)
	assert.Equal(1, svc.runJobFlowCalls)
	assert.Equal("j-transient", jfs.JobflowID)
}

func TestRunTransientAttempt_BootstrapFailure(t *testing.T) {
	assert := assert.New(t)
	// two describes: the launch-phase pre-check, then the settle branch's
	// waitForClusterFinished
	ec, jfs, svc := transientFixture([][]*types.ClusterStatus{
		{
			statusWithReason(types.ClusterStateTerminatedWithErrors, types.ClusterStateChangeReasonCodeBootstrapFailure, "Bootstrap action returned a non-zero return code"),
			statusWithReason(types.ClusterStateTerminatedWithErrors, types.ClusterStateChangeReasonCodeBootstrapFailure, "Bootstrap action returned a non-zero return code"),
		},
	})

	launchStatus, err := runTransientAttempt(context.Background(), ec, jfs)
	assert.NotNil(err)
	// the launch status comes back so the caller can decide whether to retry
	assert.True(isBootstrapFailure(launchStatus))
	assert.Equal(1, svc.runJobFlowCalls)
}

func TestRunTransientAttempt_ValidationErrorAtLaunch(t *testing.T) {
	assert := assert.New(t)
	// two describes: the launch-phase pre-check, then the settle branch's
	// waitForClusterFinished
	ec, jfs, svc := transientFixture([][]*types.ClusterStatus{
		{
			statusWithReason(types.ClusterStateTerminatedWithErrors, types.ClusterStateChangeReasonCodeValidationError, "Subnet is invalid"),
			statusWithReason(types.ClusterStateTerminatedWithErrors, types.ClusterStateChangeReasonCodeValidationError, "Subnet is invalid"),
		},
	})

	launchStatus, err := runTransientAttempt(context.Background(), ec, jfs)
	assert.NotNil(err)
	assert.Contains(err.Error(), "VALIDATION_ERROR")
	assert.False(isBootstrapFailure(launchStatus))
	assert.Equal(1, svc.runJobFlowCalls)
}

func TestRunTransientAttempt_StepFailure(t *testing.T) {
	assert := assert.New(t)
	ec, jfs, svc := transientFixture([][]*types.ClusterStatus{
		{
			bareStatus(types.ClusterStateRunning),
			statusWithReason(types.ClusterStateTerminatedWithErrors, types.ClusterStateChangeReasonCodeStepFailure, "Step failed"),
		},
	})

	launchStatus, err := runTransientAttempt(context.Background(), ec, jfs)
	assert.NotNil(err)
	assert.Contains(err.Error(), "STEP_FAILURE")
	// a job-phase failure must never look retryable: a step may already have run
	assert.Nil(launchStatus)
	assert.Equal(1, svc.runJobFlowCalls)
}

func TestRunTransientAttempt_JobPhaseTerminatedWithNonSuccessReason(t *testing.T) {
	assert := assert.New(t)
	// the cluster reached RUNNING, so a step may have started, and then died
	// mid-step reporting plain TERMINATED with a reason other than
	// ALL_STEPS_COMPLETED. waitForClusterFinished returns a nil error for any
	// plain TERMINATED, so this must still produce an error — and a nil status,
	// since a step may already have run.
	ec, jfs, svc := transientFixture([][]*types.ClusterStatus{
		{
			bareStatus(types.ClusterStateRunning),
			statusWithReason(types.ClusterStateTerminated, types.ClusterStateChangeReasonCodeInstanceFailure, "Instance failure"),
		},
	})

	launchStatus, err := runTransientAttempt(context.Background(), ec, jfs)
	assert.NotNil(err)
	assert.Nil(launchStatus)
	assert.Equal(1, svc.runJobFlowCalls)
}

func TestRunTransientAttempt_ShortJobRace(t *testing.T) {
	assert := assert.New(t)
	// the cluster passed through RUNNING between two polls, so the launch phase
	// only ever sees it terminated — but the run actually succeeded
	ec, jfs, svc := transientFixture([][]*types.ClusterStatus{
		{
			statusWithReason(types.ClusterStateTerminated, types.ClusterStateChangeReasonCodeAllStepsCompleted, "Steps completed"),
			statusWithReason(types.ClusterStateTerminated, types.ClusterStateChangeReasonCodeAllStepsCompleted, "Steps completed"),
		},
	})

	launchStatus, err := runTransientAttempt(context.Background(), ec, jfs)
	assert.Nil(err)
	assert.Nil(launchStatus)
	assert.Equal(1, svc.runJobFlowCalls)
}

func TestRunTransientAttempt_TerminatingWithoutReason(t *testing.T) {
	assert := assert.New(t)
	// the launch phase catches the cluster at TERMINATING before the reason is
	// populated; it must settle to the terminal state before classifying
	ec, jfs, svc := transientFixture([][]*types.ClusterStatus{
		{
			bareStatus(types.ClusterStateTerminating),
			statusWithReason(types.ClusterStateTerminatedWithErrors, types.ClusterStateChangeReasonCodeBootstrapFailure, "Bootstrap action returned a non-zero return code"),
		},
	})

	launchStatus, err := runTransientAttempt(context.Background(), ec, jfs)
	assert.NotNil(err)
	assert.True(isBootstrapFailure(launchStatus))
	assert.Equal(1, svc.runJobFlowCalls)
}

func TestRunTransientAttempt_TerminatingReasonNeverPopulates(t *testing.T) {
	assert := assert.New(t)
	// an unclassifiable failure must not be mistaken for a bootstrap failure
	ec, jfs, svc := transientFixture([][]*types.ClusterStatus{
		{bareStatus(types.ClusterStateTerminating), bareStatus(types.ClusterStateTerminatedWithErrors)},
	})

	launchStatus, err := runTransientAttempt(context.Background(), ec, jfs)
	assert.NotNil(err)
	assert.False(isBootstrapFailure(launchStatus))
	assert.Equal(1, svc.runJobFlowCalls)
}

func TestRunTransientAttempt_LaunchTimeout(t *testing.T) {
	assert := assert.New(t)
	// A real 60-minute timeout cannot be exercised in a test. This simulates
	// the same shape of outcome — waitForClusterReady returning a non-terminal
	// status alongside a non-nil error — by handing it an already-cancelled
	// context instead: the mock ignores ctx and keeps returning the scripted
	// STARTING status, while the SDK waiter's inter-poll wait sees the
	// cancellation and returns promptly, without ever actually sleeping.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	ec, jfs, svc := transientFixture([][]*types.ClusterStatus{
		{bareStatus(types.ClusterStateStarting)},
	})

	launchStatus, err := runTransientAttempt(ctx, ec, jfs)
	assert.NotNil(err)
	// a cluster that never resolved one way or the other must not be retried
	assert.Nil(launchStatus)
	assert.Equal(1, svc.runJobFlowCalls)
	// the still-coming-up cluster must be torn down rather than left running
	assert.Equal(1, svc.terminateCalls)
}

func TestRunTransientAttempt_TerminatedWithBootstrapFailureReason(t *testing.T) {
	assert := assert.New(t)
	// EMR can report plain TERMINATED, not TERMINATED_WITH_ERRORS, even when the
	// cluster never came up because a bootstrap action failed. The launch phase
	// never observed RUNNING here, so a bare TERMINATED must not be read as
	// success.
	// Two describes: the launch-phase pre-check, then the settle branch's
	// waitForClusterFinished.
	ec, jfs, svc := transientFixture([][]*types.ClusterStatus{
		{
			statusWithReason(types.ClusterStateTerminated, types.ClusterStateChangeReasonCodeBootstrapFailure, "Bootstrap action returned a non-zero return code"),
			statusWithReason(types.ClusterStateTerminated, types.ClusterStateChangeReasonCodeBootstrapFailure, "Bootstrap action returned a non-zero return code"),
		},
	})

	launchStatus, err := runTransientAttempt(context.Background(), ec, jfs)
	assert.NotNil(err)
	assert.True(isBootstrapFailure(launchStatus))
	assert.Equal(1, svc.runJobFlowCalls)
}

func TestRunTransientAttempt_TerminatedWithStepFailureReason(t *testing.T) {
	assert := assert.New(t)
	// same shape as above but with a reason that must not be retried.
	// Two describes: the launch-phase pre-check, then the settle branch's
	// waitForClusterFinished.
	ec, jfs, svc := transientFixture([][]*types.ClusterStatus{
		{
			statusWithReason(types.ClusterStateTerminated, types.ClusterStateChangeReasonCodeStepFailure, "Step failed"),
			statusWithReason(types.ClusterStateTerminated, types.ClusterStateChangeReasonCodeStepFailure, "Step failed"),
		},
	})

	launchStatus, err := runTransientAttempt(context.Background(), ec, jfs)
	assert.NotNil(err)
	assert.False(isBootstrapFailure(launchStatus))
	assert.Equal(1, svc.runJobFlowCalls)
}

func TestRunTransientJobFlow_RetriesBootstrapFailureThenSucceeds(t *testing.T) {
	assert := assert.New(t)
	// two describes per attempt: the launch-phase pre-check sees the
	// already-terminal status, then the settle branch's waitForClusterFinished
	// consumes a second before the reason can be classified
	bootstrapFailure := []*types.ClusterStatus{
		statusWithReason(types.ClusterStateTerminatedWithErrors, types.ClusterStateChangeReasonCodeBootstrapFailure, "Bootstrap action returned a non-zero return code"),
		statusWithReason(types.ClusterStateTerminatedWithErrors, types.ClusterStateChangeReasonCodeBootstrapFailure, "Bootstrap action returned a non-zero return code"),
	}
	cleanRun := []*types.ClusterStatus{
		bareStatus(types.ClusterStateRunning),
		statusWithReason(types.ClusterStateTerminated, types.ClusterStateChangeReasonCodeAllStepsCompleted, "Steps completed"),
	}
	ec, jfs, svc := transientFixture([][]*types.ClusterStatus{bootstrapFailure, cleanRun})

	err := runTransientJobFlowWithJitter(context.Background(), ec, jfs, 1)
	assert.Nil(err)
	assert.Equal(2, svc.runJobFlowCalls)
}

func TestRunTransientJobFlow_GivesUpAfterThreeAttempts(t *testing.T) {
	assert := assert.New(t)
	// two describes per attempt, as above
	bootstrapFailure := []*types.ClusterStatus{
		statusWithReason(types.ClusterStateTerminatedWithErrors, types.ClusterStateChangeReasonCodeBootstrapFailure, "Bootstrap action returned a non-zero return code"),
		statusWithReason(types.ClusterStateTerminatedWithErrors, types.ClusterStateChangeReasonCodeBootstrapFailure, "Bootstrap action returned a non-zero return code"),
	}
	ec, jfs, svc := transientFixture([][]*types.ClusterStatus{bootstrapFailure, bootstrapFailure, bootstrapFailure})

	err := runTransientJobFlowWithJitter(context.Background(), ec, jfs, 1)
	assert.NotNil(err)
	assert.Equal(
		"could not start the cluster due to bootstrap failure after 3 attempts: code=BOOTSTRAP_FAILURE message=\"Bootstrap action returned a non-zero return code\"",
		err.Error(),
	)
	assert.Equal(3, svc.runJobFlowCalls)
}

func TestRunTransientJobFlow_DoesNotRetryValidationError(t *testing.T) {
	assert := assert.New(t)
	// two describes: the launch-phase pre-check, then the settle branch's
	// waitForClusterFinished
	ec, jfs, svc := transientFixture([][]*types.ClusterStatus{
		{
			statusWithReason(types.ClusterStateTerminatedWithErrors, types.ClusterStateChangeReasonCodeValidationError, "Subnet is invalid"),
			statusWithReason(types.ClusterStateTerminatedWithErrors, types.ClusterStateChangeReasonCodeValidationError, "Subnet is invalid"),
		},
	})

	err := runTransientJobFlowWithJitter(context.Background(), ec, jfs, 1)
	assert.NotNil(err)
	assert.Contains(err.Error(), "VALIDATION_ERROR")
	assert.Equal(1, svc.runJobFlowCalls)
}

func TestRunTransientJobFlow_DoesNotRetryStepFailure(t *testing.T) {
	assert := assert.New(t)
	// the safety case: a step may already have run, so the playbook must not be
	// re-submitted. Two describes: launch-phase pre-check observes RUNNING (up),
	// then the job-phase waitForClusterFinished observes the step failure.
	ec, jfs, svc := transientFixture([][]*types.ClusterStatus{
		{
			bareStatus(types.ClusterStateRunning),
			statusWithReason(types.ClusterStateTerminatedWithErrors, types.ClusterStateChangeReasonCodeStepFailure, "Step failed"),
		},
	})

	err := runTransientJobFlowWithJitter(context.Background(), ec, jfs, 1)
	assert.NotNil(err)
	assert.Contains(err.Error(), "STEP_FAILURE")
	assert.Equal(1, svc.runJobFlowCalls)
}

func TestRunTransientJobFlow_DoesNotRetryUnclassifiableFailure(t *testing.T) {
	assert := assert.New(t)
	// two describes: launch-phase pre-check sees TERMINATING (not yet reasoned),
	// then the settle branch's waitForClusterFinished sees the terminal state with
	// no reason ever populated.
	ec, jfs, svc := transientFixture([][]*types.ClusterStatus{
		{bareStatus(types.ClusterStateTerminating), bareStatus(types.ClusterStateTerminatedWithErrors)},
	})

	err := runTransientJobFlowWithJitter(context.Background(), ec, jfs, 1)
	assert.NotNil(err)
	assert.Equal(1, svc.runJobFlowCalls)
}

func TestRunTransientJobFlow_SucceedsFirstTime(t *testing.T) {
	assert := assert.New(t)
	// two describes: launch-phase pre-check observes RUNNING (up), then the
	// job-phase waitForClusterFinished observes clean termination.
	ec, jfs, svc := transientFixture([][]*types.ClusterStatus{
		{
			bareStatus(types.ClusterStateRunning),
			statusWithReason(types.ClusterStateTerminated, types.ClusterStateChangeReasonCodeAllStepsCompleted, "Steps completed"),
		},
	})

	err := runTransientJobFlowWithJitter(context.Background(), ec, jfs, 1)
	assert.Nil(err)
	assert.Equal(1, svc.runJobFlowCalls)
}

// TestLaunchDoesNotCarryRaisedRetryBudget guards the reason the raised
// DescribeCluster retry budget is passed per call rather than set on the shared
// EMR client.
//
// RunJobFlow has no idempotency token, so a launch whose response is lost after
// AWS has begun creating the cluster cannot be told apart from one that never
// arrived. Every extra SDK attempt is another chance to create a second cluster,
// and on this path the playbook's steps are attached to the launch request — so
// a duplicate reruns the playbook, orphaned, outside the bootstrap-failure
// reasoning that makes relaunching safe at all.
func TestLaunchDoesNotCarryRaisedRetryBudget(t *testing.T) {
	assert := assert.New(t)
	ec, jfs, svc := transientFixture([][]*types.ClusterStatus{
		{
			bareStatus(types.ClusterStateRunning),
			statusWithReason(types.ClusterStateTerminated, types.ClusterStateChangeReasonCodeAllStepsCompleted, "Steps completed"),
		},
	})

	err := runTransientJobFlowWithJitter(context.Background(), ec, jfs, 1)
	assert.Nil(err)
	assert.Equal(1, svc.runJobFlowCalls)
	assert.Zero(svc.lastRunJobFlowOptions.RetryMaxAttempts,
		"launching must not raise the SDK retry budget: RunJobFlow is not idempotent")
}

// TestRunTransientAttempt_TerminatesWhenLaunchCannotBeObserved covers the case
// where the launch cannot be read at all, throttling that outlasts its budget
// being the way that happens in practice.
//
// The run has to be reported as failed — an unclassifiable failure must never be
// retried, because retrying re-submits the steps — but the cluster must not be
// left behind. We are about to stop watching it, and left alone it keeps billing
// and may still run the whole playbook unobserved.
func TestRunTransientAttempt_TerminatesWhenLaunchCannotBeObserved(t *testing.T) {
	assert := assert.New(t)
	ec, jfs, svc := transientFixture([][]*types.ClusterStatus{
		{bareStatus(types.ClusterStateStarting)},
	})
	svc.describeErr = errors.New("DescribeCluster failed")

	launchStatus, err := runTransientAttempt(context.Background(), ec, jfs)
	assert.NotNil(err)
	// nil status, so runTransientJobFlow will not treat this as retryable
	assert.Nil(launchStatus)
	assert.Equal(1, svc.terminateCalls)
}
