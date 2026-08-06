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

package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsretry "github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/emr"
	"github.com/aws/aws-sdk-go-v2/service/emr/types"
	log "github.com/sirupsen/logrus"

	"github.com/snowplow-devops/go-retry"
)

const (
	// launchPollMinDelay and launchPollMaxDelay bound how often the SDK waiters
	// re-describe a cluster that is coming up, or one we have asked to terminate.
	//
	// The two must differ. DescribeCluster is throttled per account and region, so
	// a fleet of runners shares one budget, and smithy only jitters the poll delay
	// when the maximum exceeds the minimum (see waiter.ComputeDelay) — with them
	// equal, every runner polls on the same fixed beat for the life of its
	// cluster. Spread apart, the delay settles at uniform random across the range
	// from the third poll — the first two are still correlated — which halves the
	// call rate and pulls apart a cohort a scheduler started together.
	launchPollMinDelay = 30 * time.Second
	launchPollMaxDelay = 90 * time.Second
	// jobPollMinDelay and jobPollMaxDelay bound the same for a transient run's job
	// phase, which is far longer and far less urgent: nothing acts on the answer
	// until the job finishes, so minutes of extra latency cost nothing and the
	// lower call rate leaves headroom for every other runner in the account.
	jobPollMinDelay = 60 * time.Second
	jobPollMaxDelay = 300 * time.Second

	// emrIdempotentRetryMaxAttempts is how many times the SDK itself retries a
	// repeatable EMR call. The default of 3 gives up inside a few hundred
	// milliseconds, which is not long enough to ride out a burst of
	// ThrottlingExceptions on an account running many clusters at once.
	//
	// It is passed per call rather than set on the client, because the client is
	// shared with RunJobFlow. See emrIdempotentRetryOptions.
	emrIdempotentRetryMaxAttempts = 8

	// emrRetryMaxBackoff caps the pause between our own retries. It sits above
	// the longest interval the current attempt counts reach, so it binds only if
	// those are raised.
	emrRetryMaxBackoff = 2 * time.Minute

	// blindWarnAfterPolls is how many consecutive failing polls a waiter must make
	// before that is worth saying out loud, and blindWarnEveryPolls how often to
	// repeat it until they recover.
	//
	// Counted in polls rather than wall clock so that the threshold means the same
	// thing on both waiters. A fixed duration does not: five minutes is five to
	// ten polls while a cluster launches but as few as two during a job, so a
	// perfectly healthy job under throttling was warning about itself. Five polls
	// is roughly three to five minutes on the launch waiter and ten to twenty on
	// the job one — the same asymmetry the poll intervals already have, for the
	// same reason.
	blindWarnAfterPolls = 5
	blindWarnEveryPolls = 5

	// blindGiveUpAfterPolls is how many consecutive failures of a kind that
	// waiting cannot fix end the wait. Its own constant, deliberately: it decides
	// when a run is abandoned, and tying it to the log cadence above would let
	// someone quieten the logs and shorten that without noticing.
	//
	// Smaller than the warn threshold, so a hopeless spell fails before it would
	// have warned — the give-up error is the louder signal anyway. Three polls is
	// one to five minutes, ample for the one such error that does clear: EMR not
	// yet recognising a jobflow ID it has just issued.
	blindGiveUpAfterPolls = 3

	// maxBlindDuration bounds how long a waiter goes on polling a cluster it
	// cannot read at all.
	//
	// It exists for the job wait, which is bounded by transientMaxWaitDuration and
	// would otherwise spend a fortnight on a cluster that has long since
	// self-terminated. Polling through an outage is worth it while the outage
	// might clear — that is what turns a throttling episode into a successful run
	// rather than a false failure — but after hours of unbroken blindness we are
	// no longer waiting on a blip, and a wedged process is the worse failure. Long
	// enough to ride out any real AWS incident; short enough to be noticed within
	// a shift.
	//
	// Nearer a floor than a bound: blindFor is only evaluated when a poll happens,
	// so overshoot of up to one poll interval is inherent, and the recovery
	// describe that follows spends its own budget on top. Expect six hours and a
	// quarter in practice.
	//
	// Never binds on the launch or terminate waits, which stop at
	// clusterWaitMaxDuration well before this.
	maxBlindDuration = 6 * time.Hour

	// clusterWaitMaxDuration is the maximum time to wait for a cluster to launch,
	// and for a cluster we have asked to terminate to do so.
	clusterWaitMaxDuration = 60 * time.Minute
	// transientMaxWaitDuration is the maximum time to wait for a transient run's
	// steps to finish and its cluster to terminate. It dwarfs
	// clusterWaitMaxDuration because it is bounded by how long the job runs, not
	// by how long provisioning should take.
	transientMaxWaitDuration = 14 * 24 * time.Hour

	// bootstrapRetryAttempts is the total number of times a cluster is launched
	// before giving up on a bootstrap failure, counting the first launch.
	bootstrapRetryAttempts = 3

	// bootstrapFailureSleepSeconds bounds the random pause between `up`'s
	// bootstrap-failure retries. Held in seconds rather than as a Duration
	// because it is passed to rand.Intn.
	bootstrapFailureSleepSeconds = 300
)

// emrLongRetryAttempts and emrLongRetryBaseDelay govern how long we wait out an
// EMR call whose failure might clear on its own.
//
// The budget is deliberately generous. These calls are how a run learns whether
// it succeeded, so giving up early reports a run whose outcome we merely failed
// to read as a failed run — and each is issued either while a cluster is already
// up or once it has gone, so waiting costs nothing that is not already being
// paid.
//
// These sit on top of the SDK's own attempts, so the real tolerance is longer
// than the sleeps below: nearer ten minutes than the three or four they add up
// to, once each attempt's own SDK retries and token waits are counted. Live
// testing against a fully saturated account budget consumed four of the six on
// the post-waiter describe in waitForClusterReady, which is the narrowest margin
// measured and the place a future throttling failure would land.
//
// emrShortRetryAttempts and emrShortRetryBaseDelay are the budget for the rest —
// the answers AWS has actually given us — and are deliberately small: a few
// seconds, matching what these calls had before throttling was ever a concern.
// Not zero, because one such answer does clear on its own: EMR briefly failing to
// recognise a jobflow ID RunJobFlow has just returned.
//
// If throttling still costs us runs in production, widen the polling before
// reaching for these — and that means the 15-second step-status loop in
// job_flow_steps.go as much as the cluster waiters above, since it issues one
// DescribeStep per step per iteration and is much the larger of the two. Retries
// are aimed at a budget that is by definition already exhausted; polling is what
// exhausts it.
//
// These are vars rather than consts only so tests can shrink them; nothing in
// the program reassigns them.
var (
	emrLongRetryAttempts   = 6
	emrLongRetryBaseDelay  = 5 * time.Second
	emrShortRetryAttempts  = 3
	emrShortRetryBaseDelay = time.Second
)

// EMRAPI defines the interface for EMR operations (for mocking in tests)
type EMRAPI interface {
	RunJobFlow(ctx context.Context, params *emr.RunJobFlowInput, optFns ...func(*emr.Options)) (*emr.RunJobFlowOutput, error)
	TerminateJobFlows(ctx context.Context, params *emr.TerminateJobFlowsInput, optFns ...func(*emr.Options)) (*emr.TerminateJobFlowsOutput, error)
	DescribeCluster(ctx context.Context, params *emr.DescribeClusterInput, optFns ...func(*emr.Options)) (*emr.DescribeClusterOutput, error)
	AddJobFlowSteps(ctx context.Context, params *emr.AddJobFlowStepsInput, optFns ...func(*emr.Options)) (*emr.AddJobFlowStepsOutput, error)
	ListSteps(ctx context.Context, params *emr.ListStepsInput, optFns ...func(*emr.Options)) (*emr.ListStepsOutput, error)
	DescribeStep(ctx context.Context, params *emr.DescribeStepInput, optFns ...func(*emr.Options)) (*emr.DescribeStepOutput, error)
}

// EmrCluster is used for starting and terminating clusters
type EmrCluster struct {
	Config ClusterConfig
	Svc    EMRAPI
}

// InitEmrCluster creates a new EmrCluster instance
func InitEmrCluster(clusterConfig ClusterConfig) (*EmrCluster, error) {
	creds, err := GetCredentialsProvider(
		clusterConfig.Credentials.AccessKeyId, clusterConfig.Credentials.SecretAccessKey)
	if err != nil {
		return nil, err
	}

	// Adaptive retry mode adds a client-side rate limiter that slows outgoing
	// calls once AWS starts throttling us, rather than hammering a budget that is
	// already exhausted. The SDK calls it experimental; it earns that here because
	// a run-transient process lives for the whole job and every waiter poll goes
	// through this client, so the limiter has hours to learn. It only ever damps
	// this process's own burst, though — nothing here coordinates a fleet, which
	// is why the poll intervals matter more than any of it.
	//
	// The attempt count deliberately stays at the SDK default: raising it here
	// would raise it for RunJobFlow too. See emrIdempotentRetryOptions.
	//
	// It also depends on an exhausted token bucket sleeping rather than erroring,
	// which is FailOnNoAttemptTokens' default. See TestSDKRetryAssumptions.
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion(clusterConfig.Region),
		config.WithCredentialsProvider(creds),
		config.WithRetryMode(aws.RetryModeAdaptive),
	)
	if err != nil {
		return nil, err
	}

	svc := emr.NewFromConfig(cfg)
	return &EmrCluster{
		Config: clusterConfig,
		Svc:    svc,
	}, nil
}

// TerminateJobFlow attempts to terminate a running cluster
func (ec EmrCluster) TerminateJobFlow(jobflowID string) error {
	return ec.TerminateJobFlowWithContext(context.Background(), jobflowID)
}

// TerminateJobFlowWithContext attempts to terminate a running cluster with context support
func (ec EmrCluster) TerminateJobFlowWithContext(ctx context.Context, jobflowID string) error {
	if err := ec.TerminateJobFlowNoWait(ctx, jobflowID); err != nil {
		return err
	}

	return ec.waitForClusterTerminated(ctx, jobflowID)
}

// TerminateJobFlowNoWait asks for a cluster to be terminated and returns as soon
// as the request is accepted.
//
// This is the form for abandoning a cluster: the caller has already decided the
// run has failed and only wants to stop it billing, so the request being
// accepted is the whole of what it needs. Waiting would delay reporting that
// failure by up to clusterWaitMaxDuration to confirm something nobody is waiting
// to hear. TerminateJobFlowWithContext, which does wait, is for `down`, where
// confirming the cluster is gone is the point of the command.
func (ec EmrCluster) TerminateJobFlowNoWait(ctx context.Context, jobflowID string) error {
	terminateJobFlowsInput := emr.TerminateJobFlowsInput{
		JobFlowIds: []string{jobflowID},
	}

	// The same budget the reads get. Terminating is idempotent — asking twice for
	// a cluster already going down changes nothing — so a throttle here is worth
	// waiting out rather than failing `down` in three seconds.
	_, err := retryEmrCall(ctx, "emr.TerminateJobFlows", func() (*emr.TerminateJobFlowsOutput, error) {
		return ec.Svc.TerminateJobFlows(ctx, &terminateJobFlowsInput, emrIdempotentRetryOptions)
	})
	if err != nil {
		return err
	}

	log.Infof("Terminating EMR cluster with jobflow id '%s'...", jobflowID)

	return nil
}

// RunJobFlow builds the params config and launches an EMR cluster
func (ec EmrCluster) RunJobFlow() (string, error) {
	return ec.RunJobFlowWithContext(context.Background())
}

// RunJobFlowWithContext builds the params config and launches an EMR cluster with context support
func (ec EmrCluster) RunJobFlowWithContext(ctx context.Context) (string, error) {
	return ec.runJobFlow(ctx, bootstrapFailureSleepSeconds)
}

func (ec EmrCluster) runJobFlow(ctx context.Context, sleepTime int) (string, error) {
	params, err := ec.GetJobFlowInput(true)
	if err != nil {
		return "", err
	}

	var done = false
	var retryCount = bootstrapRetryAttempts
	var lastStatus *types.ClusterStatus
	var jobflowID string
	var reasonCode, reasonMessage string

	for !done && retryCount > 0 {
		resp, err := retry.ExponentialWithInterface(3, time.Second, "emr.RunJobFlow", func() (interface{}, error) {
			return ec.Svc.RunJobFlow(ctx, params)
		})
		if err != nil {
			return "", err
		}

		log.Infof("Launching EMR cluster with name '%s'...", ec.Config.Name)

		runJobFlowOutput := resp.(*emr.RunJobFlowOutput)
		jobflowID = *runJobFlowOutput.JobFlowId

		clusterStatus, err := ec.waitForClusterReady(ctx, jobflowID)
		if clusterStatus == nil {
			warnClusterLeftRunning(jobflowID, nil)
			if err == nil {
				err = fmt.Errorf("EMR cluster %s could not be described after launch", jobflowID)
			}
			return "", err
		}

		reasonCode, reasonMessage = clusterStateChangeReason(clusterStatus)
		if reasonCode != "" || reasonMessage != "" {
			// A cluster that came up carries a reason too — an empty code and
			// "Cluster ready to run steps." — so the level has to follow the
			// outcome rather than the mere presence of a reason. Logging a
			// successful launch at error puts every `up` into the alert stream.
			if clusterIsUp(clusterStatus) {
				log.Infof("EMR cluster state change reason: code='%s' message=%q", reasonCode, reasonMessage)
			} else {
				log.Errorf("EMR cluster state change reason: code='%s' message=%q", reasonCode, reasonMessage)
			}
		}

		if isBootstrapFailure(clusterStatus) {

			retryCount--

			// Nothing left to pause before once the budget is spent. The loop was
			// sleeping up to sleepTime on the final attempt and then giving up
			// anyway; runTransientJobFlowWithJitter has always broken out first.
			if retryCount > 0 {
				timeout := rand.Intn(sleepTime)
				// Warn, not error: the launch is being retried, so nothing has
				// failed yet. Matches the transient path, and keeps a recoverable
				// bootstrap failure out of the alert stream.
				log.Warnf("Bootstrap failure detected, retrying in %d seconds...", timeout)
				time.Sleep(time.Second * time.Duration(timeout))
			}
		} else {
			done = true
		}

		lastStatus = clusterStatus
	}

	if retryCount <= 0 {
		return "", fmt.Errorf("could not start the cluster due to bootstrap failure: code=%s message=%q", reasonCode, reasonMessage)
	}

	// clusterIsUp, not a bare WAITING check: the running waiter resolves on
	// RUNNING too, and a cluster observed there is perfectly usable. Failing it
	// would report a healthy cluster as a failed launch and tell the operator to
	// shut it down.
	if clusterIsUp(lastStatus) {
		return jobflowID, nil
	}

	if lastStatus == nil {
		// Unreachable: the loop runs at least once and returns early on a nil
		// status. Guarded anyway, because three separate facts currently hold this
		// dereference off and none of them is local.
		return "", fmt.Errorf("EMR cluster %s could not be described after launch", jobflowID)
	}
	clusterState := string(lastStatus.State)

	warnClusterLeftRunning(jobflowID, lastStatus)
	if reasonCode != "" || reasonMessage != "" {
		return "", fmt.Errorf("EMR cluster failed to launch with state %s: code=%s message=%q", clusterState, reasonCode, reasonMessage)
	}
	return "", fmt.Errorf("EMR cluster failed to launch with state %s", clusterState)
}

// warnClusterLeftRunning names a cluster that `up` launched but is not handing
// back to the caller.
//
// `up` builds its cluster with KeepJobFlowAliveWhenNoSteps, so one that is still
// alive stays alive — there is no self-termination to fall back on as there is on
// the transient path. And the jobflow ID only ever reaches the operator through
// this function's return value, so an `up` that fails leaves them paying for a
// cluster they cannot name unless it is logged here.
//
// Silent when the cluster is on its way out, and when nothing was launched.
func warnClusterLeftRunning(jobflowID string, status *types.ClusterStatus) {
	if jobflowID == "" || clusterIsTerminalish(status) {
		return
	}
	log.Errorf("EMR cluster %s was launched and may still be running, but could not be "+
		"handed back; it will not terminate itself and must be shut down with `down`", jobflowID)
}

// clusterIsUp reports whether the cluster reached a state in which its steps can
// run.
func clusterIsUp(status *types.ClusterStatus) bool {
	return status != nil &&
		(status.State == types.ClusterStateRunning || status.State == types.ClusterStateWaiting)
}

// clusterIsTerminalish reports whether the cluster is on its way out or already
// gone. A status that is neither this nor clusterIsUp means the cluster is still
// coming up, which is what the launch wait returns when it times out.
func clusterIsTerminalish(status *types.ClusterStatus) bool {
	return status != nil &&
		(status.State == types.ClusterStateTerminating ||
			status.State == types.ClusterStateTerminated ||
			status.State == types.ClusterStateTerminatedWithErrors)
}

// clusterStateChangeReason returns the StateChangeReason code and message, or empty strings if unset.
func clusterStateChangeReason(status *types.ClusterStatus) (string, string) {
	if status == nil || status.StateChangeReason == nil {
		return "", ""
	}
	code := string(status.StateChangeReason.Code)
	msg := ""
	if status.StateChangeReason.Message != nil {
		msg = *status.StateChangeReason.Message
	}
	return code, msg
}

// isBootstrapFailure reports whether a cluster status indicates the cluster
// terminated because a bootstrap action failed. This is the only termination
// reason for which relaunching with the same steps attached is safe: bootstrap
// actions run before any step, so no step can have run.
func isBootstrapFailure(status *types.ClusterStatus) bool {
	return status != nil &&
		status.StateChangeReason != nil &&
		status.StateChangeReason.Code == types.ClusterStateChangeReasonCodeBootstrapFailure
}

// blindState is what one poll's outcome means for a run of failing describes.
type blindState struct {
	// blindFor is how long describes have been failing, and polls how many have,
	// both for the operator.
	blindFor time.Duration
	polls    int
	// warn is set on the polls where that is due to be logged.
	warn bool
	// recovered is set on the first success after a spell that was logged.
	recovered bool
	// giveUp is set when the spell is no longer worth waiting out.
	giveUp bool
}

// blindTracker follows a run of consecutive failing describes, so that a waiter
// quietly re-polling through one can say so periodically without logging every
// failure, and can stop rather than poll on forever.
//
// Its state is only advanced by observe, which takes the current time rather
// than reading it, so the whole policy can be tested without waiting for it.
type blindTracker struct {
	since     time.Time
	polls     int
	hardPolls int
	warned    int
}

// observe records one poll outcome and reports what it means. mightClear says
// whether waiting could plausibly change the answer; see errorMightClear.
func (b *blindTracker) observe(now time.Time, err error, mightClear bool) blindState {
	if err == nil {
		recovered := b.warned > 0
		*b = blindTracker{}
		return blindState{recovered: recovered}
	}

	if b.polls == 0 {
		b.since = now
	}
	b.polls++
	if mightClear {
		b.hardPolls = 0
	} else {
		b.hardPolls++
	}

	state := blindState{blindFor: now.Sub(b.since), polls: b.polls}

	// Waiting is only justified while the answer might change. Throttles, dropped
	// connections and 5xx all get the whole wait; an answer AWS has actually given
	// us — a role without DescribeCluster, say — is a misconfiguration a waiter
	// would otherwise sit on for its whole bound.
	state.giveUp = b.hardPolls >= blindGiveUpAfterPolls || state.blindFor >= maxBlindDuration

	if !state.giveUp && b.polls >= blindWarnAfterPolls && b.polls-b.warned >= blindWarnEveryPolls {
		b.warned = b.polls
		state.warn = true
	}
	return state
}

// waiterRetryable is a waiter's per-poll decision. The running and terminated
// waiters take the same shape.
type waiterRetryable func(context.Context, *emr.DescribeClusterInput, *emr.DescribeClusterOutput, error) (bool, error)

// warnWhileBlind wraps a waiter's decision so that a sustained inability to read
// the cluster becomes visible, without disturbing the decision itself.
//
// A waiter treats an API error as "not there yet" and polls again. That is what
// stops a throttled poll failing a run, and it is why a throttled launch phase is
// silent — which is right when the run goes on to succeed. But it also means a
// cluster we cannot read at all produces no output for the life of the wait, up
// to an hour on the launch path, and silence is indistinguishable from a hung
// process. So: nothing for the occasional failure, a warning once it has gone on
// long enough to be worth acting on.
func warnWhileBlind(jobflowID string, inner waiterRetryable) waiterRetryable {
	var tracker blindTracker

	return func(ctx context.Context, in *emr.DescribeClusterInput, out *emr.DescribeClusterOutput, err error) (bool, error) {
		state := tracker.observe(time.Now(), err, errorMightClear(err))

		// Returning an error ends the wait — the waiter loop treats a Retryable
		// error as terminal. See blindTracker.observe for when that is right.
		if state.giveUp {
			return false, fmt.Errorf("EMR cluster %s could not be described on %d consecutive polls over %s, so watching it has been abandoned: %w",
				jobflowID, state.polls, state.blindFor.Round(time.Second), err)
		}

		switch {
		case state.warn:
			log.Warnf("EMR cluster %s has not been describable for %s; still polling: %v",
				jobflowID, state.blindFor.Round(time.Second), err)
		case state.recovered:
			log.Infof("EMR cluster %s is describable again", jobflowID)
		}

		return inner(ctx, in, out, err)
	}
}

// describeOutputStatus unwraps a DescribeCluster response's cluster status,
// returning nil rather than panicking on a response that carries none.
func describeOutputStatus(output *emr.DescribeClusterOutput) *types.ClusterStatus {
	if output == nil || output.Cluster == nil {
		return nil
	}
	return output.Cluster.Status
}

// errorMightClear reports whether waiting could plausibly change the answer.
//
// The SDK's own retryable set draws exactly the line we want: AWS asking us to
// slow down, a connection that failed, a request that timed out, a 5xx — all
// "we could not get an answer", all worth waiting on. What is left is AWS
// answering and saying no: InvalidRequestException for a jobflow ID that does
// not exist, AccessDeniedException for a role without DescribeCluster. No amount
// of waiting improves those.
//
// Throttling alone would be too narrow. A three-minute network partition or a
// run of 502s is the same false failure this change exists to remove, reached
// through a different error code.
//
// Reads through wrapping, so it still recognises the MaxAttemptsError the SDK
// returns once its own retries are exhausted.
func errorMightClear(err error) bool {
	retryables := awsretry.IsErrorRetryables(awsretry.DefaultRetryables)
	return retryables.IsErrorRetryable(err) == aws.TrueTernary
}

// emrIdempotentRetryOptions raises the SDK's own retry budget for a single call.
// It belongs only on calls that are safe to repeat: every read, and
// TerminateJobFlows, which is a no-op against a cluster already on its way out.
//
// It is applied per call rather than to the client because the client is shared
// with RunJobFlow, and no EMR write carries an idempotency token — the API offers
// none. A launch whose response is lost after AWS began creating the cluster
// cannot be told from one that never arrived, so every extra attempt is another
// chance at a second cluster, which on the transient path reruns the playbook.
// Overriding MaxAttempts per call keeps the adaptive rate limiter, which
// AddWithMaxAttempts wraps rather than replaces. See TestSDKRetryAssumptions.
//
// Deliberately absent from the describes the waiters make internally, which
// reach the client by their own route. Those need no help: a waiter treats an
// API error as "not there yet" and polls again, so a throttled poll costs
// nothing but the next interval.
func emrIdempotentRetryOptions(o *emr.Options) {
	o.RetryMaxAttempts = emrIdempotentRetryMaxAttempts
}

// emrRetryBackoff is the pause before the next attempt: an exponential doubling
// of base, jittered upwards by up to half so that runners that were throttled
// together do not come back together.
//
// The doubling is capped at emrRetryMaxBackoff — unbounded otherwise, and the
// attempt counts driving it are vars that exist to be adjusted, so a handful more
// than are set today would run well past any interval worth waiting. The cap
// applies before the jitter, so the longest possible pause is half as much again.
func emrRetryBackoff(base time.Duration, attempt int) time.Duration {
	if base <= 0 {
		return 0
	}

	// Doubled by iteration rather than by shifting, so that reaching the cap ends
	// it before a large attempt number can overflow the shift.
	delay := base
	for i := 1; i < attempt && delay < emrRetryMaxBackoff; i++ {
		delay *= 2
	}
	if delay > emrRetryMaxBackoff {
		delay = emrRetryMaxBackoff
	}
	return delay + time.Duration(rand.Int63n(int64(delay)))/2
}

// retryEmrCall runs an EMR call that is safe to repeat, waiting out a failure
// that might clear on the long budget and giving everything else the short one.
//
// This is the shared shape behind every EMR call whose failure would otherwise be
// reported as a failed run — every read, and TerminateJobFlows. Being unable to
// reach AWS must not be allowed to masquerade as a run that went wrong.
//
// The short budget is what keeps genuinely hopeless answers cheap: a jobflow ID
// that does not exist, which is what a bad --emr-cluster produces, is reported
// in seconds rather than sitting in a waiter until it times out. It is not zero,
// because one non-throttle error does clear on its own — EMR not yet recognising
// a jobflow ID it has just issued.
func retryEmrCall[T any](ctx context.Context, label string, f func() (T, error)) (T, error) {
	var zero T

	for attempt := 1; ; attempt++ {
		result, err := f()
		if err == nil {
			return result, nil
		}

		// Choose the budget from the error in hand rather than the first one
		// seen, so that a throttle arriving mid-sequence still gets waited out.
		attempts, base := emrShortRetryAttempts, emrShortRetryBaseDelay
		if errorMightClear(err) {
			attempts, base = emrLongRetryAttempts, emrLongRetryBaseDelay
		}
		if attempt >= attempts {
			return zero, fmt.Errorf("%s: %w", label, err)
		}

		sleep := emrRetryBackoff(base, attempt)
		log.Warnf("Call to %s failed (attempt %d of %d), retrying in %s: %v",
			label, attempt, attempts, sleep, err)
		select {
		case <-ctx.Done():
			return zero, ctx.Err()
		case <-time.After(sleep):
		}
	}
}

// describeClusterStatus describes a cluster and returns its status.
func (ec EmrCluster) describeClusterStatus(ctx context.Context, input *emr.DescribeClusterInput) (*types.ClusterStatus, error) {
	output, err := retryEmrCall(ctx, "emr.DescribeCluster", func() (*emr.DescribeClusterOutput, error) {
		return ec.Svc.DescribeCluster(ctx, input, emrIdempotentRetryOptions)
	})
	if err != nil {
		return nil, err
	}

	status := describeOutputStatus(output)
	if status == nil {
		return nil, fmt.Errorf("EMR cluster %s was described without a status", aws.ToString(input.ClusterId))
	}
	return status, nil
}

// waitForClusterReady waits for the cluster to reach RUNNING or WAITING state using SDK v2 waiter.
// Returns the cluster status even on failure (needed for bootstrap failure detection).
//
// Note the absence of the existence check its siblings open with. Both callers
// pass a jobflow ID that RunJobFlow has just returned, so there is no bad
// --emr-cluster to guard against here, and the waiter's first poll is issued with
// no delay — it asks the same question the check would have, and resolves the
// same already-terminal states through its own acceptors. Dropping it removes a
// duplicate call from the path where call volume is the whole problem, and with
// it the only way this function can return no status while the cluster is
// happily running: a throttled first poll is now swallowed and re-polled rather
// than spending a retry budget to decide the launch failed.
func (ec EmrCluster) waitForClusterReady(ctx context.Context, jobflowID string) (*types.ClusterStatus, error) {
	input := &emr.DescribeClusterInput{ClusterId: aws.String(jobflowID)}

	waiter := emr.NewClusterRunningWaiter(ec.Svc, func(o *emr.ClusterRunningWaiterOptions) {
		o.MinDelay = launchPollMinDelay
		o.MaxDelay = launchPollMaxDelay
		o.Retryable = warnWhileBlind(jobflowID, o.Retryable)
	})

	// WaitForOutput rather than Wait: it hands back the very DescribeCluster
	// response that satisfied the acceptor, so a cluster that comes up needs no
	// further call. Re-describing here instead put one unretried DescribeCluster
	// on the critical path, fired at the exact moment the waiter resolved — and a
	// throttled one abandoned a launch that had in fact succeeded, leaving the
	// cluster running its steps unwatched.
	output, waiterErr := waiter.WaitForOutput(ctx, input, clusterWaitMaxDuration)
	if observed := describeOutputStatus(output); observed != nil {
		return observed, nil
	}

	// The waiter returns no output when it errors or times out, so the status has
	// to be recovered before the caller can classify what happened.
	status, err := ec.describeClusterStatus(ctx, input)
	if err != nil {
		return nil, errors.Join(waiterErr, err)
	}
	return status, waiterErr
}

// waitForClusterTerminated waits for the cluster to terminate using SDK v2 waiter.
func (ec EmrCluster) waitForClusterTerminated(ctx context.Context, jobflowID string) error {
	input := &emr.DescribeClusterInput{ClusterId: aws.String(jobflowID)}

	// Validate cluster exists before starting the waiter
	status, err := ec.describeClusterStatus(ctx, input)
	if err != nil {
		return err
	}

	// Check if already terminated
	if status.State == types.ClusterStateTerminated ||
		status.State == types.ClusterStateTerminatedWithErrors {
		return nil
	}

	waiter := emr.NewClusterTerminatedWaiter(ec.Svc, func(o *emr.ClusterTerminatedWaiterOptions) {
		o.MinDelay = launchPollMinDelay
		o.MaxDelay = launchPollMaxDelay
		o.Retryable = warnWhileBlind(jobflowID, o.Retryable)
	})

	return waiter.Wait(ctx, input, clusterWaitMaxDuration)
}

// waitForClusterFinished waits for a transient cluster to finish its steps and
// terminate, returning the final cluster status so the caller can classify the
// outcome.
//
// It is separate from waitForClusterTerminated, which serves `down`: there the
// cluster was asked to terminate and terminating with errors is still success.
// Here TERMINATED_WITH_ERRORS means the run itself failed.
//
// It has no existence check either, for the reason waitForClusterReady has none:
// its callers pass a jobflow ID RunJobFlow has just returned. That check was also
// the more dangerous of the two — it fired at the moment the launch waiter
// resolved, and exhausting its budget reported the run as failed while the
// cluster went on running its steps.
func (ec EmrCluster) waitForClusterFinished(ctx context.Context, jobflowID string) (*types.ClusterStatus, error) {
	input := &emr.DescribeClusterInput{ClusterId: aws.String(jobflowID)}

	// This leans on one property of the SDK's generated acceptors:
	// TERMINATED_WITH_ERRORS has to be a *failure* acceptor for the wait to end at
	// all. It is, in clusterTerminatedStateRetryable — but a state matching no
	// acceptor is simply polled again, so were that to change, every bootstrap
	// failure would poll for transientMaxWaitDuration instead of being classified.
	// The existence check that used to catch it directly is gone.
	// TestWaitForClusterFinished_WithWaiter_Failure is the guard.
	waiter := emr.NewClusterTerminatedWaiter(ec.Svc, func(o *emr.ClusterTerminatedWaiterOptions) {
		o.MinDelay = jobPollMinDelay
		o.MaxDelay = jobPollMaxDelay
		o.Retryable = warnWhileBlind(jobflowID, o.Retryable)
	})

	// As in waitForClusterReady, take the status from the waiter's own output so
	// that a run which terminated cleanly cannot be turned into a failure by a
	// throttled re-describe. Unlike there the reason has to be present too, since
	// that is what the caller classifies on, and EMR does not always have it
	// attached by the time the cluster reaches TERMINATED.
	output, waiterErr := waiter.WaitForOutput(ctx, input, transientMaxWaitDuration)
	observed := describeOutputStatus(output)
	if observed != nil && observed.StateChangeReason != nil {
		return observed, nil
	}

	// Otherwise the status has to be recovered: the waiter returns no output at
	// all when it errors or times out.
	//
	// A reasonless TERMINATED is deliberately not returned as good enough on its
	// own here. The caller cannot confirm a run complete without
	// ALL_STEPS_COMPLETED, so it would fail the run either way — and reporting
	// why the cluster could not be described says more than the caller's "no
	// state change reason", which hides that throttling was the cause.
	status, err := ec.describeClusterStatus(ctx, input)
	if err != nil {
		// observed rather than nil: when the waiter did see the cluster terminate
		// and only its reason is missing, discarding that makes the caller announce
		// a cluster "left to run unwatched" that has already gone. The outcome is
		// unchanged — the caller still cannot confirm the run — but the log stops
		// sending someone after a cluster that no longer exists.
		return observed, errors.Join(waiterErr, err)
	}
	return status, waiterErr
}

// --- Parameter builders

// GetJobFlowInput parses the ClusterRecord and returns a
// RunJobFlowInput object which can launch an EMR cluster
func (ec EmrCluster) GetJobFlowInput(keepJobFlowAliveWhenNoSteps bool) (*emr.RunJobFlowInput, error) {
	ec2 := ec.Config.Ec2

	ec2Subnet, placement, err := ec.GetLocation()
	if err != nil {
		return nil, err
	}

	// JobFlowInstancesConfig set
	instances := &types.JobFlowInstancesConfig{
		Ec2KeyName:     aws.String(ec.Config.Ec2.KeyName),
		Ec2SubnetId:    aws.String(ec2Subnet),
		InstanceGroups: ec.GetInstanceGroups(),
		Placement: &types.PlacementType{
			AvailabilityZone: aws.String(placement),
		},
		KeepJobFlowAliveWhenNoSteps: aws.Bool(keepJobFlowAliveWhenNoSteps),
	}

	applications, err := ec.GetApplications()
	if err != nil {
		return nil, err
	}

	// RunJobFlowInput configs set
	params := &emr.RunJobFlowInput{
		Instances:             instances,
		Name:                  aws.String(ec.Config.Name),
		JobFlowRole:           aws.String(ec.Config.Roles.Jobflow),
		ServiceRole:           aws.String(ec.Config.Roles.Service),
		LogUri:                aws.String(ec.Config.LogUri),
		Tags:                  ec.GetTags(),
		BootstrapActions:      ec.GetBootstrapActions(),
		Configurations:        ec.GetConfigurations(),
		VisibleToAllUsers:     aws.Bool(true),
		Applications:          applications,
		SecurityConfiguration: aws.String(ec.Config.SecurityConfiguration),
	}

	// Check to see if version < 4.x
	majorVersion, err := ec.GetAmiVersionMajor()
	if err != nil {
		return nil, err
	}

	if majorVersion < 4 {
		params.AmiVersion = aws.String(ec2.AmiVersion)
	} else {
		params.ReleaseLabel = aws.String("emr-" + ec2.AmiVersion)
	}

	return params, nil
}

// GetLocation figures out where the EMR Cluster is going to be placed,
// either in a classic VPC or within a created subnet
func (ec EmrCluster) GetLocation() (string, string, error) {
	location := ec.Config.Ec2.Location

	ec2Subnet := ""
	placement := ""

	if location.Vpc != nil && location.Classic != nil {
		return "", "", fmt.Errorf("Only one of Availability Zone and Subnet id should be provided")
	} else if location.Vpc != nil {
		ec2Subnet = location.Vpc.SubnetId
	} else if location.Classic != nil {
		placement = location.Classic.AvailabilityZone
	} else {
		return "", "", fmt.Errorf("At least one of Availability Zone and Subnet id is required")
	}

	return ec2Subnet, placement, nil
}

// GetInstanceGroups builds the instance groups array
func (ec EmrCluster) GetInstanceGroups() []types.InstanceGroupConfig {
	instances := ec.Config.Ec2.Instances

	var instanceGroups = []types.InstanceGroupConfig{
		{
			InstanceCount: aws.Int32(1),
			InstanceRole:  types.InstanceRoleTypeMaster,
			InstanceType:  aws.String(instances.Master.Type),
		},
		{
			InstanceCount: aws.Int32(int32(instances.Core.Count)),
			InstanceRole:  types.InstanceRoleTypeCore,
			InstanceType:  aws.String(instances.Core.Type),
		},
		{
			InstanceCount: aws.Int32(int32(instances.Task.Count)),
			InstanceRole:  types.InstanceRoleTypeTask,
			InstanceType:  aws.String(instances.Task.Type),
		},
	}

	// If task instance bid is provided setting the BidPrice  for the task instance
	if instances.Task.Bid != "" {
		instanceGroups[2].BidPrice = aws.String(instances.Task.Bid)

		// SPOT instance since a bid price parameter is mentioned
		instanceGroups[2].Market = types.MarketTypeSpot
	}

	if instances.Master.EbsConfiguration != nil {
		instanceGroups[0].EbsConfiguration = GetEbsConfiguration(instances.Master.EbsConfiguration)
	}
	if instances.Core.EbsConfiguration != nil {
		instanceGroups[1].EbsConfiguration = GetEbsConfiguration(instances.Core.EbsConfiguration)
	}
	if instances.Task.EbsConfiguration != nil {
		instanceGroups[2].EbsConfiguration = GetEbsConfiguration(instances.Task.EbsConfiguration)
	}

	if instances.Task.Count > 0 && instances.Core.Count <= 0 {
		// Removing core config when there are no such instances
		instanceGroups = append(instanceGroups[0:1], instanceGroups[2])
	} else if instances.Core.Count > 0 && instances.Task.Count <= 0 {
		// Removing task config when there are no such instances
		instanceGroups = instanceGroups[0:2]
	} else if instances.Core.Count <= 0 && instances.Task.Count <= 0 {
		// Removing task and core configs when there are no such instances mentioned
		instanceGroups = instanceGroups[0:1]
	}

	return instanceGroups
}

// GetEbsConfiguration turns a EbsConfigurationRecord into an types.EbsConfiguration
func GetEbsConfiguration(c *EbsConfigurationRecord) *types.EbsConfiguration {
	configs := c.EbsBlockDeviceConfigs

	var emrConfigsArr []types.EbsBlockDeviceConfig

	if configs != nil && len(configs) > 0 {
		emrConfigsArr = make([]types.EbsBlockDeviceConfig, len(configs))

		for i, config := range configs {
			emrVolumeSpec := &types.VolumeSpecification{
				SizeInGB:   aws.Int32(int32(config.VolumeSpecification.SizeInGB)),
				VolumeType: aws.String(config.VolumeSpecification.VolumeType),
			}
			if *emrVolumeSpec.VolumeType != "gp2" && *emrVolumeSpec.VolumeType != "gp3" {
				emrVolumeSpec.Iops = aws.Int32(int32(config.VolumeSpecification.Iops))
			}

			emrConfig := types.EbsBlockDeviceConfig{
				VolumesPerInstance:  aws.Int32(int32(config.VolumesPerInstance)),
				VolumeSpecification: emrVolumeSpec,
			}

			emrConfigsArr[i] = emrConfig
		}
	}

	return &types.EbsConfiguration{
		EbsBlockDeviceConfigs: emrConfigsArr,
		EbsOptimized:          aws.Bool(c.EbsOptimized),
	}
}

// GetAmiVersionMajor returns the major AmiVersion
func (ec EmrCluster) GetAmiVersionMajor() (int, error) {
	return strconv.Atoi(string(ec.Config.Ec2.AmiVersion[0]))
}

// GetTags builds the tags array
func (ec EmrCluster) GetTags() []types.Tag {
	tags := ec.Config.Tags

	var emrTagsArr []types.Tag

	if tags != nil && len(tags) > 0 {
		emrTagsArr = make([]types.Tag, len(tags))

		for i, tag := range tags {
			emrTag := types.Tag{
				Key:   aws.String(tag.Key),
				Value: aws.String(tag.Value),
			}

			emrTagsArr[i] = emrTag
		}
	}

	return emrTagsArr
}

// GetBootstrapActions builds the bootstrap actions options
func (ec EmrCluster) GetBootstrapActions() []types.BootstrapActionConfig {
	bootstrapActions := ec.Config.BootstrapActionConfigs

	var emrBootstrapActionArr []types.BootstrapActionConfig

	if bootstrapActions != nil && len(bootstrapActions) > 0 {
		emrBootstrapActionArr = make([]types.BootstrapActionConfig, len(bootstrapActions))

		for i, bootstrapAction := range bootstrapActions {
			scriptBootstrapAction := bootstrapAction.ScriptBootstrapAction

			arguments := make([]string, len(scriptBootstrapAction.Args))
			for j, argument := range scriptBootstrapAction.Args {
				arguments[j] = argument
			}

			emrScriptBootstrapAction := types.ScriptBootstrapActionConfig{
				Args: arguments,
				Path: aws.String(scriptBootstrapAction.Path),
			}

			emrBootstrapAction := types.BootstrapActionConfig{
				Name:                  aws.String(bootstrapAction.Name),
				ScriptBootstrapAction: &emrScriptBootstrapAction,
			}

			emrBootstrapActionArr[i] = emrBootstrapAction
		}
	}

	return emrBootstrapActionArr
}

// GetConfigurations builds the configurations options
func (ec EmrCluster) GetConfigurations() []types.Configuration {
	configurations := ec.Config.Configurations

	var emrConfigurationArr []types.Configuration

	if configurations != nil && len(configurations) > 0 {
		emrConfigurationArr = make([]types.Configuration, len(configurations))

		for i, configuration := range configurations {
			propertyMap := make(map[string]string)
			for k, v := range configuration.Properties {
				propertyMap[k] = v
			}

			emrConfiguration := types.Configuration{
				Classification: aws.String(configuration.Classification),
				Properties:     propertyMap,
			}

			emrConfigurationArr[i] = emrConfiguration
		}
	}

	return emrConfigurationArr
}

// GetApplications builds the applications options
func (ec EmrCluster) GetApplications() ([]types.Application, error) {
	applications := ec.Config.Applications

	var emrApplicationArr []types.Application
	if applications != nil && len(applications) > 0 {
		emrApplicationArr = make([]types.Application, len(applications))

		for i, application := range applications {
			emrApplication := types.Application{
				Name: aws.String(application),
			}
			emrApplicationArr[i] = emrApplication
		}
	}

	return emrApplicationArr, nil
}
