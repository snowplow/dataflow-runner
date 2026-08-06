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
	// cluster. Spread apart, the delay settles at uniform random across the range,
	// which both halves the call rate and decorrelates a cohort of runners that a
	// scheduler started together.
	launchPollMinDelay = 30 * time.Second
	launchPollMaxDelay = 90 * time.Second
	// jobPollMinDelay and jobPollMaxDelay bound the same for a transient run's job
	// phase, which is far longer and far less urgent: nothing acts on the answer
	// until the job finishes, so minutes of extra latency cost nothing and the
	// lower call rate leaves headroom for every other runner in the account.
	jobPollMinDelay = 60 * time.Second
	jobPollMaxDelay = 300 * time.Second

	// emrReadRetryMaxAttempts is how many times the SDK itself retries an EMR
	// read. The default of 3 gives up inside a few hundred milliseconds, which is
	// not long enough to ride out a burst of ThrottlingExceptions on an account
	// running many clusters at once.
	//
	// It is passed per call rather than set on the client, because the client is
	// shared with RunJobFlow. See emrReadRetryOptions.
	emrReadRetryMaxAttempts = 8

	// emrRetryMaxBackoff caps the pause between our own retries. It sits above
	// the longest interval the current attempt counts reach, so it binds only if
	// those are raised.
	emrRetryMaxBackoff = 2 * time.Minute

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

// emrThrottleRetryAttempts and emrThrottleRetryBaseDelay govern how long we wait
// out a throttled EMR read.
//
// The budget is deliberately generous. These reads are how a run learns whether
// it succeeded, so giving up early reports a run whose outcome we merely failed
// to read as a failed run — and each is issued either while a cluster is already
// up or once it has gone, so waiting costs nothing that is not already
// being paid.
//
// These attempts sit on top of the SDK's own, so the real tolerance is longer
// than the sleeps below suggest: the outer sleeps come to three or four minutes,
// but each attempt beneath them can spend up to emrReadRetryMaxAttempts SDK
// tries backing off to a 20s ceiling, plus adaptive mode's token waits. Under
// sustained throttling expect something closer to ten minutes end to end. That
// is the intended direction — patience over a false failure — but it is a good
// deal more patience than the numbers here look like on their own.
//
// emrRetryAttempts and emrRetryBaseDelay are the budget for everything else, and
// are deliberately small: a few seconds, matching what these calls had before
// throttling was ever a concern. They exist because an error that is not a
// throttle is usually permanent but not always — EMR can briefly fail to
// recognise a jobflow ID that RunJobFlow has only just returned — and the price
// of treating that as fatal on the transient path is an abandoned cluster.
//
// These are vars rather than consts only so tests can shrink them; nothing in
// the program reassigns them.
var (
	emrThrottleRetryAttempts  = 6
	emrThrottleRetryBaseDelay = 5 * time.Second
	emrRetryAttempts          = 3
	emrRetryBaseDelay         = time.Second
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
	// calls once AWS starts throttling us, rather than hammering the same
	// exhausted budget. DescribeCluster is throttled account-wide, so an account
	// running many clusters at once sees this well before any single runner is at
	// fault.
	//
	// Note what is deliberately absent: the attempt count stays at the SDK
	// default of 3. Raising it here would raise it for RunJobFlow too, and see
	// emrReadRetryOptions for why that is not a trade worth making. Adaptive
	// mode alone does not change the count — it wraps a standard retryer built
	// with whatever MaxAttempts it is given.
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
	terminateJobFlowsInput := emr.TerminateJobFlowsInput{
		JobFlowIds: []string{jobflowID},
	}

	_, err := retry.ExponentialWithInterface(3, time.Second, "emr.TerminateJobFlow", func() (interface{}, error) {
		return ec.Svc.TerminateJobFlows(ctx, &terminateJobFlowsInput)
	})
	if err != nil {
		return err
	}

	log.Infof("Terminating EMR cluster with jobflow id '%s'...", jobflowID)

	return ec.waitForClusterTerminated(ctx, jobflowID)
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
	var clusterState string
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
		clusterStatus, err := ec.waitForClusterReady(ctx, *runJobFlowOutput.JobFlowId)
		if err != nil && clusterStatus == nil {
			return "", err
		}

		reasonCode, reasonMessage = clusterStateChangeReason(clusterStatus)
		if reasonCode != "" || reasonMessage != "" {
			log.Errorf("EMR cluster state change reason: code='%s' message=%q", reasonCode, reasonMessage)
		}

		if isBootstrapFailure(clusterStatus) {

			retryCount--

			timeout := rand.Intn(sleepTime)
			log.Errorf("Bootstrap failure detected, retrying in %d seconds...", timeout)
			time.Sleep(time.Second * time.Duration(timeout))
		} else {
			done = true
		}

		clusterState = string(clusterStatus.State)
		jobflowID = *runJobFlowOutput.JobFlowId
	}

	if retryCount <= 0 {
		return "", fmt.Errorf("could not start the cluster due to bootstrap failure: code=%s message=%q", reasonCode, reasonMessage)
	}

	if clusterState == "WAITING" {
		return jobflowID, nil
	}
	if reasonCode != "" || reasonMessage != "" {
		return "", fmt.Errorf("EMR cluster failed to launch with state %s: code=%s message=%q", clusterState, reasonCode, reasonMessage)
	}
	return "", fmt.Errorf("EMR cluster failed to launch with state %s", clusterState)
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

// fetchStateChangeReason describes the cluster and returns its StateChangeReason code and message.
// It returns a non-nil error if the cluster cannot be described, or empty strings if no reason is set.
func (ec EmrCluster) fetchStateChangeReason(ctx context.Context, jobflowID string) (string, string, error) {
	resp, err := ec.Svc.DescribeCluster(ctx, &emr.DescribeClusterInput{ClusterId: aws.String(jobflowID)})
	if err != nil {
		return "", "", err
	}
	if resp.Cluster == nil || resp.Cluster.Status == nil {
		return "", "", nil
	}
	code, message := clusterStateChangeReason(resp.Cluster.Status)
	return code, message, nil
}

// describeOutputStatus unwraps a DescribeCluster response's cluster status,
// returning nil rather than panicking on a response that carries none.
func describeOutputStatus(output *emr.DescribeClusterOutput) *types.ClusterStatus {
	if output == nil || output.Cluster == nil {
		return nil
	}
	return output.Cluster.Status
}

// isThrottleError reports whether an error is AWS asking us to slow down, as
// distinct from telling us we asked for something that does not exist. Only the
// former is worth waiting out. It reads through wrapping, so it still recognises
// the MaxAttemptsError the SDK returns once its own retries are exhausted.
func isThrottleError(err error) bool {
	throttles := awsretry.IsErrorThrottles(awsretry.DefaultThrottles)
	return throttles.IsErrorThrottle(err) == aws.TrueTernary
}

// emrReadRetryOptions raises the SDK's own retry budget for a single call. It
// belongs on reads only, which are safe to repeat.
//
// It is applied per call rather than to the client because the client is shared
// with RunJobFlow, and RunJobFlow has no idempotency token — the EMR API offers
// none. A launch whose response is lost after AWS has begun creating the cluster
// is indistinguishable from one that never arrived, so every extra SDK attempt
// is another chance to create a second cluster. On the transient path the
// playbook's steps are attached to the launch request, so a second cluster
// reruns the playbook, and the duplicate is orphaned: we only ever learn the
// jobflow ID of the response we did receive.
//
// That hazard is not new, but it has no business growing to buy DescribeCluster
// some throttling headroom. Overriding MaxAttempts per call keeps the adaptive
// retryer intact — AddWithMaxAttempts wraps it rather than replacing it, so the
// rate limiter still applies.
//
// Deliberately absent from the describes the waiters make internally, which
// reach the client by their own route. Those need no help: a waiter treats an
// API error as "not there yet" and simply polls again, so a throttled poll costs
// nothing but the next interval. Giving them a larger budget would only add
// calls to the budget we are already short of.
func emrReadRetryOptions(o *emr.Options) {
	o.RetryMaxAttempts = emrReadRetryMaxAttempts
}

// emrRetryBackoff is the pause before the next attempt: an exponential doubling
// of base, jittered upwards by up to half so that runners that were throttled
// together do not come back together.
//
// Capped at emrRetryMaxBackoff. The doubling is unbounded otherwise, and the
// attempt counts it is driven by are vars that exist to be adjusted — a handful
// more than are set today would shift well past any interval worth waiting.
func emrRetryBackoff(base time.Duration, attempt int) time.Duration {
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

// retryEmrRead runs an EMR read, waiting out a throttled response on the long
// budget and giving everything else the short one.
//
// This is the shared shape behind every EMR read whose failure would otherwise
// be reported as a failed run. Being told to slow down must not be allowed to
// masquerade as a run that went wrong.
//
// The short budget is what keeps genuinely hopeless answers cheap: a jobflow ID
// that does not exist, which is what a bad --emr-cluster produces, is reported
// in seconds rather than sitting in a waiter until it times out. It is not zero,
// because one non-throttle error does clear on its own — EMR not yet recognising
// a jobflow ID it has just issued.
func retryEmrRead[T any](ctx context.Context, label string, f func() (T, error)) (T, error) {
	var zero T

	for attempt := 1; ; attempt++ {
		result, err := f()
		if err == nil {
			return result, nil
		}

		// Choose the budget from the error in hand rather than the first one
		// seen, so that a throttle arriving mid-sequence still gets waited out.
		attempts, base := emrRetryAttempts, emrRetryBaseDelay
		throttled := isThrottleError(err)
		if throttled {
			attempts, base = emrThrottleRetryAttempts, emrThrottleRetryBaseDelay
		}
		if attempt >= attempts {
			return zero, fmt.Errorf("%s: %w", label, err)
		}

		sleep := emrRetryBackoff(base, attempt)
		if throttled {
			log.Warnf("Throttled calling %s (attempt %d of %d), retrying in %s...",
				label, attempt, attempts, sleep)
		} else {
			log.Warnf("Call to %s failed (attempt %d of %d), retrying in %s: %v",
				label, attempt, attempts, sleep, err)
		}
		select {
		case <-ctx.Done():
			return zero, ctx.Err()
		case <-time.After(sleep):
		}
	}
}

// describeClusterStatus describes a cluster and returns its status.
func (ec EmrCluster) describeClusterStatus(ctx context.Context, input *emr.DescribeClusterInput) (*types.ClusterStatus, error) {
	output, err := retryEmrRead(ctx, "emr.DescribeCluster", func() (*emr.DescribeClusterOutput, error) {
		return ec.Svc.DescribeCluster(ctx, input, emrReadRetryOptions)
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
func (ec EmrCluster) waitForClusterReady(ctx context.Context, jobflowID string) (*types.ClusterStatus, error) {
	input := &emr.DescribeClusterInput{ClusterId: aws.String(jobflowID)}

	// Validate cluster exists before starting the waiter
	status, err := ec.describeClusterStatus(ctx, input)
	if err != nil {
		return nil, err
	}

	// Check if already in a state the waiter would not carry us past
	switch status.State {
	case types.ClusterStateWaiting, types.ClusterStateRunning,
		types.ClusterStateTerminating, types.ClusterStateTerminated, types.ClusterStateTerminatedWithErrors:
		return status, nil
	}

	waiter := emr.NewClusterRunningWaiter(ec.Svc, func(o *emr.ClusterRunningWaiterOptions) {
		o.MinDelay = launchPollMinDelay
		o.MaxDelay = launchPollMaxDelay
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
	status, err = ec.describeClusterStatus(ctx, input)
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
func (ec EmrCluster) waitForClusterFinished(ctx context.Context, jobflowID string) (*types.ClusterStatus, error) {
	input := &emr.DescribeClusterInput{ClusterId: aws.String(jobflowID)}

	status, err := ec.describeClusterStatus(ctx, input)
	if err != nil {
		return nil, err
	}

	// Resolve immediately if the cluster is already in a terminal state
	switch status.State {
	case types.ClusterStateTerminated:
		return status, nil
	case types.ClusterStateTerminatedWithErrors:
		return status, fmt.Errorf("EMR cluster %s terminated with errors", jobflowID)
	}

	waiter := emr.NewClusterTerminatedWaiter(ec.Svc, func(o *emr.ClusterTerminatedWaiterOptions) {
		o.MinDelay = jobPollMinDelay
		o.MaxDelay = jobPollMaxDelay
	})

	// As in waitForClusterReady, take the status from the waiter's own output so
	// that a run which terminated cleanly cannot be turned into a failure by a
	// throttled re-describe. Unlike there the reason has to be present too, since
	// that is what the caller classifies on, and EMR does not always have it
	// attached by the time the cluster reaches TERMINATED.
	output, waiterErr := waiter.WaitForOutput(ctx, input, transientMaxWaitDuration)
	if observed := describeOutputStatus(output); observed != nil && observed.StateChangeReason != nil {
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
	status, err = ec.describeClusterStatus(ctx, input)
	if err != nil {
		return nil, errors.Join(waiterErr, err)
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
