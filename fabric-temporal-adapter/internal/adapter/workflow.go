package adapter

import (
	"fmt"
	"time"
	"unicode/utf8"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// Retryable commit codes: the transaction lost a race with another one;
// re-endorsing against fresh state usually succeeds.
var conflictCodes = map[string]bool{
	"MVCC_READ_CONFLICT":    true,
	"PHANTOM_READ_CONFLICT": true,
}

// activities is used only for typed activity references; it is never called.
var activities *Activities

func retry(maxAttempts int32) *temporal.RetryPolicy {
	return &temporal.RetryPolicy{
		InitialInterval:    time.Second,
		BackoffCoefficient: 2,
		MaximumInterval:    30 * time.Second,
		MaximumAttempts:    maxAttempts,
	}
}

// FabricTransaction resolves the user's identity (wallet, else CA enroll)
// and runs a query or an invoke.
//
// Invoke is split into Endorse → Submit → CommitStatus so a retry never
// creates a second transaction: the tx ID is fixed at endorsement, and a
// re-submit of the same envelope cannot be committed twice.
func FabricTransaction(ctx workflow.Context, req Request) (*Result, error) {
	if err := req.Validate(); err != nil {
		return nil, temporal.NewNonRetryableApplicationError(err.Error(), "InvalidRequest", nil)
	}
	log := workflow.GetLogger(ctx)

	idCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 2 * time.Minute,
		RetryPolicy:         retry(5),
	})
	var id IdentityOutput
	if err := workflow.ExecuteActivity(idCtx, activities.EnsureIdentity, IdentityInput{
		ConnectionProfile: req.ConnectionProfile,
		Organization:      req.Organization,
		UserID:            req.UserID,
		EnrollmentSecret:  req.EnrollmentSecret,
	}).Get(ctx, &id); err != nil {
		return nil, err
	}

	target := Target{
		ConnectionProfile: req.ConnectionProfile, Organization: req.Organization, UserID: req.UserID,
		Channel: req.Channel, Chaincode: req.Chaincode, Contract: req.Contract,
	}
	prop := ProposalInput{Target: target, Function: req.Function, Args: req.Args,
		Transient: req.Transient, EndorsingOrgs: req.EndorsingOrgs}

	txCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 2 * time.Minute,
		RetryPolicy:         retry(5),
	})

	if req.CallType == CallQuery {
		var out EvaluateOutput
		if err := workflow.ExecuteActivity(txCtx, activities.Evaluate, prop).Get(ctx, &out); err != nil {
			return nil, err
		}
		return newResult(out.TransactionID, out.Payload, id.Enrolled), nil
	}

	commitTimeout := 5 * time.Minute
	if req.Options.CommitTimeoutSeconds > 0 {
		commitTimeout = time.Duration(req.Options.CommitTimeoutSeconds) * time.Second
	}
	submitCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: time.Minute,
		RetryPolicy:         retry(10),
	})
	commitCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: commitTimeout,
		HeartbeatTimeout:    30 * time.Second,
		RetryPolicy:         retry(5),
	})

	maxRounds := req.maxConflictRetries() + 1
	for round := 1; ; round++ {
		var e EndorseOutput
		if err := workflow.ExecuteActivity(txCtx, activities.Endorse, prop).Get(ctx, &e); err != nil {
			return nil, err
		}
		var s SubmitOutput
		if err := workflow.ExecuteActivity(submitCtx, activities.Submit, SubmitInput{Target: target, Prepared: e.Prepared}).Get(ctx, &s); err != nil {
			return nil, err
		}
		var c CommitOutput
		if err := workflow.ExecuteActivity(commitCtx, activities.CommitStatus, CommitInput{Target: target, Commit: s.Commit}).Get(ctx, &c); err != nil {
			return nil, err
		}
		if c.Successful {
			r := newResult(e.TransactionID, e.Payload, id.Enrolled)
			r.BlockNumber, r.ValidationCode, r.Attempts = c.BlockNumber, c.CodeName, round
			return r, nil
		}
		if conflictCodes[c.CodeName] && round < maxRounds {
			log.Warn("transaction invalidated by conflict; re-endorsing", "txId", e.TransactionID, "code", c.CodeName, "round", round)
			if err := workflow.Sleep(ctx, time.Duration(round)*time.Second); err != nil {
				return nil, err
			}
			continue
		}
		return nil, temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("transaction %s failed to commit: %s", e.TransactionID, c), "CommitFailed", nil, c)
	}
}

func newResult(txID string, payload []byte, enrolled bool) *Result {
	r := &Result{TransactionID: txID, Payload: payload, IdentityEnrolled: enrolled}
	if utf8.Valid(payload) {
		r.PayloadText = string(payload)
	}
	return r
}
