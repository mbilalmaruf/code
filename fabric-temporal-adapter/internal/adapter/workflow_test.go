package adapter

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/mock"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
)

func baseRequest(callType string) Request {
	return Request{
		CallType: callType, Channel: "mychannel", Chaincode: "basic", Function: "ReadAsset",
		Args: []string{"asset1"}, UserID: "alice", ConnectionProfile: []byte(`{"organizations":{}}`),
	}
}

func newEnv(t *testing.T) *testsuite.TestWorkflowEnvironment {
	s := &testsuite.WorkflowTestSuite{}
	env := s.NewTestWorkflowEnvironment()
	env.RegisterActivity(&Activities{})
	env.OnActivity(activities.EnsureIdentity, mock.Anything, mock.Anything).
		Return(IdentityOutput{Label: "alice", MSPID: "Org1MSP", Enrolled: true}, nil)
	return env
}

func TestQuery(t *testing.T) {
	env := newEnv(t)
	env.OnActivity(activities.Evaluate, mock.Anything, mock.Anything).
		Return(EvaluateOutput{TransactionID: "tx1", Payload: []byte(`{"id":"asset1"}`)}, nil)
	env.ExecuteWorkflow(FabricTransaction, baseRequest("query"))
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	var r Result
	_ = env.GetWorkflowResult(&r)
	if r.PayloadText != `{"id":"asset1"}` || !r.IdentityEnrolled || r.TransactionID != "tx1" {
		t.Fatalf("result = %+v", r)
	}
	env.AssertNotCalled(t, "Endorse", mock.Anything, mock.Anything)
}

func TestInvokeRetriesOnMVCCConflict(t *testing.T) {
	env := newEnv(t)
	env.OnActivity(activities.Endorse, mock.Anything, mock.Anything).
		Return(EndorseOutput{TransactionID: "tx1", Prepared: []byte("p1"), Payload: []byte("ok")}, nil).Once()
	env.OnActivity(activities.Endorse, mock.Anything, mock.Anything).
		Return(EndorseOutput{TransactionID: "tx2", Prepared: []byte("p2"), Payload: []byte("ok")}, nil).Once()
	env.OnActivity(activities.Submit, mock.Anything, mock.Anything).Return(SubmitOutput{Commit: []byte("c")}, nil)
	env.OnActivity(activities.CommitStatus, mock.Anything, mock.Anything).
		Return(CommitOutput{Successful: false, Code: 11, CodeName: "MVCC_READ_CONFLICT"}, nil).Once()
	env.OnActivity(activities.CommitStatus, mock.Anything, mock.Anything).
		Return(CommitOutput{Successful: true, Code: 0, CodeName: "VALID", BlockNumber: 42}, nil).Once()

	req := baseRequest("invoke")
	env.ExecuteWorkflow(FabricTransaction, req)
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	var r Result
	_ = env.GetWorkflowResult(&r)
	if r.TransactionID != "tx2" || r.BlockNumber != 42 || r.Attempts != 2 || r.ValidationCode != "VALID" {
		t.Fatalf("result = %+v", r)
	}
}

func TestInvokeCommitFailureIsFinal(t *testing.T) {
	env := newEnv(t)
	env.OnActivity(activities.Endorse, mock.Anything, mock.Anything).
		Return(EndorseOutput{TransactionID: "tx1", Prepared: []byte("p")}, nil).Once()
	env.OnActivity(activities.Submit, mock.Anything, mock.Anything).Return(SubmitOutput{Commit: []byte("c")}, nil)
	env.OnActivity(activities.CommitStatus, mock.Anything, mock.Anything).
		Return(CommitOutput{Successful: false, Code: 10, CodeName: "ENDORSEMENT_POLICY_FAILURE"}, nil)
	env.ExecuteWorkflow(FabricTransaction, baseRequest("invoke"))
	err := env.GetWorkflowError()
	var ae *temporal.ApplicationError
	if !errors.As(err, &ae) || ae.Type() != "CommitFailed" {
		t.Fatalf("want CommitFailed, got %v", err)
	}
}

func TestConflictRetriesExhausted(t *testing.T) {
	env := newEnv(t)
	env.OnActivity(activities.Endorse, mock.Anything, mock.Anything).Return(EndorseOutput{TransactionID: "tx"}, nil)
	env.OnActivity(activities.Submit, mock.Anything, mock.Anything).Return(SubmitOutput{}, nil)
	env.OnActivity(activities.CommitStatus, mock.Anything, mock.Anything).
		Return(CommitOutput{Code: 11, CodeName: "MVCC_READ_CONFLICT"}, nil)
	req := baseRequest("invoke")
	one := 1
	req.Options.MaxConflictRetries = &one
	env.ExecuteWorkflow(FabricTransaction, req)
	if env.GetWorkflowError() == nil {
		t.Fatal("expected failure after retries")
	}
	env.AssertNumberOfCalls(t, "Endorse", 2)
}

func TestInvalidRequest(t *testing.T) {
	env := newEnv(t)
	req := baseRequest("delete")
	req.UserID = ""
	env.ExecuteWorkflow(FabricTransaction, req)
	var ae *temporal.ApplicationError
	if err := env.GetWorkflowError(); !errors.As(err, &ae) || ae.Type() != "InvalidRequest" {
		t.Fatalf("want InvalidRequest, got %v", err)
	}
}
