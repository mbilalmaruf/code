// Package adapter holds the Temporal workflow and activities that execute a
// Fabric chaincode query or invoke on behalf of a user.
package adapter

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

// Names other services use to start the workflow (any Temporal SDK).
const (
	WorkflowName = "FabricTransaction"

	CallQuery  = "query"
	CallInvoke = "invoke"
)

// Request is the workflow input.
type Request struct {
	// CallType is "query" (evaluate, no ledger write) or "invoke" (endorse,
	// submit to orderer, wait for commit).
	CallType  string `json:"callType"`
	Channel   string `json:"channel"`
	Chaincode string `json:"chaincode"`
	// Contract is the optional contract name inside the chaincode.
	Contract string   `json:"contract,omitempty"`
	Function string   `json:"function"`
	Args     []string `json:"args,omitempty"`
	// Transient data (private data inputs); not written to the ledger.
	Transient map[string]string `json:"transient,omitempty"`
	// EndorsingOrgs restricts endorsement to these MSP IDs (invoke only).
	EndorsingOrgs []string `json:"endorsingOrgs,omitempty"`

	// UserID is the wallet label / CA enrollment ID of the caller.
	UserID string `json:"userId"`
	// EnrollmentSecret is used if the user is not in the wallet. When empty,
	// the worker registers the user with its configured registrar.
	EnrollmentSecret string `json:"enrollmentSecret,omitempty"`
	// Organization selects the org in the profile (default client.organization).
	Organization string `json:"organization,omitempty"`
	// ConnectionProfile is a Fabric common connection profile (JSON).
	ConnectionProfile json.RawMessage `json:"connectionProfile"`

	Options Options `json:"options,omitempty"`
}

type Options struct {
	// MaxConflictRetries re-runs endorse+submit when the transaction is
	// invalidated by MVCC_READ_CONFLICT / PHANTOM_READ_CONFLICT. Default 3.
	MaxConflictRetries *int `json:"maxConflictRetries,omitempty"`
	// CommitTimeoutSeconds bounds the wait for commit status. Default 300.
	CommitTimeoutSeconds int `json:"commitTimeoutSeconds,omitempty"`
}

func (r *Request) Validate() error {
	var errs []string
	r.CallType = strings.ToLower(strings.TrimSpace(r.CallType))
	if r.CallType != CallQuery && r.CallType != CallInvoke {
		errs = append(errs, `callType must be "query" or "invoke"`)
	}
	for name, v := range map[string]string{"channel": r.Channel, "chaincode": r.Chaincode, "function": r.Function, "userId": r.UserID} {
		if strings.TrimSpace(v) == "" {
			errs = append(errs, name+" is required")
		}
	}
	if len(r.ConnectionProfile) == 0 {
		errs = append(errs, "connectionProfile is required")
	}
	if r.Options.MaxConflictRetries != nil && *r.Options.MaxConflictRetries < 0 {
		errs = append(errs, "options.maxConflictRetries must be >= 0")
	}
	if len(errs) > 0 {
		return errors.New(strings.Join(errs, "; "))
	}
	return nil
}

func (r *Request) maxConflictRetries() int {
	if r.Options.MaxConflictRetries == nil {
		return 3
	}
	return *r.Options.MaxConflictRetries
}

// Result is the workflow output.
type Result struct {
	TransactionID string `json:"transactionId"`
	// Payload is the chaincode response (base64 in JSON).
	Payload []byte `json:"payload,omitempty"`
	// PayloadText is Payload as a string when it is valid UTF-8.
	PayloadText string `json:"payloadText,omitempty"`
	// Invoke only.
	BlockNumber    uint64 `json:"blockNumber,omitempty"`
	ValidationCode string `json:"validationCode,omitempty"`
	// IdentityEnrolled is true when this run enrolled the user.
	IdentityEnrolled bool `json:"identityEnrolled"`
	// Attempts counts endorse+submit rounds (invoke; >1 after MVCC conflicts).
	Attempts int `json:"attempts,omitempty"`
}

// Target identifies who calls what; shared by the transaction activities.
// It carries no secrets: credentials are loaded from the wallet inside each
// activity so private keys never enter workflow history.
type Target struct {
	ConnectionProfile json.RawMessage `json:"connectionProfile"`
	Organization      string          `json:"organization,omitempty"`
	UserID            string          `json:"userId"`
	Channel           string          `json:"channel"`
	Chaincode         string          `json:"chaincode"`
	Contract          string          `json:"contract,omitempty"`
}

type IdentityInput struct {
	ConnectionProfile json.RawMessage `json:"connectionProfile"`
	Organization      string          `json:"organization,omitempty"`
	UserID            string          `json:"userId"`
	EnrollmentSecret  string          `json:"enrollmentSecret,omitempty"`
}

type IdentityOutput struct {
	Label    string `json:"label"`
	MSPID    string `json:"mspId"`
	Enrolled bool   `json:"enrolled"`
}

type ProposalInput struct {
	Target        Target            `json:"target"`
	Function      string            `json:"function"`
	Args          []string          `json:"args,omitempty"`
	Transient     map[string]string `json:"transient,omitempty"`
	EndorsingOrgs []string          `json:"endorsingOrgs,omitempty"`
}

type EvaluateOutput struct {
	TransactionID string `json:"transactionId"`
	Payload       []byte `json:"payload"`
}

type EndorseOutput struct {
	TransactionID string `json:"transactionId"`
	// Prepared is the serialized endorsed (unsigned) transaction; Submit
	// rebuilds it with Gateway.NewTransaction, keeping the same tx ID.
	Prepared []byte `json:"prepared"`
	Payload  []byte `json:"payload"`
}

type SubmitInput struct {
	Target   Target `json:"target"`
	Prepared []byte `json:"prepared"`
}

type SubmitOutput struct {
	// Commit is the serialized commit-status request (Commit.Bytes()).
	Commit []byte `json:"commit"`
}

type CommitInput struct {
	Target Target `json:"target"`
	Commit []byte `json:"commit"`
}

type CommitOutput struct {
	Successful  bool   `json:"successful"`
	Code        int32  `json:"code"`
	CodeName    string `json:"codeName"`
	BlockNumber uint64 `json:"blockNumber"`
}

func (c CommitOutput) String() string { return fmt.Sprintf("%s (%d)", c.CodeName, c.Code) }
