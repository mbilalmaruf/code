package adapter

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/hyperledger/fabric-gateway/pkg/client"
	"github.com/hyperledger/fabric-protos-go-apiv2/peer"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"

	"fabric-adapter/internal/ca"
	"fabric-adapter/internal/config"
	"fabric-adapter/internal/fabric"
	"fabric-adapter/internal/profile"
	"fabric-adapter/internal/secret"
	"fabric-adapter/internal/wallet"
)

// Activities are registered on the worker; they hold shared connections.
type Activities struct {
	cfg    *config.Config
	pool   *fabric.Pool
	wallet *wallet.Store
	log    *slog.Logger
}

func NewActivities(cfg *config.Config, pool *fabric.Pool, w *wallet.Store, log *slog.Logger) *Activities {
	return &Activities{cfg: cfg, pool: pool, wallet: w, log: log}
}

func permanent(errType string, format string, a ...any) error {
	return temporal.NewNonRetryableApplicationError(fmt.Sprintf(format, a...), errType, nil)
}

func resolveOrg(raw []byte, org string) (*profile.Org, error) {
	p, err := profile.Parse(raw)
	if err != nil {
		return nil, permanent("InvalidProfile", "%v", err)
	}
	o, err := p.ResolveOrg(org)
	if err != nil {
		return nil, permanent("InvalidProfile", "%v", err)
	}
	return o, nil
}

// ---- identity ---------------------------------------------------------------

// EnsureIdentity returns the user's wallet identity, enrolling it with the
// org's CA first if it is not in the wallet:
//   - enrollmentSecret given → enroll with it;
//   - otherwise → register via the configured registrar, then enroll.
func (a *Activities) EnsureIdentity(ctx context.Context, in IdentityInput) (IdentityOutput, error) {
	org, err := resolveOrg(in.ConnectionProfile, in.Organization)
	if err != nil {
		return IdentityOutput{}, err
	}
	label := a.cfg.Wallet.Label(in.UserID, org.MSPID)
	out := IdentityOutput{Label: label, MSPID: org.MSPID}

	if id, err := a.loadIdentity(ctx, label, org.MSPID); err != nil || id != nil {
		return out, err
	}
	if org.CA == nil {
		return out, permanent("NoCA", "user %q is not in the wallet and organization %s has no certificate authority in the profile", in.UserID, org.Name)
	}
	cac, err := ca.New(org.CA.URL, org.CA.CAName, org.CA.TLSCACertPEM, org.CA.Verify, a.cfg.Timeouts.CA())
	if err != nil {
		return out, permanent("InvalidProfile", "CA %s: %v", org.CA.Key, err)
	}

	secret := in.EnrollmentSecret
	if secret == "" {
		reg, err := a.registrarFor(org.CA)
		if err != nil {
			return out, err
		}
		regCred, err := a.registrarCredential(ctx, cac, reg, org.MSPID)
		if err != nil {
			return out, err
		}
		secret, err = cac.Register(ctx, regCred, ca.RegisterRequest{
			ID: in.UserID, Type: reg.IdentityType, Affiliation: reg.Affiliation,
		})
		var cerr *ca.Error
		if errors.As(err, &cerr) && cerr.AlreadyRegistered() {
			// Possibly a concurrent run registering the same user: give it a
			// chance to finish and store the identity (activity retry).
			if id, lerr := a.loadIdentity(ctx, label, org.MSPID); lerr != nil || id != nil {
				return out, lerr
			}
			return out, temporal.NewApplicationError(fmt.Sprintf(
				"user %q is already registered at the CA but not in the wallet; pass enrollmentSecret if this persists", in.UserID),
				"AlreadyRegistered")
		}
		if err != nil {
			return out, caError("register "+in.UserID, err)
		}
		activity.GetLogger(ctx).Info("registered user at CA", "user", in.UserID, "ca", org.CA.Key)
	}

	cred, err := cac.Enroll(ctx, in.UserID, secret)
	if err != nil {
		return out, caError("enroll "+in.UserID, err)
	}
	switch err := a.wallet.Create(ctx, label, wallet.NewIdentity(org.MSPID, cred.CertificatePEM, cred.PrivateKeyPEM)); {
	case errors.Is(err, wallet.ErrConflict):
		// Someone stored it first; use theirs so all runs share one identity.
		_, err = a.loadIdentity(ctx, label, org.MSPID)
		return out, err
	case err != nil:
		return out, fmt.Errorf("store identity %q: %w", label, err)
	}
	activity.GetLogger(ctx).Info("enrolled user and stored in wallet", "user", in.UserID, "label", label)
	out.Enrolled = true
	return out, nil
}

// loadIdentity returns (nil, nil) when label is absent.
func (a *Activities) loadIdentity(ctx context.Context, label, mspID string) (*wallet.Identity, error) {
	id, err := a.wallet.Get(ctx, label)
	if err != nil || id == nil {
		return nil, err
	}
	if id.MSPID != mspID {
		return nil, permanent("WrongMSP", "wallet identity %q belongs to %s, not %s (set wallet.labelFormat to include {mspId})", label, id.MSPID, mspID)
	}
	s, err := fabric.NewSigner(id.MSPID, []byte(id.Credentials.Certificate), []byte(id.Credentials.PrivateKey))
	if err != nil {
		return nil, permanent("InvalidIdentity", "wallet identity %q: %v", label, err)
	}
	if time.Now().After(s.NotAfter) {
		return nil, permanent("CertificateExpired", "certificate of wallet identity %q expired at %s", label, s.NotAfter.Format(time.RFC3339))
	}
	return id, nil
}

func (a *Activities) registrarFor(c *profile.ResolvedCA) (config.RegistrarConfig, error) {
	for _, r := range a.cfg.Registrars {
		if r.CA == c.Key || (c.CAName != "" && r.CA == c.CAName) || strings.TrimRight(r.CA, "/") == c.URL {
			return r, nil
		}
	}
	if a.cfg.AllowProfileRegistrar && c.Registrar != nil && c.Registrar.EnrollID != "" {
		return config.RegistrarConfig{
			CA: c.Key, EnrollID: c.Registrar.EnrollID, EnrollSecret: secret.Plain(c.Registrar.EnrollSecret),
			WalletLabel: "admin", IdentityType: "client",
		}, nil
	}
	return config.RegistrarConfig{}, permanent("NoRegistrar",
		"user is not in the wallet, no enrollmentSecret was given and no registrar is configured for CA %s", c.Key)
}

// registrarCredential loads the registrar from the wallet, enrolling it once if needed.
func (a *Activities) registrarCredential(ctx context.Context, cac *ca.Client, r config.RegistrarConfig, mspID string) (*ca.Credential, error) {
	id, err := a.loadIdentity(ctx, r.WalletLabel, mspID)
	if err != nil {
		return nil, err
	}
	if id == nil {
		cred, err := cac.Enroll(ctx, r.EnrollID, r.EnrollSecret.String())
		if err != nil {
			return nil, caError("enroll registrar "+r.EnrollID, err)
		}
		if err := a.wallet.Create(ctx, r.WalletLabel, wallet.NewIdentity(mspID, cred.CertificatePEM, cred.PrivateKeyPEM)); err != nil && !errors.Is(err, wallet.ErrConflict) {
			return nil, fmt.Errorf("store registrar identity: %w", err)
		}
		if id, err = a.loadIdentity(ctx, r.WalletLabel, mspID); err != nil || id == nil {
			return nil, fmt.Errorf("registrar identity %q not readable after enroll: %v", r.WalletLabel, err)
		}
	}
	return &ca.Credential{CertificatePEM: []byte(id.Credentials.Certificate), PrivateKeyPEM: []byte(id.Credentials.PrivateKey)}, nil
}

func caError(what string, err error) error {
	var cerr *ca.Error
	if errors.As(err, &cerr) && !cerr.Transient() {
		return permanent("CAError", "%s: %v", what, err)
	}
	return fmt.Errorf("%s: %w", what, err)
}

// ---- transactions -------------------------------------------------------------

// session opens a gateway for the target's user on a peer chosen by attempt
// number, so retries fail over across the org's peers.
func (a *Activities) session(ctx context.Context, t Target) (*client.Gateway, *client.Contract, error) {
	org, err := resolveOrg(t.ConnectionProfile, t.Organization)
	if err != nil {
		return nil, nil, err
	}
	label := a.cfg.Wallet.Label(t.UserID, org.MSPID)
	id, err := a.loadIdentity(ctx, label, org.MSPID)
	if err != nil {
		return nil, nil, err
	}
	if id == nil {
		return nil, nil, permanent("IdentityMissing", "wallet identity %q disappeared", label)
	}
	signer, err := fabric.NewSigner(id.MSPID, []byte(id.Credentials.Certificate), []byte(id.Credentials.PrivateKey))
	if err != nil {
		return nil, nil, permanent("InvalidIdentity", "%v", err)
	}
	attempt := int(activity.GetInfo(ctx).Attempt)
	ep := org.Endpoints[(attempt-1)%len(org.Endpoints)]
	conn, err := a.pool.Conn(ep)
	if err != nil {
		return nil, nil, permanent("InvalidProfile", "%v", err)
	}
	tm := a.cfg.Timeouts
	gw, err := fabric.Connect(conn, signer, fabric.Timeouts{
		Evaluate: tm.Evaluate(), Endorse: tm.Endorse(), Submit: tm.Submit(), CommitStatus: tm.CommitStatus(),
	})
	if err != nil {
		return nil, nil, err
	}
	nw := gw.GetNetwork(t.Channel)
	var c *client.Contract
	if t.Contract != "" {
		c = nw.GetContractWithName(t.Chaincode, t.Contract)
	} else {
		c = nw.GetContract(t.Chaincode)
	}
	activity.GetLogger(ctx).Debug("gateway session", "peer", ep.Name, "user", t.UserID, "attempt", attempt)
	return gw, c, nil
}

func proposalOptions(in ProposalInput) []client.ProposalOption {
	opts := []client.ProposalOption{client.WithArguments(in.Args...)}
	if len(in.Transient) > 0 {
		tm := make(map[string][]byte, len(in.Transient))
		for k, v := range in.Transient {
			tm[k] = []byte(v)
		}
		opts = append(opts, client.WithTransient(tm))
	}
	if len(in.EndorsingOrgs) > 0 {
		opts = append(opts, client.WithEndorsingOrganizations(in.EndorsingOrgs...))
	}
	return opts
}

// gatewayError keeps network failures retryable and makes everything else
// (chaincode errors, endorsement policy failures, access denied) permanent.
func gatewayError(what string, err error) error {
	if fabric.Retryable(err) {
		return fmt.Errorf("%s: %w", what, err)
	}
	return permanent("GatewayError", "%s: %v", what, err)
}

// Evaluate runs a query (no ledger write).
func (a *Activities) Evaluate(ctx context.Context, in ProposalInput) (EvaluateOutput, error) {
	gw, c, err := a.session(ctx, in.Target)
	if err != nil {
		return EvaluateOutput{}, err
	}
	defer gw.Close()
	p, err := c.NewProposal(in.Function, proposalOptions(in)...)
	if err != nil {
		return EvaluateOutput{}, permanent("InvalidProposal", "%v", err)
	}
	res, err := p.EvaluateWithContext(ctx)
	if err != nil {
		return EvaluateOutput{}, gatewayError("evaluate "+in.Function, err)
	}
	return EvaluateOutput{TransactionID: p.TransactionID(), Payload: res}, nil
}

// Endorse collects endorsements. Safe to retry: nothing reaches the ledger.
func (a *Activities) Endorse(ctx context.Context, in ProposalInput) (EndorseOutput, error) {
	gw, c, err := a.session(ctx, in.Target)
	if err != nil {
		return EndorseOutput{}, err
	}
	defer gw.Close()
	p, err := c.NewProposal(in.Function, proposalOptions(in)...)
	if err != nil {
		return EndorseOutput{}, permanent("InvalidProposal", "%v", err)
	}
	tx, err := p.EndorseWithContext(ctx)
	if err != nil {
		return EndorseOutput{}, gatewayError("endorse "+in.Function, err)
	}
	b, err := tx.Bytes()
	if err != nil {
		return EndorseOutput{}, err
	}
	return EndorseOutput{TransactionID: tx.TransactionID(), Prepared: b, Payload: tx.Result()}, nil
}

// Submit sends the endorsed transaction to the orderer. A retry re-sends the
// same transaction ID, which Fabric commits at most once.
func (a *Activities) Submit(ctx context.Context, in SubmitInput) (SubmitOutput, error) {
	gw, _, err := a.session(ctx, in.Target)
	if err != nil {
		return SubmitOutput{}, err
	}
	defer gw.Close()
	tx, err := gw.NewTransaction(in.Prepared)
	if err != nil {
		return SubmitOutput{}, permanent("InvalidTransaction", "%v", err)
	}
	commit, err := tx.SubmitWithContext(ctx)
	if err != nil {
		return SubmitOutput{}, gatewayError("submit "+tx.TransactionID(), err)
	}
	b, err := commit.Bytes()
	if err != nil {
		return SubmitOutput{}, err
	}
	return SubmitOutput{Commit: b}, nil
}

// CommitStatus blocks until the transaction is committed and reports its
// validation code. It heartbeats so a lost worker is detected quickly.
func (a *Activities) CommitStatus(ctx context.Context, in CommitInput) (CommitOutput, error) {
	gw, _, err := a.session(ctx, in.Target)
	if err != nil {
		return CommitOutput{}, err
	}
	defer gw.Close()
	commit, err := gw.NewCommit(in.Commit)
	if err != nil {
		return CommitOutput{}, permanent("InvalidCommit", "%v", err)
	}
	stop := heartbeat(ctx, 10*time.Second)
	defer stop()
	st, err := commit.StatusWithContext(ctx)
	if err != nil {
		return CommitOutput{}, gatewayError("commit status "+commit.TransactionID(), err)
	}
	return CommitOutput{
		Successful:  st.Successful,
		Code:        int32(st.Code),
		CodeName:    peer.TxValidationCode_name[int32(st.Code)],
		BlockNumber: st.BlockNumber,
	}, nil
}

func heartbeat(ctx context.Context, every time.Duration) func() {
	done := make(chan struct{})
	go func() {
		t := time.NewTicker(every)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			case <-t.C:
				activity.RecordHeartbeat(ctx)
			}
		}
	}()
	return func() { close(done) }
}
