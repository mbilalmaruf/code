// Package fabric wraps fabric-gateway: shared gRPC connections per peer and
// gateway sessions for a wallet identity.
package fabric

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/hyperledger/fabric-gateway/pkg/client"
	"github.com/hyperledger/fabric-gateway/pkg/hash"
	"github.com/hyperledger/fabric-gateway/pkg/identity"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"fabric-adapter/internal/profile"
)

// Pool shares one gRPC connection per (address, TLS CA, server name), as
// recommended by fabric-gateway (connections are expensive, gateways cheap).
type Pool struct {
	mu    sync.Mutex
	conns map[string]*grpc.ClientConn
}

func NewPool() *Pool { return &Pool{conns: map[string]*grpc.ClientConn{}} }

func (p *Pool) Conn(ep profile.Endpoint) (*grpc.ClientConn, error) {
	h := sha256.Sum256(ep.TLSCACertPEM)
	key := ep.Address + "|" + ep.ServerNameOverride + "|" + hex.EncodeToString(h[:8])
	p.mu.Lock()
	defer p.mu.Unlock()
	if c, ok := p.conns[key]; ok {
		return c, nil
	}
	var creds grpc.DialOption
	if ep.TLS {
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(ep.TLSCACertPEM) {
			return nil, fmt.Errorf("peer %s: tlsCACerts contain no valid certificate", ep.Name)
		}
		creds = grpc.WithTransportCredentials(credentials.NewClientTLSFromCert(pool, ep.ServerNameOverride))
	} else {
		creds = grpc.WithTransportCredentials(insecure.NewCredentials())
	}
	c, err := grpc.NewClient("dns:///"+ep.Address, creds)
	if err != nil {
		return nil, fmt.Errorf("peer %s: %w", ep.Name, err)
	}
	p.conns[key] = c
	return c, nil
}

func (p *Pool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for k, c := range p.conns {
		_ = c.Close()
		delete(p.conns, k)
	}
}

// Signer is a gateway identity plus its signing function.
type Signer struct {
	ID       *identity.X509Identity
	Sign     identity.Sign
	Hash     hash.Hash
	NotAfter time.Time
}

// NewSigner builds a Signer from PEM credentials. The hash follows the key:
// P-384 → SHA-384, Ed25519 → none, otherwise SHA-256.
func NewSigner(mspID string, certPEM, keyPEM []byte) (*Signer, error) {
	cert, err := identity.CertificateFromPEM(certPEM)
	if err != nil {
		return nil, fmt.Errorf("certificate: %w", err)
	}
	id, err := identity.NewX509Identity(mspID, cert)
	if err != nil {
		return nil, err
	}
	key, err := parsePrivateKey(keyPEM)
	if err != nil {
		return nil, fmt.Errorf("private key: %w", err)
	}
	sign, err := identity.NewPrivateKeySign(key)
	if err != nil {
		return nil, err
	}
	h := hash.SHA256
	switch k := key.(type) {
	case *ecdsa.PrivateKey:
		if k.Curve == elliptic.P384() {
			h = hash.SHA384
		}
		if !k.PublicKey.Equal(cert.PublicKey) {
			return nil, errors.New("private key does not match certificate")
		}
	case ed25519.PrivateKey:
		h = hash.NONE
	}
	return &Signer{ID: id, Sign: sign, Hash: h, NotAfter: cert.NotAfter}, nil
}

// parsePrivateKey accepts PKCS#8 (Node wallets) and SEC1 (fabric-ca-client msp dirs).
func parsePrivateKey(p []byte) (crypto.PrivateKey, error) {
	block, _ := pem.Decode(p)
	if block == nil {
		return nil, errors.New("invalid PEM")
	}
	if k, err := x509.ParsePKCS8PrivateKey(block.Bytes); err == nil {
		return k, nil
	}
	return x509.ParseECPrivateKey(block.Bytes)
}

// Timeouts for gateway calls.
type Timeouts struct {
	Evaluate, Endorse, Submit, CommitStatus time.Duration
}

// Connect opens a gateway session (cheap) on a pooled connection.
func Connect(conn grpc.ClientConnInterface, s *Signer, t Timeouts) (*client.Gateway, error) {
	return client.Connect(s.ID,
		client.WithSign(s.Sign),
		client.WithHash(s.Hash),
		client.WithClientConnection(conn),
		client.WithEvaluateTimeout(t.Evaluate),
		client.WithEndorseTimeout(t.Endorse),
		client.WithSubmitTimeout(t.Submit),
		client.WithCommitStatusTimeout(t.CommitStatus),
	)
}

// Retryable reports whether a gateway error is worth retrying: the peer or
// orderer was unreachable, overloaded or timed out. Chaincode errors,
// endorsement mismatches and permission errors are not retryable.
func Retryable(err error) bool {
	switch status.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded, codes.ResourceExhausted:
		return true
	}
	return false
}
