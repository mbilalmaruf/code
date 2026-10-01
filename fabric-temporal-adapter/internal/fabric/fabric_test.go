package fabric

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func certAndKey(t *testing.T, curve elliptic.Curve, sec1 bool) ([]byte, []byte, *ecdsa.PrivateKey) {
	t.Helper()
	key, _ := ecdsa.GenerateKey(curve, rand.Reader)
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "u"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour)}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	var kp []byte
	if sec1 {
		b, _ := x509.MarshalECPrivateKey(key)
		kp = pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: b})
	} else {
		b, _ := x509.MarshalPKCS8PrivateKey(key)
		kp = pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: b})
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), kp, key
}

func TestNewSigner(t *testing.T) {
	cert, key, _ := certAndKey(t, elliptic.P256(), false)
	s, err := NewSigner("Org1MSP", cert, key)
	if err != nil {
		t.Fatal(err)
	}
	if s.ID.MspID() != "Org1MSP" {
		t.Fatal("msp id")
	}
	if sig, err := s.Sign(s.Hash([]byte("hello"))); err != nil || len(sig) == 0 {
		t.Fatalf("sign: %v", err)
	}

	cert384, key384, _ := certAndKey(t, elliptic.P384(), true) // SEC1 key
	s384, err := NewSigner("Org1MSP", cert384, key384)
	if err != nil {
		t.Fatal(err)
	}
	if len(s384.Hash([]byte("x"))) != 48 {
		t.Fatal("P-384 identity must use SHA-384")
	}

	_, otherKey, _ := certAndKey(t, elliptic.P256(), false)
	if _, err := NewSigner("Org1MSP", cert, otherKey); err == nil {
		t.Fatal("mismatched key must be rejected")
	}
}

func TestRetryable(t *testing.T) {
	if !Retryable(status.Error(codes.Unavailable, "down")) {
		t.Error("Unavailable should retry")
	}
	if Retryable(status.Error(codes.Aborted, "endorsement mismatch")) {
		t.Error("Aborted should not retry")
	}
	if Retryable(errors.New("plain")) {
		t.Error("non-gRPC errors are not retryable here")
	}
}
