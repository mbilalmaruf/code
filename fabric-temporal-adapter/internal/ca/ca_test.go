package ca

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/asn1"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// fakeCA signs CSRs with a throwaway CA and verifies register tokens the way
// fabric-ca-server does.
type fakeCA struct {
	t          *testing.T
	key        *ecdsa.PrivateKey
	cert       *x509.Certificate
	registered map[string]string
}

func newFakeCA(t *testing.T) *fakeCA {
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test-ca"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true,
		KeyUsage: x509.KeyUsageCertSign, BasicConstraintsValid: true}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, _ := x509.ParseCertificate(der)
	return &fakeCA{t: t, key: key, cert: cert, registered: map[string]string{"admin": "adminpw"}}
}

func (f *fakeCA) fail(w http.ResponseWriter, status, code int, msg string) {
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"success": false, "result": nil,
		"errors": []map[string]any{{"code": code, "message": msg}}})
}

func (f *fakeCA) ok(w http.ResponseWriter, result any) {
	_ = json.NewEncoder(w).Encode(map[string]any{"success": true, "result": result})
}

func (f *fakeCA) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	switch r.URL.Path {
	case "/api/v1/enroll":
		user, pass, _ := r.BasicAuth()
		if f.registered[user] == "" || f.registered[user] != pass {
			f.fail(w, 401, 20, "Authentication failure")
			return
		}
		var req struct {
			CSR    string `json:"certificate_request"`
			CAName string `json:"caname"`
		}
		_ = json.Unmarshal(body, &req)
		if req.CAName != "ca-org1" {
			f.fail(w, 404, 19, "CA not found")
			return
		}
		block, _ := pem.Decode([]byte(req.CSR))
		csr, err := x509.ParseCertificateRequest(block.Bytes)
		if err != nil || csr.CheckSignature() != nil || csr.Subject.CommonName != user {
			f.fail(w, 400, 0, "bad csr")
			return
		}
		tmpl := &x509.Certificate{SerialNumber: big.NewInt(time.Now().UnixNano()), Subject: csr.Subject,
			NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature}
		der, _ := x509.CreateCertificate(rand.Reader, tmpl, f.cert, csr.PublicKey, f.key)
		certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
		f.ok(w, map[string]any{"Cert": base64.StdEncoding.EncodeToString(certPEM)})
	case "/api/v1/register":
		if err := verifyToken(r.Header.Get("Authorization"), r.Method, r.URL.RequestURI(), body); err != "" {
			f.fail(w, 401, 20, err)
			return
		}
		var req RegisterRequest
		_ = json.Unmarshal(body, &req)
		if _, exists := f.registered[req.ID]; exists {
			f.fail(w, 400, 74, "Identity '"+req.ID+"' is already registered")
			return
		}
		f.registered[req.ID] = "generated-secret"
		f.ok(w, map[string]any{"secret": "generated-secret"})
	default:
		http.NotFound(w, r)
	}
}

func verifyToken(tok, method, uri string, body []byte) string {
	parts := strings.Split(tok, ".")
	if len(parts) != 2 {
		return "malformed token"
	}
	certPEM, _ := base64.StdEncoding.DecodeString(parts[0])
	sig, _ := base64.StdEncoding.DecodeString(parts[1])
	block, _ := pem.Decode(certPEM)
	if block == nil {
		return "bad token cert"
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return "bad token cert"
	}
	b64 := base64.StdEncoding.EncodeToString
	payload := method + "." + b64([]byte(uri)) + "." + b64(body) + "." + parts[0]
	digest := sha256.Sum256([]byte(payload))
	var rs struct{ R, S *big.Int }
	if _, err := asn1.Unmarshal(sig, &rs); err != nil {
		return "bad signature encoding"
	}
	pub := cert.PublicKey.(*ecdsa.PublicKey)
	if rs.S.Cmp(new(big.Int).Rsh(pub.Curve.Params().N, 1)) > 0 {
		return "signature is not low-S"
	}
	if !ecdsa.Verify(pub, digest[:], rs.R, rs.S) {
		return "signature verification failed"
	}
	return ""
}

func TestEnrollRegisterFlow(t *testing.T) {
	srv := httptest.NewServer(newFakeCA(t))
	defer srv.Close()
	c, err := New(srv.URL, "ca-org1", nil, true, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	if _, err := c.Enroll(ctx, "admin", "wrong"); err == nil {
		t.Fatal("expected auth failure")
	} else if ce, ok := err.(*Error); !ok || ce.HTTPStatus != 401 || ce.Transient() {
		t.Fatalf("want non-transient 401 Error, got %v", err)
	}

	admin, err := c.Enroll(ctx, "admin", "adminpw")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(admin.PrivateKeyPEM), "BEGIN PRIVATE KEY") {
		t.Fatal("private key must be PKCS#8 for Node wallet compatibility")
	}

	for i := 0; i < 20; i++ { // exercise both S halves for the low-S check
		secret, err := c.Register(ctx, admin, RegisterRequest{ID: "user" + string(rune('a'+i)), Affiliation: ""})
		if err != nil {
			t.Fatalf("register: %v", err)
		}
		if secret != "generated-secret" {
			t.Fatalf("secret = %q", secret)
		}
	}
	_, err = c.Register(ctx, admin, RegisterRequest{ID: "usera"})
	ce, ok := err.(*Error)
	if !ok || !ce.AlreadyRegistered() {
		t.Fatalf("want AlreadyRegistered, got %v", err)
	}

	user, err := c.Enroll(ctx, "usera", "generated-secret")
	if err != nil {
		t.Fatal(err)
	}
	block, _ := pem.Decode(user.CertificatePEM)
	cert, _ := x509.ParseCertificate(block.Bytes)
	if cert.Subject.CommonName != "usera" {
		t.Fatalf("CN = %q", cert.Subject.CommonName)
	}
}
