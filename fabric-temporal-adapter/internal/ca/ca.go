// Package ca is a minimal Fabric CA REST client: enroll and register.
// fabric-gateway has no CA client, and the old fabric-sdk-go is deprecated,
// so this talks to /api/v1/{enroll,register} directly.
package ca

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/asn1"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"strings"
	"time"
)

type Client struct {
	url    string
	caName string
	http   *http.Client
}

// New builds a client for baseURL (https://host:7054). tlsCAPEM may be empty
// for system roots; verify=false skips TLS verification (dev only).
func New(baseURL, caName string, tlsCAPEM []byte, verify bool, timeout time.Duration) (*Client, error) {
	tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12}
	if len(tlsCAPEM) > 0 {
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(tlsCAPEM) {
			return nil, errors.New("CA tlsCACerts contain no valid certificate")
		}
		tlsCfg.RootCAs = pool
	}
	if !verify {
		tlsCfg.InsecureSkipVerify = true //nolint:gosec // profile httpOptions.verify=false
	}
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.TLSClientConfig = tlsCfg
	return &Client{url: strings.TrimRight(baseURL, "/"), caName: caName, http: &http.Client{Transport: tr, Timeout: timeout}}, nil
}

// Credential is an enrolled identity.
type Credential struct {
	CertificatePEM []byte
	PrivateKeyPEM  []byte // PKCS#8
}

// Error is an error reported by the CA.
type Error struct {
	HTTPStatus int
	Code       int
	Message    string
}

func (e *Error) Error() string {
	return fmt.Sprintf("fabric-ca: HTTP %d: code %d: %s", e.HTTPStatus, e.Code, e.Message)
}

// AlreadyRegistered reports whether the CA rejected a register because the id exists.
func (e *Error) AlreadyRegistered() bool {
	return e.Code == 74 || strings.Contains(strings.ToLower(e.Message), "already registered")
}

// Transient reports whether retrying could help.
func (e *Error) Transient() bool { return e.HTTPStatus >= 500 || e.HTTPStatus == 429 }

type response struct {
	Success bool            `json:"success"`
	Result  json.RawMessage `json:"result"`
	Errors  []struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	} `json:"errors"`
}

func (c *Client) do(ctx context.Context, path string, body any, auth func(*http.Request, []byte) error, out any) error {
	b, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url+path, bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if err := auth(req, b); err != nil {
		return err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return err
	}
	var r response
	if jerr := json.Unmarshal(raw, &r); jerr != nil {
		return &Error{HTTPStatus: resp.StatusCode, Message: strings.TrimSpace(string(raw))}
	}
	if !r.Success || resp.StatusCode != http.StatusOK {
		e := &Error{HTTPStatus: resp.StatusCode, Message: "request failed"}
		if len(r.Errors) > 0 {
			e.Code, e.Message = r.Errors[0].Code, r.Errors[0].Message
		}
		return e
	}
	return json.Unmarshal(r.Result, out)
}

// Enroll generates a P-256 key and CSR (CN = enrollmentID) and enrolls it.
func (c *Client) Enroll(ctx context.Context, enrollmentID, secret string) (*Credential, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	csrDER, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{
		Subject: pkix.Name{CommonName: enrollmentID},
	}, key)
	if err != nil {
		return nil, err
	}
	body := map[string]any{
		"certificate_request": string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: csrDER})),
	}
	if c.caName != "" {
		body["caname"] = c.caName
	}
	var result struct {
		Cert string `json:"Cert"`
	}
	err = c.do(ctx, "/api/v1/enroll", body, func(r *http.Request, _ []byte) error {
		r.SetBasicAuth(enrollmentID, secret)
		return nil
	}, &result)
	if err != nil {
		return nil, err
	}
	certPEM, err := base64.StdEncoding.DecodeString(result.Cert)
	if err != nil {
		return nil, fmt.Errorf("decode enrolled certificate: %w", err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return nil, err
	}
	return &Credential{
		CertificatePEM: certPEM,
		PrivateKeyPEM:  pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}),
	}, nil
}

// RegisterRequest registers a new identity. An empty Secret lets the CA generate one.
type RegisterRequest struct {
	ID             string      `json:"id"`
	Type           string      `json:"type,omitempty"`
	Secret         string      `json:"secret,omitempty"`
	Affiliation    string      `json:"affiliation"`
	MaxEnrollments int         `json:"max_enrollments,omitempty"`
	Attributes     []Attribute `json:"attrs,omitempty"`
	CAName         string      `json:"caname,omitempty"`
}

type Attribute struct {
	Name  string `json:"name"`
	Value string `json:"value"`
	ECert bool   `json:"ecert,omitempty"`
}

// Register registers req using registrar's credential and returns the
// enrollment secret.
func (c *Client) Register(ctx context.Context, registrar *Credential, req RegisterRequest) (string, error) {
	if req.CAName == "" {
		req.CAName = c.caName
	}
	if req.Type == "" {
		req.Type = "client"
	}
	key, err := parsePrivateKey(registrar.PrivateKeyPEM)
	if err != nil {
		return "", fmt.Errorf("registrar key: %w", err)
	}
	var result struct {
		Secret string `json:"secret"`
	}
	err = c.do(ctx, "/api/v1/register", req, func(r *http.Request, body []byte) error {
		tok, err := Token(registrar.CertificatePEM, key, r.Method, r.URL.RequestURI(), body)
		if err != nil {
			return err
		}
		r.Header.Set("Authorization", tok)
		return nil
	}, &result)
	return result.Secret, err
}

// Token builds the Fabric CA authorization token:
// b64(cert) "." b64(sig), sig = ECDSA-SHA256 over
// method "." b64(uri) "." b64(body) "." b64(cert), low-S normalised.
func Token(certPEM []byte, key *ecdsa.PrivateKey, method, uri string, body []byte) (string, error) {
	b64 := base64.StdEncoding.EncodeToString
	b64cert := b64(certPEM)
	payload := method + "." + b64([]byte(uri)) + "." + b64(body) + "." + b64cert
	digest := sha256.Sum256([]byte(payload))
	r, s, err := ecdsa.Sign(rand.Reader, key, digest[:])
	if err != nil {
		return "", err
	}
	s = toLowS(key.Curve, s)
	sig, err := asn1.Marshal(struct{ R, S *big.Int }{r, s})
	if err != nil {
		return "", err
	}
	return b64cert + "." + b64(sig), nil
}

func toLowS(curve elliptic.Curve, s *big.Int) *big.Int {
	n := curve.Params().N
	half := new(big.Int).Rsh(n, 1)
	if s.Cmp(half) > 0 {
		return new(big.Int).Sub(n, s)
	}
	return s
}

// parsePrivateKey accepts PKCS#8 ("PRIVATE KEY") and SEC1 ("EC PRIVATE KEY").
func parsePrivateKey(p []byte) (*ecdsa.PrivateKey, error) {
	block, _ := pem.Decode(p)
	if block == nil {
		return nil, errors.New("invalid private key PEM")
	}
	if k, err := x509.ParsePKCS8PrivateKey(block.Bytes); err == nil {
		ek, ok := k.(*ecdsa.PrivateKey)
		if !ok {
			return nil, fmt.Errorf("unsupported key type %T", k)
		}
		return ek, nil
	}
	return x509.ParseECPrivateKey(block.Bytes)
}
