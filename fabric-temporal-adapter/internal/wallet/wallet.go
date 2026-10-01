// Package wallet stores Fabric identities in CouchDB using the same document
// format as the Node fabric-network CouchDBWalletStore, so identities enrolled
// by legacy Node services are reused and vice versa:
//
//	{ "_id": "<label>", "data": "<identity JSON string>" }
//
// where the identity JSON is
//
//	{"credentials":{"certificate":"<PEM>","privateKey":"<PEM>"},"mspId":"Org1MSP","type":"X.509","version":1}
package wallet

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Identity is a stored X.509 identity.
type Identity struct {
	Credentials struct {
		Certificate string `json:"certificate"`
		PrivateKey  string `json:"privateKey"`
	} `json:"credentials"`
	MSPID   string `json:"mspId"`
	Type    string `json:"type"`
	Version int    `json:"version"`
}

func NewIdentity(mspID string, certPEM, keyPEM []byte) *Identity {
	id := &Identity{MSPID: mspID, Type: "X.509", Version: 1}
	id.Credentials.Certificate = string(certPEM)
	id.Credentials.PrivateKey = string(keyPEM)
	return id
}

// Store is a CouchDB-backed wallet.
type Store struct {
	base       *url.URL
	db         string
	user, pass string
	http       *http.Client
}

// ErrConflict means another writer stored the label first.
var ErrConflict = errors.New("wallet: identity already exists (conflict)")

func New(rawURL, user, pass, db string, timeout time.Duration, insecureTLS bool) (*Store, error) {
	u, err := url.Parse(strings.TrimRight(rawURL, "/"))
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") {
		return nil, errors.New("wallet: invalid couchdb url")
	}
	if u.User != nil {
		if user == "" {
			user = u.User.Username()
		}
		if p, ok := u.User.Password(); ok && pass == "" {
			pass = p
		}
		u.User = nil
	}
	tr := http.DefaultTransport.(*http.Transport).Clone()
	if insecureTLS {
		tr.TLSClientConfig = &tls.Config{InsecureSkipVerify: true} //nolint:gosec // opt-in
	}
	return &Store{base: u, db: db, user: user, pass: pass, http: &http.Client{Transport: tr, Timeout: timeout}}, nil
}

func (s *Store) req(ctx context.Context, method, escPath string, body []byte) (*http.Response, error) {
	u := *s.base
	p, _ := url.PathUnescape(escPath)
	u.Path = s.base.Path + p
	u.RawPath = s.base.EscapedPath() + escPath
	var r io.Reader
	if body != nil {
		r = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, u.String(), r)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if s.user != "" || s.pass != "" {
		req.SetBasicAuth(s.user, s.pass)
	}
	return s.http.Do(req)
}

func (s *Store) docPath(label string) string {
	return "/" + url.PathEscape(s.db) + "/" + url.PathEscape(label)
}

// HTTPError is a non-success CouchDB response.
type HTTPError struct {
	Code int
	Body string
}

func (e *HTTPError) Error() string { return fmt.Sprintf("wallet couchdb: HTTP %d: %s", e.Code, e.Body) }

func httpErr(resp *http.Response) error {
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
	return &HTTPError{Code: resp.StatusCode, Body: strings.TrimSpace(string(b))}
}

// EnsureDB creates the wallet database if it does not exist.
func (s *Store) EnsureDB(ctx context.Context) error {
	resp, err := s.req(ctx, http.MethodPut, "/"+url.PathEscape(s.db), nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusCreated, http.StatusAccepted, http.StatusPreconditionFailed: // 412 = exists
		return nil
	}
	return httpErr(resp)
}

// Get returns the identity for label, or (nil, nil) if absent.
func (s *Store) Get(ctx context.Context, label string) (*Identity, error) {
	resp, err := s.req(ctx, http.MethodGet, s.docPath(label), nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil, nil
	}
	if resp.StatusCode != http.StatusOK {
		return nil, httpErr(resp)
	}
	var doc struct {
		Data json.RawMessage `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&doc); err != nil {
		return nil, fmt.Errorf("wallet doc %q: %w", label, err)
	}
	// Node stores data as a JSON string; accept an embedded object too.
	data := bytes.TrimSpace(doc.Data)
	if len(data) > 0 && data[0] == '"' {
		var s string
		if err := json.Unmarshal(data, &s); err != nil {
			return nil, err
		}
		data = []byte(s)
	}
	var id Identity
	if err := json.Unmarshal(data, &id); err != nil {
		return nil, fmt.Errorf("wallet doc %q: invalid identity: %w", label, err)
	}
	if id.Credentials.Certificate == "" || id.Credentials.PrivateKey == "" {
		return nil, fmt.Errorf("wallet doc %q: missing credentials", label)
	}
	return &id, nil
}

// Create stores a new identity; ErrConflict if label already exists.
func (s *Store) Create(ctx context.Context, label string, id *Identity) error {
	data, err := json.Marshal(id)
	if err != nil {
		return err
	}
	body, _ := json.Marshal(map[string]string{"data": string(data)})
	resp, err := s.req(ctx, http.MethodPut, s.docPath(label), body)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusCreated, http.StatusAccepted:
		return nil
	case http.StatusConflict:
		return ErrConflict
	}
	return httpErr(resp)
}
