package wallet

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeCouch keeps docs in memory: PUT /db, GET/PUT /db/doc.
type fakeCouch struct {
	mu   sync.Mutex
	dbs  map[string]bool
	docs map[string][]byte
}

func (f *fakeCouch) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if u, p, _ := r.BasicAuth(); u != "admin" || p != "pw" {
		w.WriteHeader(401)
		return
	}
	parts := strings.SplitN(strings.TrimPrefix(r.URL.EscapedPath(), "/"), "/", 2)
	switch {
	case len(parts) == 1 && r.Method == http.MethodPut:
		if f.dbs[parts[0]] {
			w.WriteHeader(412)
			return
		}
		f.dbs[parts[0]] = true
		w.WriteHeader(201)
	case len(parts) == 2 && r.Method == http.MethodGet:
		d, ok := f.docs[r.URL.Path]
		if !ok {
			w.WriteHeader(404)
			return
		}
		_, _ = w.Write(d)
	case len(parts) == 2 && r.Method == http.MethodPut:
		if _, ok := f.docs[r.URL.Path]; ok {
			w.WriteHeader(409)
			return
		}
		b, _ := io.ReadAll(r.Body)
		f.docs[r.URL.Path] = b
		w.WriteHeader(201)
	default:
		w.WriteHeader(400)
	}
}

func TestWalletRoundTripAndNodeFormat(t *testing.T) {
	fc := &fakeCouch{dbs: map[string]bool{}, docs: map[string][]byte{}}
	srv := httptest.NewServer(fc)
	defer srv.Close()
	s, err := New(strings.Replace(srv.URL, "http://", "http://admin:pw@", 1), "", "", "wallet", 5*time.Second, false)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := s.EnsureDB(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.EnsureDB(ctx); err != nil { // already exists
		t.Fatal(err)
	}
	if id, err := s.Get(ctx, "alice"); err != nil || id != nil {
		t.Fatalf("missing label: id=%v err=%v", id, err)
	}
	if err := s.Create(ctx, "alice", NewIdentity("Org1MSP", []byte("CERT"), []byte("KEY"))); err != nil {
		t.Fatal(err)
	}
	if err := s.Create(ctx, "alice", NewIdentity("Org1MSP", []byte("C2"), []byte("K2"))); !errors.Is(err, ErrConflict) {
		t.Fatalf("want ErrConflict, got %v", err)
	}

	// The stored doc must match Node fabric-network: data is a JSON *string*.
	var doc map[string]any
	_ = json.Unmarshal(fc.docs["/wallet/alice"], &doc)
	data, ok := doc["data"].(string)
	if !ok || !strings.Contains(data, `"type":"X.509"`) || !strings.Contains(data, `"mspId":"Org1MSP"`) {
		t.Fatalf("doc not in Node wallet format: %s", fc.docs["/wallet/alice"])
	}

	// A doc written by Node (with _rev) is readable.
	fc.docs["/wallet/bob"] = []byte(`{"_id":"bob","_rev":"1-x","data":"{\"credentials\":{\"certificate\":\"C\",\"privateKey\":\"K\"},\"mspId\":\"Org1MSP\",\"type\":\"X.509\",\"version\":1}"}`)
	id, err := s.Get(ctx, "bob")
	if err != nil || id == nil || id.Credentials.Certificate != "C" || id.MSPID != "Org1MSP" {
		t.Fatalf("node doc: %+v %v", id, err)
	}
}
