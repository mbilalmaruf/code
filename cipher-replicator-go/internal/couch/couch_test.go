package couch

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestChangesFeed(t *testing.T) {
	var gotPath, gotSince, gotUser string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.EscapedPath()
		gotSince = r.URL.Query().Get("since")
		gotUser, _, _ = r.BasicAuth()
		fmt.Fprintln(w, `{"seq":"1-abc","id":"d1","changes":[{"rev":"1-x"}],"doc":{"_id":"d1","documentName":"claim"}}`)
		fmt.Fprintln(w) // heartbeat
		fmt.Fprintln(w, `{"seq":2,"id":"d2","deleted":true,"changes":[{"rev":"2-y"}],"doc":{"_id":"d2","_deleted":true}}`)
		fmt.Fprintln(w, `{"last_seq":"2-def","pending":0}`)
	}))
	defer srv.Close()

	u := strings.Replace(srv.URL, "http://", "http://bob:pw@", 1)
	c, err := New(u, "", "", 5*time.Second, false)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(c.Redacted(), "pw") {
		t.Fatal("credentials leaked into Redacted()")
	}
	var got []Change
	last, err := c.Changes(context.Background(), "mychannel_cc$$pcoll/x", "0", time.Second, func(ch Change) error {
		got = append(got, ch)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if last != "2-def" {
		t.Errorf("last = %q", last)
	}
	if gotPath != "/mychannel_cc$$pcoll%2Fx/_changes" {
		t.Errorf("path = %q", gotPath)
	}
	if gotSince != "0" || gotUser != "bob" {
		t.Errorf("since=%q user=%q", gotSince, gotUser)
	}
	if len(got) != 2 || got[0].Seq != "1-abc" || got[0].Rev != "1-x" || got[1].Seq != "2" || !got[1].Deleted {
		t.Fatalf("changes = %+v", got)
	}
}

func TestChangesStall(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		<-r.Context().Done() // never send anything
	}))
	defer srv.Close()
	c, _ := New(srv.URL, "", "", 5*time.Second, false)
	_, err := c.Changes(context.Background(), "db", "0", 50*time.Millisecond, func(Change) error { return nil })
	if !errors.Is(err, ErrStalled) {
		t.Fatalf("want ErrStalled, got %v", err)
	}
}

func TestStatusError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(404)
		fmt.Fprint(w, `{"error":"not_found","reason":"Database does not exist."}`)
	}))
	defer srv.Close()
	c, _ := New(srv.URL, "", "", 5*time.Second, false)
	_, ok, err := c.UpdateSeq(context.Background(), "nope")
	if err != nil || ok {
		t.Fatalf("UpdateSeq missing db: ok=%v err=%v", ok, err)
	}
	_, err = c.Changes(context.Background(), "nope", "0", time.Second, func(Change) error { return nil })
	var se *StatusError
	if !errors.As(err, &se) || se.Code != 404 || se.Transient() {
		t.Fatalf("want non-transient 404, got %v", err)
	}
}
