// Package couch is a minimal CouchDB client: server ping, database listing
// and a continuous _changes feed reader.
package couch

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type Client struct {
	base       *url.URL
	user, pass string
	http       *http.Client // bounded requests
	stream     *http.Client // long-lived feed, no overall timeout
}

// New builds a client. Credentials embedded in rawURL are moved to basic
// auth; explicit user/pass take precedence.
func New(rawURL, user, pass string, timeout time.Duration, insecureTLS bool) (*Client, error) {
	u, err := url.Parse(strings.TrimRight(rawURL, "/"))
	if err != nil {
		return nil, errors.New("invalid couchdb url") // don't echo: may contain credentials
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, fmt.Errorf("couchdb url scheme must be http or https, got %q", u.Scheme)
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
	tr.ResponseHeaderTimeout = timeout
	if insecureTLS {
		tr.TLSClientConfig = &tls.Config{InsecureSkipVerify: true} //nolint:gosec // opt-in via config
	}
	return &Client{
		base:   u,
		user:   user,
		pass:   pass,
		http:   &http.Client{Transport: tr, Timeout: timeout},
		stream: &http.Client{Transport: tr},
	}, nil
}

// Redacted returns the server URL without credentials, for logs.
func (c *Client) Redacted() string { return c.base.String() }

// newReq builds a GET for escapedPath (already percent-encoded, see dbPath).
func (c *Client) newReq(ctx context.Context, escapedPath string, q url.Values) (*http.Request, error) {
	p, err := url.PathUnescape(escapedPath)
	if err != nil {
		return nil, err
	}
	u := *c.base
	u.Path = c.base.Path + p
	u.RawPath = c.base.EscapedPath() + escapedPath
	u.RawQuery = q.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	if c.user != "" || c.pass != "" {
		req.SetBasicAuth(c.user, c.pass)
	}
	return req, nil
}

// dbPath escapes a database name as one path segment ('/' becomes %2F,
// Fabric's '$' is left as is).
func dbPath(db string) string { return "/" + url.PathEscape(db) }

func (c *Client) getJSON(ctx context.Context, path string, q url.Values, out any) error {
	req, err := c.newReq(ctx, path, q)
	if err != nil {
		return err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return statusError(resp)
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

// StatusError is a non-200 CouchDB response.
type StatusError struct {
	Code   int
	Reason string
}

func (e *StatusError) Error() string { return fmt.Sprintf("couchdb: HTTP %d: %s", e.Code, e.Reason) }

// Transient reports whether retrying could help (5xx, 429).
func (e *StatusError) Transient() bool { return e.Code >= 500 || e.Code == http.StatusTooManyRequests }

func statusError(resp *http.Response) error {
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	var body struct {
		Error  string `json:"error"`
		Reason string `json:"reason"`
	}
	reason := strings.TrimSpace(string(b))
	if json.Unmarshal(b, &body) == nil && body.Error != "" {
		reason = body.Error + ": " + body.Reason
	}
	return &StatusError{Code: resp.StatusCode, Reason: reason}
}

// Ping checks connectivity and credentials.
func (c *Client) Ping(ctx context.Context) error {
	var v map[string]any
	if err := c.getJSON(ctx, "/", nil, &v); err != nil {
		return err
	}
	// Credentials are only exercised by a db-level call; _all_dbs needs auth
	// on secured servers.
	var dbs []string
	return c.getJSON(ctx, "/_all_dbs", url.Values{"limit": {"1"}}, &dbs)
}

// AllDBs lists every database on the server.
func (c *Client) AllDBs(ctx context.Context) ([]string, error) {
	var dbs []string
	err := c.getJSON(ctx, "/_all_dbs", nil, &dbs)
	return dbs, err
}

// DBInfo returns a database's current update_seq; ok=false if it does not exist.
func (c *Client) UpdateSeq(ctx context.Context, db string) (seq string, ok bool, err error) {
	var info struct {
		UpdateSeq json.RawMessage `json:"update_seq"`
	}
	err = c.getJSON(ctx, dbPath(db), nil, &info)
	var se *StatusError
	if errors.As(err, &se) && se.Code == http.StatusNotFound {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return seqString(info.UpdateSeq), true, nil
}

// Change is one row of the _changes feed.
type Change struct {
	Seq     string
	ID      string
	Rev     string
	Deleted bool
	Doc     json.RawMessage
}

type rawChange struct {
	Seq     json.RawMessage `json:"seq"`
	ID      string          `json:"id"`
	Deleted bool            `json:"deleted"`
	Changes []struct {
		Rev string `json:"rev"`
	} `json:"changes"`
	Doc     json.RawMessage `json:"doc"`
	LastSeq json.RawMessage `json:"last_seq"`
	Error   string          `json:"error"`
	Reason  string          `json:"reason"`
}

// seqString normalises CouchDB 1.x numeric and 2.x+ string sequences.
func seqString(raw json.RawMessage) string {
	raw = bytes.TrimSpace(raw)
	if len(raw) > 0 && raw[0] == '"' {
		var s string
		if json.Unmarshal(raw, &s) == nil {
			return s
		}
	}
	return string(raw)
}

// ErrStalled is returned when the feed produced nothing (not even a
// heartbeat) for 3x the heartbeat interval.
var ErrStalled = errors.New("couchdb changes feed stalled (no heartbeat)")

// Changes follows db's continuous feed from since, calling fn for every
// change in order. It returns the last sequence seen when the feed ends
// (server closed it, ctx cancelled, stall, or fn error).
func (c *Client) Changes(ctx context.Context, db, since string, heartbeat time.Duration, fn func(Change) error) (string, error) {
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	q := url.Values{
		"feed":         {"continuous"},
		"include_docs": {"true"},
		"heartbeat":    {strconv.FormatInt(heartbeat.Milliseconds(), 10)},
		"since":        {since},
	}
	req, err := c.newReq(ctx, dbPath(db)+"/_changes", q)
	if err != nil {
		return since, err
	}
	resp, err := c.stream.Do(req)
	if err != nil {
		return since, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return since, statusError(resp)
	}

	watchdog := time.AfterFunc(3*heartbeat, func() { cancel(ErrStalled) })
	defer watchdog.Stop()

	last := since
	r := bufio.NewReaderSize(resp.Body, 1<<20)
	for {
		line, err := r.ReadBytes('\n')
		watchdog.Reset(3 * heartbeat)
		if len(bytes.TrimSpace(line)) > 0 {
			var rc rawChange
			if jerr := json.Unmarshal(line, &rc); jerr != nil {
				return last, fmt.Errorf("decode change: %w", jerr)
			}
			switch {
			case rc.Error != "":
				return last, fmt.Errorf("couchdb changes error: %s: %s", rc.Error, rc.Reason)
			case len(rc.LastSeq) > 0:
				return seqString(rc.LastSeq), nil
			case rc.ID != "":
				ch := Change{Seq: seqString(rc.Seq), ID: rc.ID, Deleted: rc.Deleted, Doc: rc.Doc}
				if len(rc.Changes) > 0 {
					ch.Rev = rc.Changes[0].Rev
				}
				// fn may block on backpressure; that is not a stalled feed.
				watchdog.Stop()
				ferr := fn(ch)
				watchdog.Reset(3 * heartbeat)
				if ferr != nil {
					return last, ferr
				}
				last = ch.Seq
			}
		}
		if err != nil {
			if cause := context.Cause(ctx); cause != nil && cause != context.Canceled {
				return last, cause
			}
			if ctx.Err() != nil {
				return last, ctx.Err()
			}
			if errors.Is(err, io.EOF) {
				return last, io.ErrUnexpectedEOF // feed closed without last_seq
			}
			return last, err
		}
	}
}
