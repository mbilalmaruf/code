// Package profile parses a Fabric common connection profile (the JSON format
// used by the Node/Java/Go SDKs) and extracts what the adapter needs: the
// client org's MSP ID, its peers (gateway endpoints) and its CA.
package profile

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
)

type Profile struct {
	Name   string `json:"name"`
	Client struct {
		Organization string `json:"organization"`
	} `json:"client"`
	Organizations          map[string]Organization `json:"organizations"`
	Peers                  map[string]Peer         `json:"peers"`
	CertificateAuthorities map[string]CA           `json:"certificateAuthorities"`
}

type Organization struct {
	MSPID                  string   `json:"mspid"`
	Peers                  []string `json:"peers"`
	CertificateAuthorities []string `json:"certificateAuthorities"`
}

type Peer struct {
	URL         string         `json:"url"`
	TLSCACerts  TLSCerts       `json:"tlsCACerts"`
	GRPCOptions map[string]any `json:"grpcOptions"`
}

type CA struct {
	URL         string         `json:"url"`
	CAName      string         `json:"caName"`
	TLSCACerts  TLSCerts       `json:"tlsCACerts"`
	HTTPOptions map[string]any `json:"httpOptions"`
	Registrar   []Registrar    `json:"registrar"`
}

type Registrar struct {
	EnrollID     string `json:"enrollId"`
	EnrollSecret string `json:"enrollSecret"`
}

// TLSCerts accepts {"pem": "..."}, {"pem": ["...", "..."]} or {"path": "..."}.
type TLSCerts struct {
	PEM  []string
	Path string
}

func (t *TLSCerts) UnmarshalJSON(b []byte) error {
	var raw struct {
		PEM  json.RawMessage `json:"pem"`
		Path string          `json:"path"`
	}
	if err := json.Unmarshal(b, &raw); err != nil {
		return err
	}
	t.Path = raw.Path
	p := bytes.TrimSpace(raw.PEM)
	switch {
	case len(p) == 0 || bytes.Equal(p, []byte("null")):
	case p[0] == '[':
		return json.Unmarshal(p, &t.PEM)
	default:
		var s string
		if err := json.Unmarshal(p, &s); err != nil {
			return err
		}
		t.PEM = []string{s}
	}
	return nil
}

// Bytes returns the concatenated PEM (reading Path on the worker if set).
func (t TLSCerts) Bytes() ([]byte, error) {
	if len(t.PEM) > 0 {
		return []byte(strings.Join(t.PEM, "\n")), nil
	}
	if t.Path != "" {
		return os.ReadFile(t.Path)
	}
	return nil, nil
}

// Parse decodes and minimally validates a profile.
func Parse(raw []byte) (*Profile, error) {
	if len(bytes.TrimSpace(raw)) == 0 {
		return nil, errors.New("connection profile is empty")
	}
	var p Profile
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, fmt.Errorf("connection profile is not valid JSON: %w", err)
	}
	if len(p.Organizations) == 0 {
		return nil, errors.New("connection profile has no organizations")
	}
	return &p, nil
}

// Endpoint is a resolved peer gateway endpoint.
type Endpoint struct {
	Name               string
	Address            string // host:port, no scheme
	TLS                bool
	TLSCACertPEM       []byte
	ServerNameOverride string
}

// Org is everything the adapter needs about one organization.
type Org struct {
	Name      string
	MSPID     string
	Endpoints []Endpoint
	CA        *ResolvedCA
}

type ResolvedCA struct {
	Key          string // key in certificateAuthorities
	URL          string
	CAName       string
	TLSCACertPEM []byte
	Verify       bool
	Registrar    *Registrar
}

// ResolveOrg returns org (or client.organization when org is empty).
func (p *Profile) ResolveOrg(org string) (*Org, error) {
	if org == "" {
		org = p.Client.Organization
	}
	if org == "" {
		if len(p.Organizations) != 1 {
			return nil, errors.New("organization not given and profile has no client.organization")
		}
		for k := range p.Organizations {
			org = k
		}
	}
	o, ok := p.Organizations[org]
	if !ok {
		return nil, fmt.Errorf("organization %q not in connection profile", org)
	}
	if o.MSPID == "" {
		return nil, fmt.Errorf("organization %q has no mspid", org)
	}
	out := &Org{Name: org, MSPID: o.MSPID}

	peerNames := o.Peers
	if len(peerNames) == 0 { // fall back to every peer in the profile
		for k := range p.Peers {
			peerNames = append(peerNames, k)
		}
		sort.Strings(peerNames)
	}
	for _, name := range peerNames {
		pe, ok := p.Peers[name]
		if !ok {
			return nil, fmt.Errorf("peer %q listed for %s but not defined", name, org)
		}
		ep, err := resolvePeer(name, pe)
		if err != nil {
			return nil, err
		}
		out.Endpoints = append(out.Endpoints, ep)
	}
	if len(out.Endpoints) == 0 {
		return nil, fmt.Errorf("organization %q has no peers", org)
	}

	if len(o.CertificateAuthorities) > 0 {
		key := o.CertificateAuthorities[0]
		ca, ok := p.CertificateAuthorities[key]
		if !ok {
			return nil, fmt.Errorf("certificate authority %q listed for %s but not defined", key, org)
		}
		pem, err := ca.TLSCACerts.Bytes()
		if err != nil {
			return nil, fmt.Errorf("CA %s tlsCACerts: %w", key, err)
		}
		rc := &ResolvedCA{Key: key, URL: strings.TrimRight(ca.URL, "/"), CAName: ca.CAName, TLSCACertPEM: pem, Verify: true}
		if v, ok := ca.HTTPOptions["verify"].(bool); ok {
			rc.Verify = v
		}
		if len(ca.Registrar) > 0 {
			r := ca.Registrar[0]
			rc.Registrar = &r
		}
		out.CA = rc
	}
	return out, nil
}

func resolvePeer(name string, pe Peer) (Endpoint, error) {
	ep := Endpoint{Name: name}
	u := pe.URL
	switch {
	case strings.HasPrefix(u, "grpcs://"):
		ep.TLS, u = true, strings.TrimPrefix(u, "grpcs://")
	case strings.HasPrefix(u, "grpc://"):
		u = strings.TrimPrefix(u, "grpc://")
	default:
		return ep, fmt.Errorf("peer %q url %q must start with grpcs:// or grpc://", name, pe.URL)
	}
	ep.Address = u
	if ep.TLS {
		pem, err := pe.TLSCACerts.Bytes()
		if err != nil {
			return ep, fmt.Errorf("peer %q tlsCACerts: %w", name, err)
		}
		if len(pem) == 0 {
			return ep, fmt.Errorf("peer %q uses grpcs but has no tlsCACerts", name)
		}
		ep.TLSCACertPEM = pem
	}
	for _, k := range []string{"ssl-target-name-override", "hostnameOverride"} {
		if s, ok := pe.GRPCOptions[k].(string); ok && s != "" {
			ep.ServerNameOverride = s
			break
		}
	}
	return ep, nil
}
