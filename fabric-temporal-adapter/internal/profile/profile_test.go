package profile

import "testing"

const sample = `{
  "name": "test-network-org1",
  "client": {"organization": "Org1"},
  "organizations": {
    "Org1": {"mspid": "Org1MSP", "peers": ["peer0.org1", "peer1.org1"], "certificateAuthorities": ["ca.org1"]}
  },
  "peers": {
    "peer0.org1": {"url": "grpcs://localhost:7051", "tlsCACerts": {"pem": "PEM0"},
                   "grpcOptions": {"ssl-target-name-override": "peer0.org1.example.com"}},
    "peer1.org1": {"url": "grpcs://localhost:8051", "tlsCACerts": {"pem": ["PEM1a", "PEM1b"]},
                   "grpcOptions": {"hostnameOverride": "peer1.org1.example.com"}}
  },
  "certificateAuthorities": {
    "ca.org1": {"url": "https://localhost:7054/", "caName": "ca-org1", "tlsCACerts": {"pem": ["CAPEM"]},
                "httpOptions": {"verify": false}, "registrar": [{"enrollId": "admin", "enrollSecret": "adminpw"}]}
  }
}`

func TestResolveOrg(t *testing.T) {
	p, err := Parse([]byte(sample))
	if err != nil {
		t.Fatal(err)
	}
	o, err := p.ResolveOrg("")
	if err != nil {
		t.Fatal(err)
	}
	if o.MSPID != "Org1MSP" || len(o.Endpoints) != 2 {
		t.Fatalf("org = %+v", o)
	}
	e0, e1 := o.Endpoints[0], o.Endpoints[1]
	if e0.Address != "localhost:7051" || !e0.TLS || string(e0.TLSCACertPEM) != "PEM0" || e0.ServerNameOverride != "peer0.org1.example.com" {
		t.Errorf("peer0 = %+v", e0)
	}
	if string(e1.TLSCACertPEM) != "PEM1a\nPEM1b" || e1.ServerNameOverride != "peer1.org1.example.com" {
		t.Errorf("peer1 = %+v", e1)
	}
	if o.CA == nil || o.CA.URL != "https://localhost:7054" || o.CA.CAName != "ca-org1" || o.CA.Verify || o.CA.Registrar.EnrollID != "admin" {
		t.Errorf("ca = %+v", o.CA)
	}
	if _, err := p.ResolveOrg("Org9"); err == nil {
		t.Error("unknown org should fail")
	}
}

func TestBadProfiles(t *testing.T) {
	for name, raw := range map[string]string{
		"empty":    ``,
		"no orgs":  `{"organizations":{}}`,
		"not json": `{`,
	} {
		if _, err := Parse([]byte(raw)); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
	p, _ := Parse([]byte(`{"client":{"organization":"O"},"organizations":{"O":{"mspid":"M","peers":["p"]}},
		"peers":{"p":{"url":"grpcs://h:1"}}}`))
	if _, err := p.ResolveOrg(""); err == nil {
		t.Error("grpcs peer without tlsCACerts should fail")
	}
}
