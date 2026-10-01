// Package config is the worker configuration (JSON file).
package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"fabric-adapter/internal/secret"
)

// EnvEncryptionKey overrides Config.EncryptionKey.
const EnvEncryptionKey = "ADAPTER_ENCRYPTION_KEY"

type Config struct {
	// EncryptionKey decrypts secret fields stored as legacy encrypted blobs.
	EncryptionKey string         `json:"encryptionKey"`
	Temporal      TemporalConfig `json:"temporal"`
	Wallet        WalletConfig   `json:"wallet"`
	// Registrars register users that are neither in the wallet nor given an
	// enrollmentSecret. Matched to the profile's CA by key, caName or URL.
	Registrars []RegistrarConfig `json:"registrars"`
	// AllowProfileRegistrar falls back to certificateAuthorities[].registrar
	// in the request's connection profile. Off by default: it means admin
	// secrets travel in workflow inputs.
	AllowProfileRegistrar bool          `json:"allowProfileRegistrar"`
	Timeouts              TimeoutConfig `json:"timeouts"`
	Log                   LogConfig     `json:"log"`
}

type TemporalConfig struct {
	HostPort  string     `json:"hostPort"`
	Namespace string     `json:"namespace"`
	TaskQueue string     `json:"taskQueue"`
	TLS       *TLSConfig `json:"tls"`
	// PayloadEncryptionKey enables AES-GCM encryption of workflow/activity
	// payloads in Temporal history. Every client starting the workflow must
	// use the same codec (see internal/codec).
	PayloadEncryptionKey    secret.Value `json:"payloadEncryptionKey"`
	MaxConcurrentActivities int          `json:"maxConcurrentActivities"`
}

type TLSConfig struct {
	CAFile     string `json:"caFile"`
	CertFile   string `json:"certFile"`
	KeyFile    string `json:"keyFile"`
	ServerName string `json:"serverName"`
}

type WalletConfig struct {
	URL         secret.Value `json:"url"`
	Username    secret.Value `json:"username"`
	Password    secret.Value `json:"password"`
	Database    string       `json:"database"`
	InsecureTLS bool         `json:"insecureSkipVerify"`
	// LabelFormat builds the wallet label from {user} and {mspId}. Default
	// "{user}" matches Node fabric-network wallets.
	LabelFormat string `json:"labelFormat"`
}

type RegistrarConfig struct {
	CA           string       `json:"ca"`
	EnrollID     string       `json:"enrollId"`
	EnrollSecret secret.Value `json:"enrollSecret"`
	// WalletLabel caches the registrar identity in the wallet. Default "admin".
	WalletLabel string `json:"walletLabel"`
	Affiliation string `json:"affiliation"`
	// IdentityType for registered users. Default "client".
	IdentityType string `json:"identityType"`
}

type TimeoutConfig struct {
	CAMs           int `json:"caMs"`
	WalletMs       int `json:"walletMs"`
	EvaluateMs     int `json:"evaluateMs"`
	EndorseMs      int `json:"endorseMs"`
	SubmitMs       int `json:"submitMs"`
	CommitStatusMs int `json:"commitStatusMs"`
}

type LogConfig struct {
	Level  string `json:"level"`
	Format string `json:"format"`
}

func ms(n int) time.Duration { return time.Duration(n) * time.Millisecond }

func (t TimeoutConfig) CA() time.Duration           { return ms(t.CAMs) }
func (t TimeoutConfig) Wallet() time.Duration       { return ms(t.WalletMs) }
func (t TimeoutConfig) Evaluate() time.Duration     { return ms(t.EvaluateMs) }
func (t TimeoutConfig) Endorse() time.Duration      { return ms(t.EndorseMs) }
func (t TimeoutConfig) Submit() time.Duration       { return ms(t.SubmitMs) }
func (t TimeoutConfig) CommitStatus() time.Duration { return ms(t.CommitStatusMs) }

// Load reads, defaults, resolves secrets and validates.
func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	var c Config
	if err := json.Unmarshal(raw, &c); err != nil {
		return nil, fmt.Errorf("parse config %s: %w", path, err)
	}
	if k := os.Getenv(EnvEncryptionKey); k != "" {
		c.EncryptionKey = k
	}
	c.defaults()
	if err := c.resolve(); err != nil {
		return nil, err
	}
	return &c, c.validate()
}

func (c *Config) defaults() {
	t := &c.Temporal
	if t.HostPort == "" {
		t.HostPort = "localhost:7233"
	}
	if t.Namespace == "" {
		t.Namespace = "default"
	}
	if t.TaskQueue == "" {
		t.TaskQueue = "fabric-adapter"
	}
	if c.Wallet.Database == "" {
		c.Wallet.Database = "wallet"
	}
	if c.Wallet.LabelFormat == "" {
		c.Wallet.LabelFormat = "{user}"
	}
	for i := range c.Registrars {
		r := &c.Registrars[i]
		if r.WalletLabel == "" {
			r.WalletLabel = "admin"
		}
		if r.IdentityType == "" {
			r.IdentityType = "client"
		}
	}
	d := &c.Timeouts
	set := func(p *int, v int) {
		if *p == 0 {
			*p = v
		}
	}
	set(&d.CAMs, 30000)
	set(&d.WalletMs, 15000)
	set(&d.EvaluateMs, 30000)
	set(&d.EndorseMs, 30000)
	set(&d.SubmitMs, 30000)
	set(&d.CommitStatusMs, 300000)
	if c.Log.Level == "" {
		c.Log.Level = "info"
	}
	if c.Log.Format == "" {
		c.Log.Format = "json"
	}
}

func (c *Config) resolve() error {
	vals := []struct {
		name string
		v    *secret.Value
	}{
		{"temporal.payloadEncryptionKey", &c.Temporal.PayloadEncryptionKey},
		{"wallet.url", &c.Wallet.URL},
		{"wallet.username", &c.Wallet.Username},
		{"wallet.password", &c.Wallet.Password},
	}
	for i := range c.Registrars {
		vals = append(vals, struct {
			name string
			v    *secret.Value
		}{fmt.Sprintf("registrars[%d].enrollSecret", i), &c.Registrars[i].EnrollSecret})
	}
	var errs []error
	for _, f := range vals {
		if err := f.v.Resolve(c.EncryptionKey); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", f.name, err))
		}
	}
	return errors.Join(errs...)
}

func (c *Config) validate() error {
	var errs []string
	if !c.Wallet.URL.IsSet() {
		errs = append(errs, "wallet.url is required")
	}
	if !strings.Contains(c.Wallet.LabelFormat, "{user}") {
		errs = append(errs, "wallet.labelFormat must contain {user}")
	}
	for i, r := range c.Registrars {
		if r.CA == "" || r.EnrollID == "" || !r.EnrollSecret.IsSet() {
			errs = append(errs, fmt.Sprintf("registrars[%d]: ca, enrollId and enrollSecret are required", i))
		}
	}
	if c.Temporal.TLS != nil && (c.Temporal.TLS.CertFile == "") != (c.Temporal.TLS.KeyFile == "") {
		errs = append(errs, "temporal.tls: certFile and keyFile must be set together")
	}
	if len(errs) > 0 {
		return errors.New(strings.Join(errs, "; "))
	}
	return nil
}

// Label returns the wallet label for user in mspID.
func (w WalletConfig) Label(user, mspID string) string {
	return strings.NewReplacer("{user}", user, "{mspId}", mspID).Replace(w.LabelFormat)
}
