// Package temporalx builds the Temporal client shared by the worker and starter.
package temporalx

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"log/slog"
	"os"

	"go.temporal.io/sdk/client"
	tlog "go.temporal.io/sdk/log"

	"fabric-adapter/internal/codec"
	"fabric-adapter/internal/config"
)

func Dial(cfg *config.Config, log *slog.Logger) (client.Client, error) {
	dc, err := codec.DataConverter(cfg.Temporal.PayloadEncryptionKey.String())
	if err != nil {
		return nil, err
	}
	opts := client.Options{
		HostPort:      cfg.Temporal.HostPort,
		Namespace:     cfg.Temporal.Namespace,
		DataConverter: dc,
		Logger:        tlog.NewStructuredLogger(log),
	}
	if t := cfg.Temporal.TLS; t != nil {
		tc := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: t.ServerName}
		if t.CAFile != "" {
			pem, err := os.ReadFile(t.CAFile)
			if err != nil {
				return nil, err
			}
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM(pem) {
				return nil, errors.New("temporal.tls.caFile has no valid certificate")
			}
			tc.RootCAs = pool
		}
		if t.CertFile != "" {
			kp, err := tls.LoadX509KeyPair(t.CertFile, t.KeyFile)
			if err != nil {
				return nil, err
			}
			tc.Certificates = []tls.Certificate{kp}
		}
		opts.ConnectionOptions.TLS = tc
	}
	return client.Dial(opts)
}

// Logger builds the process logger from config.
func Logger(c config.LogConfig) *slog.Logger {
	var lvl slog.Level
	_ = lvl.UnmarshalText([]byte(c.Level))
	o := &slog.HandlerOptions{Level: lvl}
	if c.Format == "text" {
		return slog.New(slog.NewTextHandler(os.Stdout, o))
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, o))
}
