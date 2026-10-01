// Package notify sends the crash email.
package notify

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/smtp"
	"strconv"
	"strings"
	"time"

	"cipher-replicator/internal/config"
)

// Mailer sends plain-text mail over SMTP.
type Mailer struct {
	cfg  config.EmailConfig
	user string
	pass string
}

// New returns nil when email is disabled. Secrets must already be resolved;
// if they are not (e.g. the decryption key is wrong) they are treated as empty.
func New(cfg config.EmailConfig) *Mailer {
	if !cfg.Enabled {
		return nil
	}
	m := &Mailer{cfg: cfg}
	func() {
		defer func() { _ = recover() }() // unresolved secret: send without auth
		m.user, m.pass = cfg.Username.String(), cfg.Password.String()
	}()
	return m
}

// Send delivers subject/body to the configured recipients.
func (m *Mailer) Send(ctx context.Context, subject, body string) error {
	if m == nil {
		return nil
	}
	timeout := time.Duration(m.cfg.TimeoutMs) * time.Millisecond
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	addr := net.JoinHostPort(m.cfg.Host, strconv.Itoa(m.cfg.Port))
	tlsCfg := &tls.Config{ServerName: m.cfg.Host, InsecureSkipVerify: m.cfg.InsecureTLS} //nolint:gosec // opt-in

	d := &net.Dialer{}
	var conn net.Conn
	var err error
	if m.cfg.TLSMode == "tls" {
		conn, err = (&tls.Dialer{NetDialer: d, Config: tlsCfg}).DialContext(ctx, "tcp", addr)
	} else {
		conn, err = d.DialContext(ctx, "tcp", addr)
	}
	if err != nil {
		return fmt.Errorf("smtp dial: %w", err)
	}
	if dl, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(dl)
	}
	c, err := smtp.NewClient(conn, m.cfg.Host)
	if err != nil {
		conn.Close()
		return fmt.Errorf("smtp: %w", err)
	}
	defer c.Close()

	if m.cfg.TLSMode == "starttls" {
		if ok, _ := c.Extension("STARTTLS"); !ok {
			return errors.New("smtp server does not support STARTTLS (set email.tlsMode)")
		}
		if err := c.StartTLS(tlsCfg); err != nil {
			return fmt.Errorf("smtp starttls: %w", err)
		}
	}
	if m.user != "" {
		if err := c.Auth(smtp.PlainAuth("", m.user, m.pass, m.cfg.Host)); err != nil {
			return fmt.Errorf("smtp auth: %w", err)
		}
	}
	if err := c.Mail(m.cfg.From); err != nil {
		return fmt.Errorf("smtp MAIL FROM: %w", err)
	}
	for _, to := range m.cfg.To {
		if err := c.Rcpt(to); err != nil {
			return fmt.Errorf("smtp RCPT TO %s: %w", to, err)
		}
	}
	w, err := c.Data()
	if err != nil {
		return fmt.Errorf("smtp DATA: %w", err)
	}
	if _, err := w.Write([]byte(buildMessage(m.cfg.From, m.cfg.To, m.cfg.SubjectPrefix+" "+subject, body))); err != nil {
		return err
	}
	if err := w.Close(); err != nil {
		return err
	}
	return c.Quit()
}

func buildMessage(from string, to []string, subject, body string) string {
	var b strings.Builder
	h := func(k, v string) { b.WriteString(k + ": " + v + "\r\n") }
	h("From", from)
	h("To", strings.Join(to, ", "))
	h("Subject", strings.NewReplacer("\r", " ", "\n", " ").Replace(subject))
	h("Date", time.Now().Format(time.RFC1123Z))
	h("MIME-Version", "1.0")
	h("Content-Type", "text/plain; charset=UTF-8")
	b.WriteString("\r\n")
	b.WriteString(strings.ReplaceAll(strings.ReplaceAll(body, "\r\n", "\n"), "\n", "\r\n"))
	return b.String()
}
