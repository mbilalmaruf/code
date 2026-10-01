// Command replicator streams CouchDB documents into denormalised Postgres
// tables. See README.md and CLAUDE.md.
//
//	replicator [-config configs/app.json]            run
//	replicator validate [-config configs/app.json]   load + validate configs, then exit
//	replicator encrypt  < plaintext                   print an encrypted blob (key from REPLICATOR_ENCRYPTION_KEY)
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"runtime/debug"
	"strings"
	"syscall"
	"time"

	"cipher-replicator/internal/config"
	"cipher-replicator/internal/notify"
	"cipher-replicator/internal/replicator"
	"cipher-replicator/internal/secret"
)

func main() {
	cmd := "run"
	args := os.Args[1:]
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		cmd, args = args[0], args[1:]
	}
	fs := flag.NewFlagSet(cmd, flag.ExitOnError)
	defaultCfg := os.Getenv("REPLICATOR_CONFIG")
	if defaultCfg == "" {
		defaultCfg = "configs/app.json"
	}
	cfgPath := fs.String("config", defaultCfg, "path to the application config (env REPLICATOR_CONFIG)")
	_ = fs.Parse(args)

	switch cmd {
	case "run":
		os.Exit(run(*cfgPath))
	case "validate":
		os.Exit(validate(*cfgPath))
	case "encrypt":
		os.Exit(encrypt())
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q (run | validate | encrypt)\n", cmd)
		os.Exit(2)
	}
}

func newLogger(c config.LogConfig) *slog.Logger {
	var lvl slog.Level
	_ = lvl.UnmarshalText([]byte(c.Level))
	opts := &slog.HandlerOptions{Level: lvl}
	if strings.EqualFold(c.Format, "text") {
		return slog.New(slog.NewTextHandler(os.Stdout, opts))
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, opts))
}

func run(cfgPath string) (code int) {
	app, err := config.LoadApp(cfgPath)
	if app == nil {
		slog.Error("cannot start", "error", err)
		return 1
	}
	log := newLogger(app.Log)
	mailer := notify.New(app.Email)

	crash := func(err error) int {
		log.Error("replicator crashed", "error", err)
		body := fmt.Sprintf("Instance: %s\nTime: %s\nConfig: %s\n\nError:\n%v\n",
			app.InstanceName, time.Now().UTC().Format(time.RFC3339), cfgPath, err)
		ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer cancel()
		if mErr := mailer.Send(ctx, app.InstanceName+" crashed", body); mErr != nil {
			log.Error("crash email failed", "error", mErr)
		} else if mailer != nil {
			log.Info("crash email sent", "to", app.Email.To)
		}
		return 1
	}
	defer func() {
		if r := recover(); r != nil {
			code = crash(fmt.Errorf("panic: %v\n%s", r, debug.Stack()))
		}
	}()
	if err != nil {
		return crash(fmt.Errorf("app config: %w", err))
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	log.Info("starting", "instance", app.InstanceName, "config", cfgPath)
	if err := replicator.Run(ctx, app, config.NewFileSource(app), log); err != nil {
		return crash(err)
	}
	return 0
}

func validate(cfgPath string) int {
	app, err := config.LoadApp(cfgPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "app config:", err)
		return 1
	}
	procs, err := config.NewFileSource(app).Load(context.Background())
	if err == nil {
		err = config.PrepareAll(procs)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	for _, p := range procs {
		fmt.Printf("ok  %-24s enabled=%-5v tables=%d  (%s)\n", p.Name, p.IsEnabled(), len(p.SchemaConfig), p.Origin)
	}
	return 0
}

func encrypt() int {
	key := os.Getenv(config.EnvEncryptionKey)
	if key == "" {
		fmt.Fprintln(os.Stderr, config.EnvEncryptionKey+" must be set")
		return 1
	}
	in, err := io.ReadAll(os.Stdin)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	plaintext := strings.TrimRight(string(in), "\r\n")
	if plaintext == "" {
		fmt.Fprintln(os.Stderr, errors.New("empty input"))
		return 1
	}
	b, err := secret.Encrypt(plaintext, key)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	out, _ := json.Marshal(b)
	fmt.Println(string(out))
	return 0
}
