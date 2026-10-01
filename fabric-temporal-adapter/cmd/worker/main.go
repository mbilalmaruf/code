// Command worker runs the Temporal worker for the FabricTransaction workflow.
//
//	worker [-config configs/worker.json]   (env ADAPTER_CONFIG)
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"fabric-adapter/internal/adapter"
	"fabric-adapter/internal/config"
	"fabric-adapter/internal/fabric"
	"fabric-adapter/internal/temporalx"
	"fabric-adapter/internal/wallet"
)

func main() {
	def := os.Getenv("ADAPTER_CONFIG")
	if def == "" {
		def = "configs/worker.json"
	}
	path := flag.String("config", def, "worker config file")
	flag.Parse()
	if err := run(*path); err != nil {
		fmt.Fprintln(os.Stderr, "worker:", err)
		os.Exit(1)
	}
}

func run(path string) error {
	cfg, err := config.Load(path)
	if err != nil {
		return err
	}
	log := temporalx.Logger(cfg.Log)

	w, err := wallet.New(cfg.Wallet.URL.String(), cfg.Wallet.Username.String(), cfg.Wallet.Password.String(),
		cfg.Wallet.Database, cfg.Timeouts.Wallet(), cfg.Wallet.InsecureTLS)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := w.EnsureDB(ctx); err != nil {
		return fmt.Errorf("wallet database %q: %w", cfg.Wallet.Database, err)
	}

	c, err := temporalx.Dial(cfg, log)
	if err != nil {
		return fmt.Errorf("temporal: %w", err)
	}
	defer c.Close()

	pool := fabric.NewPool()
	defer pool.Close()

	wk := worker.New(c, cfg.Temporal.TaskQueue, worker.Options{
		MaxConcurrentActivityExecutionSize: cfg.Temporal.MaxConcurrentActivities,
	})
	wk.RegisterWorkflowWithOptions(adapter.FabricTransaction, workflow.RegisterOptions{Name: adapter.WorkflowName})
	wk.RegisterActivityWithOptions(adapter.NewActivities(cfg, pool, w, log), activity.RegisterOptions{})

	log.Info("worker started", "taskQueue", cfg.Temporal.TaskQueue, "namespace", cfg.Temporal.Namespace,
		"payloadEncryption", cfg.Temporal.PayloadEncryptionKey.IsSet())
	return wk.Run(worker.InterruptCh())
}
