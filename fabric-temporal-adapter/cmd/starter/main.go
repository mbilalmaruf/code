// Command starter runs one FabricTransaction workflow from a request file and
// prints the result. Useful for testing; production callers start the
// workflow from their own Temporal client.
//
//	starter -request req.json [-id my-idempotency-key] [-config configs/worker.json]
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"go.temporal.io/sdk/client"

	"fabric-adapter/internal/adapter"
	"fabric-adapter/internal/config"
	"fabric-adapter/internal/temporalx"
)

func main() {
	def := os.Getenv("ADAPTER_CONFIG")
	if def == "" {
		def = "configs/worker.json"
	}
	cfgPath := flag.String("config", def, "config file (temporal connection + payload key)")
	reqPath := flag.String("request", "", "request JSON file (required)")
	id := flag.String("id", "", "workflow ID; reuse the same ID to deduplicate a request (default: random)")
	timeout := flag.Duration("timeout", 10*time.Minute, "how long to wait for the result")
	flag.Parse()
	if err := run(*cfgPath, *reqPath, *id, *timeout); err != nil {
		fmt.Fprintln(os.Stderr, "starter:", err)
		os.Exit(1)
	}
}

func run(cfgPath, reqPath, id string, timeout time.Duration) error {
	if reqPath == "" {
		return fmt.Errorf("-request is required")
	}
	cfg, err := config.Load(cfgPath)
	if err != nil {
		return err
	}
	raw, err := os.ReadFile(reqPath)
	if err != nil {
		return err
	}
	var req adapter.Request
	if err := json.Unmarshal(raw, &req); err != nil {
		return fmt.Errorf("parse request: %w", err)
	}
	if err := req.Validate(); err != nil {
		return err
	}
	c, err := temporalx.Dial(cfg, temporalx.Logger(cfg.Log))
	if err != nil {
		return err
	}
	defer c.Close()

	if id == "" {
		id = fmt.Sprintf("fabric-%s-%d", req.Function, time.Now().UnixNano())
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        id,
		TaskQueue: cfg.Temporal.TaskQueue,
	}, adapter.WorkflowName, req)
	if err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "started workflow %s (run %s)\n", run.GetID(), run.GetRunID())
	var res adapter.Result
	if err := run.Get(ctx, &res); err != nil {
		return err
	}
	out, _ := json.MarshalIndent(res, "", "  ")
	fmt.Println(string(out))
	return nil
}
