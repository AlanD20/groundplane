// Package agent is the execution plane: a gRPC client (dial-out, pull
// model) driving a worker pool that applies Controller-assigned tasks
// through Docker Compose. The Agent has no decision authority and
// carries no scripts of its own. See mvp.md, "Agent (execution plane)".
package agent

import (
	"context"
	"log/slog"
	"time"

	"github.com/sample-tenant/groundplane/internal/common/runner"
	"github.com/sample-tenant/groundplane/pkg/errs"
)

// Client owns the one long-lived bidirectional stream to the
// Controller, initiated by the Agent (see proto/agent.proto). It signals
// Ready at PullInterval — the idle Ready IS the heartbeat, authoritative
// for dead-agent detection; HTTP/2 keepalive proves only socket
// liveness and drives reconnect timing.
type Client struct {
	ControllerAddr string
	AgentID        string
	Logger         *slog.Logger

	PullInterval       time.Duration
	MaxConcurrentTasks int

	pool *WorkerPool
}

func NewClient(controllerAddr string, logger *slog.Logger) *Client {
	return &Client{
		ControllerAddr:     controllerAddr,
		Logger:             logger,
		PullInterval:       5 * time.Second,
		MaxConcurrentTasks: 4,
	}
}

// Connect dials the Controller and performs the join-token handshake,
// receiving AgentConfig (pull interval, max concurrent tasks, labels).
//
// TODO: wire google.golang.org/grpc + proto/agentpb (generated via
// `make proto`). Sketch:
//
//	conn, err := grpc.NewClient(c.ControllerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
//	stub := agentpb.NewAgentChannelClient(conn)
//	resp, err := stub.Connect(ctx, &agentpb.ConnectRequest{JoinToken: token})
func (c *Client) Connect(ctx context.Context, joinToken string) error {
	return errs.New(errs.CodeNotImplemented, "agent: Connect not implemented")
}

// Run opens the Channel stream and blocks, sending an idle Ready at
// PullInterval, receiving TaskAssignment/TaskAbort/ConfigUpdate/
// ImageUpdate/Shutdown, and feeding assignments to the worker pool.
// Reconnects with exponential backoff on a dropped stream.
func (c *Client) Run(ctx context.Context) error {
	c.pool = NewWorkerPool(c.MaxConcurrentTasks, runner.New(c.Logger), c.Logger)
	go c.pool.Run(ctx)

	ticker := time.NewTicker(c.PullInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			// TODO: send Ready{capacity: c.pool.Capacity()} on the open stream.
			c.Logger.Debug("agent: ready", "capacity", c.pool.Capacity())
		}
	}
}
