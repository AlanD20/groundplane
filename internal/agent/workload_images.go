package agent

import (
	"context"
	"sync"

	"github.com/AlanD20/groundplane/internal/common/workloadimage"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// WorkloadImageResolver is the Agent's read-only Docker observation port.
type WorkloadImageResolver interface {
	Resolve(context.Context, *agentpb.ResolveWorkloadImages) (*agentpb.WorkloadImageResolutionResult, error)
}

func (c *Client) SetWorkloadImageResolver(resolver WorkloadImageResolver) error {
	if c == nil || resolver == nil {
		return errs.New(errs.KindValidationFailed, "agent: image resolver is required")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.started || c.pool != nil {
		return errs.New(errs.KindStateConflict, "agent: image resolver cannot change after start")
	}
	c.images = resolver
	return nil
}

type imageOutput struct {
	result *agentpb.WorkloadImageResolutionResult
	err    error
}

// Only the stream loop calls start and consumes results. The one worker never
// writes to the stream; close cancels and joins it before session replacement.
type imageSession struct {
	ctx      context.Context
	cancel   context.CancelFunc
	resolver WorkloadImageResolver
	outputs  chan imageOutput
	workers  sync.WaitGroup
	busy     bool
}

func newImageSession(ctx context.Context, resolver WorkloadImageResolver) *imageSession {
	ctx, cancel := context.WithCancel(ctx)
	return &imageSession{ctx: ctx, cancel: cancel, resolver: resolver, outputs: make(chan imageOutput, 1)}
}

func (session *imageSession) start(request *agentpb.ResolveWorkloadImages) error {
	if session.busy || session.resolver == nil {
		return errs.New(errs.KindStateConflict, "agent: image resolver unavailable or busy")
	}
	if err := workloadimage.ValidateRequest(request); err != nil {
		return err
	}
	if err := session.ctx.Err(); err != nil {
		return err
	}
	owned := proto.CloneOf(request)
	session.busy = true
	session.workers.Add(1)
	go func() {
		defer session.workers.Done()
		ctx, cancel := context.WithTimeout(session.ctx, workloadimage.Timeout)
		defer cancel()
		result, err := session.resolver.Resolve(ctx, owned)
		if ctx.Err() != nil {
			result, err = nil, ctx.Err()
		}
		if err == nil {
			err = workloadimage.ValidateResult(owned, result)
		}
		select {
		case session.outputs <- imageOutput{result: result, err: err}:
		case <-session.ctx.Done():
		}
	}()
	return nil
}

func (session *imageSession) close() {
	session.cancel()
	session.workers.Wait()
}
