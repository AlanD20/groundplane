package agent

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func (p *WorkerPool) CheckpointScript(ctx context.Context, request *agentpb.ScriptCheckpointRequest) error {
	if ctx == nil || p == nil || p.scriptCheckpoints == nil {
		return errs.New(errs.KindInternal, "agent: Script checkpoint transport is not configured")
	}
	validated, err := executionplan.ValidateScriptCheckpointRequest(request)
	if err != nil {
		return err
	}
	acknowledged, abandon, err := p.scriptCheckpoints.Register(validated)
	if err != nil {
		return err
	}
	select {
	case p.outputs <- WorkerOutput{ScriptCheckpoint: proto.Clone(validated).(*agentpb.ScriptCheckpointRequest)}:
	case <-ctx.Done():
		abandon()
		return ctx.Err()
	}
	select {
	case <-acknowledged:
		return nil
	case <-ctx.Done():
		abandon()
		return ctx.Err()
	}
}

func (p *WorkerPool) AcceptScriptCheckpointAck(ack *agentpb.ScriptCheckpointAck) error {
	if p == nil || p.scriptCheckpoints == nil {
		return errs.New(errs.KindInternal, "agent: Script checkpoint transport is not configured")
	}
	return p.scriptCheckpoints.Accept(ack)
}
