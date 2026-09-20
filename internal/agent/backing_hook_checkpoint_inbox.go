package agent

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func (p *WorkerPool) checkpointBackingHook(
	ctx context.Context,
	request *agentpb.BackingHookCheckpointRequest,
) (*agentpb.BackingHookCheckpointAck, error) {
	if p == nil || p.backingHookCheckpoints == nil || ctx == nil {
		return nil, errs.New(errs.KindInternal, "agent: backing hook checkpoint transport is not configured")
	}
	validated, err := executionplan.ValidateBackingHookCheckpointRequest(request)
	if err != nil {
		return nil, err
	}
	acks, abandon, err := p.backingHookCheckpoints.Register(validated)
	if err != nil {
		return nil, err
	}
	defer abandon()
	select {
	case p.outputs <- WorkerOutput{BackingHookCheckpoint: proto.Clone(validated).(*agentpb.BackingHookCheckpointRequest)}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	select {
	case ack := <-acks:
		return ack, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (p *WorkerPool) AcceptBackingHookCheckpointAck(ack *agentpb.BackingHookCheckpointAck) error {
	if p == nil || p.backingHookCheckpoints == nil || ack == nil {
		return errs.New(errs.KindInternal, "agent: backing hook checkpoint acknowledgement is missing")
	}
	return p.backingHookCheckpoints.Accept(ack)
}
