package checkpointdelivery

import (
	"context"
	"sync"

	"github.com/AlanD20/groundplane/internal/common/attachexecution"
	"github.com/AlanD20/groundplane/internal/infra/agentworkjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type AttachRemovalExchange func(context.Context, *agentpb.AttachRemovalCheckpointRequest) (*agentpb.AttachRemovalCheckpointAck, error)

type AttachRemoval struct {
	mu       sync.Mutex
	input    attachexecution.RemovalInput
	journal  *agentworkjournal.Journal
	exchange AttachRemovalExchange
}

func NewAttachRemoval(input attachexecution.RemovalInput, journal *agentworkjournal.Journal,
	exchange AttachRemovalExchange,
) (*AttachRemoval, error) {
	if input.Validate() != nil || input.Attempt.StartedAt == nil || input.Attempt.FinishedAt != nil || journal == nil || exchange == nil {
		return nil, errs.New(errs.KindValidationFailed, "Attach removal reporting requires a started Agent attempt")
	}
	return &AttachRemoval{input: input, journal: journal, exchange: exchange}, nil
}

func (delivery *AttachRemoval) Checkpoint(ctx context.Context, next attachexecution.RemovalProgress) error {
	delivery.mu.Lock()
	defer delivery.mu.Unlock()
	record, err := delivery.journal.PrepareAttachRemoval(ctx, delivery.input, next)
	if err != nil {
		return err
	}
	return delivery.deliver(ctx, *record)
}

// Resume only replays a durable report. It never reexecutes a Detach command.
func (delivery *AttachRemoval) Resume(ctx context.Context) error {
	delivery.mu.Lock()
	defer delivery.mu.Unlock()
	record, err := delivery.journal.AttachRemoval(ctx, delivery.input)
	if err != nil || record == nil {
		return err
	}
	return delivery.deliver(ctx, *record)
}

func (delivery *AttachRemoval) Current(ctx context.Context) (*attachexecution.RemovalProgress, error) {
	delivery.mu.Lock()
	defer delivery.mu.Unlock()
	record, err := delivery.journal.AttachRemoval(ctx, delivery.input)
	if err != nil || record == nil {
		return nil, err
	}
	if !record.Acknowledged {
		return nil, errs.New(errs.KindStateConflict, "Attach removal command boundary is not acknowledged by the Controller")
	}
	return &record.Report.Progress, nil
}

func (delivery *AttachRemoval) deliver(ctx context.Context, record agentworkjournal.AttachRemovalRecord) error {
	if record.Acknowledged {
		return nil
	}
	request, err := attachexecution.EncodeRemovalCheckpointRequest(delivery.input, record.Report)
	if err != nil {
		return err
	}
	ack, err := delivery.exchange(ctx, request)
	if err != nil {
		return err
	}
	return delivery.journal.AcknowledgeAttachRemoval(ctx, delivery.input, ack)
}
