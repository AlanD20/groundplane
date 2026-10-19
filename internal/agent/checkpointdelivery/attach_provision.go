package checkpointdelivery

import (
	"context"
	"sync"

	"github.com/AlanD20/groundplane/internal/common/attachexecution"
	"github.com/AlanD20/groundplane/internal/infra/agentworkjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type AttachProvisionExchange func(context.Context, *agentpb.AttachProvisionCheckpointRequest) (*agentpb.AttachProvisionCheckpointAck, error)

type AttachProvision struct {
	mu       sync.Mutex
	input    attachexecution.ProvisionInput
	journal  *agentworkjournal.Journal
	exchange AttachProvisionExchange
}

func NewAttachProvision(input attachexecution.ProvisionInput, journal *agentworkjournal.Journal,
	exchange AttachProvisionExchange,
) (*AttachProvision, error) {
	if input.Validate() != nil || input.Attempt.StartedAt == nil || input.Attempt.FinishedAt != nil || journal == nil ||
		exchange == nil {
		return nil, errs.New(
			errs.KindValidationFailed,
			"Attach provisioning reporting requires a started Agent attempt",
		)
	}
	return &AttachProvision{input: input, journal: journal, exchange: exchange}, nil
}

func (delivery *AttachProvision) Checkpoint(ctx context.Context, next attachexecution.ProvisionProgress) error {
	delivery.mu.Lock()
	defer delivery.mu.Unlock()
	record, err := delivery.journal.PrepareAttachProvision(ctx, delivery.input, next)
	if err != nil {
		return err
	}
	return delivery.deliver(ctx, *record)
}

// Resume only replays a durable report. It never reexecutes a provisioning command.
func (delivery *AttachProvision) Resume(ctx context.Context) error {
	delivery.mu.Lock()
	defer delivery.mu.Unlock()
	record, err := delivery.journal.AttachProvision(ctx, delivery.input)
	if err != nil || record == nil {
		return err
	}
	return delivery.deliver(ctx, *record)
}

func (delivery *AttachProvision) Current(ctx context.Context) (*attachexecution.ProvisionProgress, error) {
	delivery.mu.Lock()
	defer delivery.mu.Unlock()
	record, err := delivery.journal.AttachProvision(ctx, delivery.input)
	if err != nil || record == nil {
		return nil, err
	}
	if !record.Acknowledged {
		return nil, errs.New(
			errs.KindStateConflict,
			"Attach provisioning command boundary is not acknowledged by the Controller",
		)
	}
	return &record.Report.Progress, nil
}

func (delivery *AttachProvision) deliver(ctx context.Context, record agentworkjournal.AttachProvisionRecord) error {
	if record.Acknowledged {
		return nil
	}
	request, err := attachexecution.EncodeProvisionCheckpointRequest(delivery.input, record.Report)
	if err != nil {
		return err
	}
	ack, err := delivery.exchange(ctx, request)
	if err != nil {
		return err
	}
	return delivery.journal.AcknowledgeAttachProvision(ctx, delivery.input, ack)
}
