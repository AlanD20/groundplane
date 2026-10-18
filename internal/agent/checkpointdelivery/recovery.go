package checkpointdelivery

import (
	"context"
	"sync"

	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/infra/agentworkjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type RecoveryExchange func(context.Context, *agentpb.ReleaseRecoveryCheckpointRequest) (*agentpb.ReleaseRecoveryCheckpointAck, error)

type Recovery struct {
	mu       sync.Mutex
	input    releaseexecution.RecoveryInput
	journal  *agentworkjournal.Journal
	exchange RecoveryExchange
}

func NewRecovery(input releaseexecution.RecoveryInput, journal *agentworkjournal.Journal, exchange RecoveryExchange) (*Recovery, error) {
	if input.Validate() != nil || input.Input.Attempt.StartedAt == nil || journal == nil || exchange == nil {
		return nil, errs.New(errs.KindValidationFailed, "Recovery checkpoint delivery requires a started cleanup attempt")
	}
	return &Recovery{input: input, journal: journal, exchange: exchange}, nil
}

func (delivery *Recovery) Checkpoint(ctx context.Context, next releaseexecution.RecoveryProgress) error {
	delivery.mu.Lock()
	defer delivery.mu.Unlock()
	report, err := delivery.journal.PrepareRecovery(ctx, delivery.input, next)
	if err != nil {
		return err
	}
	return delivery.deliver(ctx, *report)
}

// Resume sends the saved report, never executes its action again. The recovery
// driver must inspect an in-flight action after this acknowledgement succeeds.
func (delivery *Recovery) Resume(ctx context.Context) error {
	delivery.mu.Lock()
	defer delivery.mu.Unlock()
	report, err := delivery.journal.Recovery(ctx, delivery.input)
	if err != nil || report == nil {
		return err
	}
	return delivery.deliver(ctx, *report)
}

func (delivery *Recovery) Current(ctx context.Context) (*releaseexecution.RecoveryReport, error) {
	delivery.mu.Lock()
	defer delivery.mu.Unlock()
	report, err := delivery.journal.Recovery(ctx, delivery.input)
	if err != nil || report == nil {
		return report, err
	}
	if report.Ack == nil || report.Validate(delivery.input) != nil {
		return nil, errs.New(errs.KindStateConflict, "Recovery cursor has no durable Controller acknowledgement")
	}
	return report, nil
}

func (delivery *Recovery) deliver(ctx context.Context, report releaseexecution.RecoveryReport) error {
	if report.Ack != nil {
		return nil
	}
	request, err := releaseexecution.EncodeRecoveryRequest(delivery.input, report)
	if err != nil {
		return err
	}
	response, err := delivery.exchange(ctx, request)
	if err != nil {
		return err
	}
	ack, err := releaseexecution.DecodeRecoveryAck(delivery.input, report, response)
	if err != nil {
		return err
	}
	return delivery.journal.AcknowledgeRecovery(ctx, ack)
}
