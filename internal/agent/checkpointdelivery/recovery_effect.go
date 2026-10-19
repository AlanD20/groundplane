package checkpointdelivery

import (
	"context"
	"sync"
	"time"

	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/agentworkjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type RecoveryEffect struct {
	mu       sync.Mutex
	input    releaseexecution.RecoveryInput
	journal  *agentworkjournal.Journal
	exchange EffectExchange
}

func NewRecoveryEffect(
	input releaseexecution.RecoveryInput,
	journal *agentworkjournal.Journal,
	exchange EffectExchange,
) (*RecoveryEffect, error) {
	if input.Validate() != nil || input.Input.Attempt.StartedAt == nil || journal == nil || exchange == nil {
		return nil, errs.New(errs.KindInternal, "Release recovery evidence delivery is not configured")
	}
	return &RecoveryEffect{input: input, journal: journal, exchange: exchange}, nil
}

func (delivery *RecoveryEffect) Record(
	ctx context.Context,
	boundary releaseexecution.RecoveryReport,
	result releaseexecution.RecoveryEffectResult,
) (workexecution.Input, error) {
	delivery.mu.Lock()
	defer delivery.mu.Unlock()
	record, err := delivery.journal.PrepareRecoveryEffect(ctx, delivery.input, boundary, result, time.Now().UTC())
	if err != nil {
		return workexecution.Input{}, err
	}
	return delivery.send(ctx, *record)
}

func (delivery *RecoveryEffect) Resume(
	ctx context.Context,
	boundary releaseexecution.RecoveryReport,
) (workexecution.Input, bool, error) {
	delivery.mu.Lock()
	defer delivery.mu.Unlock()
	record, err := delivery.journal.RecoveryEffect(ctx, delivery.input, boundary)
	if err != nil || record == nil {
		return workexecution.Input{}, false, err
	}
	reference, err := delivery.send(ctx, *record)
	return reference, true, err
}

func (delivery *RecoveryEffect) send(
	ctx context.Context,
	record agentworkjournal.RecoveryEffectRecord,
) (workexecution.Input, error) {
	if record.Acknowledged {
		return record.Reference, nil
	}
	identity, err := workexecution.EncodeAgentAttempt(delivery.input.Input.Attempt)
	if err != nil {
		return workexecution.Input{}, err
	}
	ack, err := delivery.exchange(ctx, identity, record.Raw, record.Reference)
	if err != nil {
		return workexecution.Input{}, err
	}
	if ack != record.Reference {
		return workexecution.Input{}, errs.New(
			errs.KindStateConflict,
			"Release recovery evidence acknowledgement differs",
		)
	}
	if err := delivery.journal.AcknowledgeRecoveryEffect(ctx, delivery.input, record.Boundary, ack); err != nil {
		return workexecution.Input{}, err
	}
	return ack, nil
}
