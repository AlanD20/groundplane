package checkpointdelivery

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"sync"
	"time"

	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/agentworkjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type EffectExchange func(context.Context, *agentpb.WorkAttemptIdentity, []byte, workexecution.Input) (workexecution.Input, error)

type Effect struct {
	mu       sync.Mutex
	input    releaseexecution.Input
	journal  *agentworkjournal.Journal
	exchange EffectExchange
}

func NewEffect(input releaseexecution.Input, journal *agentworkjournal.Journal, exchange EffectExchange) (*Effect, error) {
	if input.Attempt.Validate() != nil || input.Attempt.StartedAt == nil || journal == nil || exchange == nil {
		return nil, errs.New(errs.KindInternal, "Release effect evidence delivery is not configured")
	}
	return &Effect{input: input, journal: journal, exchange: exchange}, nil
}

func (delivery *Effect) RecordHelper(ctx context.Context, boundary releaseexecution.CheckpointReport, response *agentpb.ReleaseHelperResponse) (workexecution.Input, error) {
	result, err := releaseexecution.HelperResult(response)
	if err != nil {
		return workexecution.Input{}, err
	}
	return delivery.record(ctx, boundary, result)
}

func (delivery *Effect) RecordReadiness(ctx context.Context, boundary releaseexecution.CheckpointReport, observed *agentpb.ObservedProject) (workexecution.Input, error) {
	result, err := releaseexecution.ReadinessResult(delivery.input, boundary.Progress, observed)
	if err != nil {
		return workexecution.Input{}, err
	}
	return delivery.record(ctx, boundary, result)
}

func (delivery *Effect) RecordDrain(ctx context.Context, boundary releaseexecution.CheckpointReport, at time.Time) (workexecution.Input, error) {
	at = at.UTC()
	return delivery.record(ctx, boundary, releaseexecution.EffectResult{DrainedAt: &at})
}

func (delivery *Effect) RecordConfiguration(ctx context.Context, boundary releaseexecution.CheckpointReport, files []entrymaterialization.Header) (workexecution.Input, error) {
	result, err := releaseexecution.ConfigurationResult(delivery.input, boundary, files)
	if err != nil {
		return workexecution.Input{}, err
	}
	return delivery.record(ctx, boundary, result)
}

func (delivery *Effect) record(ctx context.Context, boundary releaseexecution.CheckpointReport, result releaseexecution.EffectResult) (workexecution.Input, error) {
	delivery.mu.Lock()
	defer delivery.mu.Unlock()
	record, err := delivery.journal.PrepareEffect(ctx, delivery.input, boundary, result, time.Now().UTC())
	if err != nil {
		return workexecution.Input{}, err
	}
	return delivery.send(ctx, *record)
}

// Resume replays saved bytes only. found=false means the host effect still needs
// operation-specific inspection, never that it is safe to repeat the mutation.
func (delivery *Effect) Resume(ctx context.Context, boundary releaseexecution.CheckpointReport) (reference workexecution.Input, found bool, err error) {
	delivery.mu.Lock()
	defer delivery.mu.Unlock()
	record, err := delivery.journal.Effect(ctx, delivery.input, boundary)
	if err != nil || record == nil {
		return reference, false, err
	}
	reference, err = delivery.send(ctx, *record)
	return reference, true, err
}

func (delivery *Effect) send(ctx context.Context, record agentworkjournal.EffectRecord) (workexecution.Input, error) {
	if record.Acknowledged {
		return record.Reference, nil
	}
	identity, err := workexecution.EncodeAgentAttempt(delivery.input.Attempt)
	if err != nil {
		return workexecution.Input{}, err
	}
	ack, err := delivery.exchange(ctx, identity, record.Raw, record.Reference)
	if err != nil {
		return workexecution.Input{}, err
	}
	if ack != record.Reference {
		return workexecution.Input{}, errs.New(errs.KindStateConflict, "Release effect evidence acknowledgement differs")
	}
	if err := delivery.journal.AcknowledgeEffect(ctx, delivery.input, record.Boundary, ack); err != nil {
		return workexecution.Input{}, err
	}
	return ack, nil
}
