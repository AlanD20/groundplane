package checkpointdelivery

import (
	"context"
	"sync"

	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/agentworkjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type ReleaseExchange func(context.Context, *agentpb.ReleaseWorkCheckpointRequest) (*agentpb.ReleaseWorkCheckpointAck, error)

// Release coordinates durable cursor delivery, not host effects. A successful
// send without an exact durable acknowledgement cannot advance execution.
type Release struct {
	mu       sync.Mutex
	input    releaseexecution.Input
	journal  *agentworkjournal.Journal
	exchange ReleaseExchange
}

func NewRelease(input releaseexecution.Input, journal *agentworkjournal.Journal, exchange ReleaseExchange) (*Release, error) {
	if input.Attempt.Validate() != nil || input.Attempt.Executor.Executor != workexecution.Agent || input.Attempt.StartedAt == nil || input.Attempt.FinishedAt != nil || input.Release == nil || journal == nil || exchange == nil {
		return nil, errs.New(errs.KindValidationFailed, "Release checkpoint delivery requires a started Agent attempt")
	}
	return &Release{input: input, journal: journal, exchange: exchange}, nil
}

func (delivery *Release) Checkpoint(ctx context.Context, next releaseexecution.Progress) error {
	if delivery == nil || ctx == nil {
		return errs.New(errs.KindInternal, "Release checkpoint delivery is not configured")
	}
	delivery.mu.Lock()
	defer delivery.mu.Unlock()
	report, err := delivery.journal.PrepareRelease(ctx, delivery.input, next)
	if err != nil {
		return err
	}
	return delivery.deliver(ctx, *report)
}

// Resume redelivers the pending report only. The host executor must inspect any
// in-flight effect separately; an acknowledgement never means repeat the effect.
func (delivery *Release) Resume(ctx context.Context) error {
	if delivery == nil || ctx == nil {
		return errs.New(errs.KindInternal, "Release checkpoint delivery is not configured")
	}
	delivery.mu.Lock()
	defer delivery.mu.Unlock()
	report, err := delivery.journal.Release(ctx, delivery.input)
	if err != nil || report == nil {
		return err
	}
	return delivery.deliver(ctx, *report)
}

// Current reloads the durable acknowledged cursor. It is not cached transport
// state: a caller must use it after delivery before crossing an effect boundary.
func (delivery *Release) Current(ctx context.Context) (*releaseexecution.CheckpointReport, error) {
	if delivery == nil || ctx == nil {
		return nil, errs.New(errs.KindInternal, "Release checkpoint delivery is not configured")
	}
	delivery.mu.Lock()
	defer delivery.mu.Unlock()
	report, err := delivery.journal.Release(ctx, delivery.input)
	if err != nil || report == nil {
		return report, err
	}
	if report.Ack == nil || report.ValidateEnvelope(delivery.input.Attempt) != nil {
		return nil, errs.New(errs.KindStateConflict, "Release cursor has no durable Controller acknowledgement")
	}
	return report, nil
}

func (delivery *Release) deliver(ctx context.Context, report releaseexecution.CheckpointReport) error {
	if report.Ack != nil {
		return nil
	}
	request, err := releaseexecution.EncodeCheckpointRequest(delivery.input, report)
	if err != nil {
		return err
	}
	response, err := delivery.exchange(ctx, request)
	if err != nil {
		return err
	}
	ack, err := releaseexecution.DecodeCheckpointAck(delivery.input, report, response)
	if err != nil {
		return err
	}
	return delivery.journal.AcknowledgeRelease(ctx, ack)
}
