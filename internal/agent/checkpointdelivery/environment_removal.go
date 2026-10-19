package checkpointdelivery

import (
	"context"
	"sync"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/hierarchyexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/agentworkjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type EnvironmentRemovalExchange func(context.Context, *agentpb.EnvironmentRemovalCheckpointRequest) (*agentpb.EnvironmentRemovalCheckpointAck, error)

type EnvironmentRemoval struct {
	mu       sync.Mutex
	input    hierarchyexecution.EnvironmentInput
	journal  *agentworkjournal.Journal
	exchange EnvironmentRemovalExchange
}

func NewEnvironmentRemoval(input hierarchyexecution.EnvironmentInput, journal *agentworkjournal.Journal,
	exchange EnvironmentRemovalExchange,
) (*EnvironmentRemoval, error) {
	if input.Attempt.Validate() != nil || input.Attempt.Executor.Executor != workexecution.Agent ||
		input.Validate() != nil || input.Attempt.StartedAt == nil || input.Attempt.FinishedAt != nil ||
		executionplan.ValidateEnvironmentRemovalInput(input.Removal) != nil || journal == nil || exchange == nil {
		return nil, errs.New(
			errs.KindValidationFailed,
			"Environment removal reporting requires a started Agent attempt",
		)
	}
	return &EnvironmentRemoval{input: input, journal: journal, exchange: exchange}, nil
}

func (delivery *EnvironmentRemoval) Checkpoint(ctx context.Context, next hierarchyexecution.EnvironmentProgress) error {
	delivery.mu.Lock()
	defer delivery.mu.Unlock()
	record, err := delivery.journal.PrepareEnvironmentRemoval(ctx, delivery.input, next)
	if err != nil {
		return err
	}
	return delivery.deliver(ctx, *record)
}

// Resume only replays a saved report. The runtime must inspect an in-flight
// deletion before deciding whether any further host action is permitted.
func (delivery *EnvironmentRemoval) Resume(ctx context.Context) error {
	delivery.mu.Lock()
	defer delivery.mu.Unlock()
	record, err := delivery.journal.EnvironmentRemoval(ctx, delivery.input)
	if err != nil || record == nil {
		return err
	}
	return delivery.deliver(ctx, *record)
}

func (delivery *EnvironmentRemoval) Current(ctx context.Context) (*hierarchyexecution.EnvironmentProgress, error) {
	delivery.mu.Lock()
	defer delivery.mu.Unlock()
	record, err := delivery.journal.EnvironmentRemoval(ctx, delivery.input)
	if err != nil || record == nil {
		return nil, err
	}
	if !record.Acknowledged {
		return nil, errs.New(
			errs.KindStateConflict,
			"Environment removal progress has not been acknowledged by the Controller",
		)
	}
	return &record.Report.Progress, nil
}

func (delivery *EnvironmentRemoval) deliver(
	ctx context.Context,
	record agentworkjournal.EnvironmentRemovalRecord,
) error {
	if record.Acknowledged {
		return nil
	}
	request, err := hierarchyexecution.EncodeEnvironmentCheckpointRequest(delivery.input, record.Report)
	if err != nil {
		return err
	}
	ack, err := delivery.exchange(ctx, request)
	if err != nil {
		return err
	}
	return delivery.journal.AcknowledgeEnvironmentRemoval(ctx, delivery.input, ack)
}
