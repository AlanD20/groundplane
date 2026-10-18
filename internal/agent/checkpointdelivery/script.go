package checkpointdelivery

import (
	"context"
	"sync"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/agentworkjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// ScriptExchange sends an owned report over the authenticated native channel and
// returns its acknowledgement. Reconnect must not reconstruct the report.
type ScriptExchange func(context.Context, *agentpb.ScriptCheckpointRequest) (*agentpb.ScriptCheckpointAck, error)

type Script struct {
	mu       sync.Mutex
	attempt  workexecution.Attempt
	journal  *agentworkjournal.Journal
	exchange ScriptExchange
}

func NewScript(attempt workexecution.Attempt, journal *agentworkjournal.Journal, exchange ScriptExchange) (*Script, error) {
	if attempt.Validate() != nil || attempt.Executor.Executor != workexecution.Agent ||
		attempt.FinishedAt != nil || journal == nil || exchange == nil {
		return nil, errs.New(errs.KindValidationFailed, "Script checkpoint delivery requires an active Agent attempt")
	}
	return &Script{attempt: attempt, journal: journal, exchange: exchange}, nil
}

// Checkpoint is the Script runtime callback. Storage failures stop dependent
// effects; transport failures leave the exact pending report available to Resume.
func (delivery *Script) Checkpoint(ctx context.Context, request *agentpb.ScriptCheckpointRequest) error {
	if ctx == nil || delivery == nil {
		return errs.New(errs.KindInternal, "Script checkpoint delivery is not configured")
	}
	delivery.mu.Lock()
	defer delivery.mu.Unlock()
	record, err := delivery.journal.PrepareScript(ctx, delivery.attempt, request, time.Now().UTC())
	if err != nil {
		return err
	}
	return delivery.deliver(ctx, record)
}

// Resume only delivers a previously persisted report. It does not invoke the
// Script, prepare its files, create a container, or grant a new attempt.
func (delivery *Script) Resume(ctx context.Context) error {
	if ctx == nil || delivery == nil {
		return errs.New(errs.KindInternal, "Script checkpoint delivery is not configured")
	}
	delivery.mu.Lock()
	defer delivery.mu.Unlock()
	record, err := delivery.journal.Script(ctx, delivery.attempt.ID)
	if err != nil || record == nil {
		return err
	}
	if !workexecution.SameAssignment(record.Attempt, delivery.attempt) {
		return errs.New(errs.KindStateConflict, "Script report belongs to another execution")
	}
	return delivery.deliver(ctx, *record)
}

func (delivery *Script) deliver(ctx context.Context, record agentworkjournal.ScriptRecord) error {
	if record.Ack != nil {
		return nil
	}
	ack, err := delivery.exchange(ctx, proto.Clone(record.Report).(*agentpb.ScriptCheckpointRequest))
	if err != nil {
		return err
	}
	return delivery.journal.AcknowledgeScript(ctx, ack)
}
