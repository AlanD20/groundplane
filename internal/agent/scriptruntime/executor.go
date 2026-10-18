package scriptruntime

import (
	"context"
	"encoding/hex"
	"errors"
	"time"

	"github.com/AlanD20/groundplane/internal/agent/checkpointdelivery"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/agentworkjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Exchange interface {
	Start(context.Context, *agentpb.WorkStartRequest) (*agentpb.WorkStartAck, error)
	Script(workexecution.Attempt) (checkpointdelivery.ScriptExchange, error)
	CompleteScript(context.Context, *agentpb.ScriptWorkCompletionRequest) (*agentpb.ScriptWorkCompletionAck, error)
}

type Executor struct {
	runtime *Runtime
	journal *agentworkjournal.Journal
}

func NewExecutor(runtime *Runtime, journal *agentworkjournal.Journal) (*Executor, error) {
	if runtime == nil || journal == nil {
		return nil, errs.New(errs.KindInternal, "Script executor requires runtime and durable journal")
	}
	return &Executor{runtime: runtime, journal: journal}, nil
}

// Execute owns invocation plaintext. A returned acknowledgement means the
// Controller saved the outcome, not necessarily that the command succeeded.
func (executor *Executor) Execute(ctx context.Context, invocation Invocation, exchange Exchange) (*agentpb.ScriptWorkCompletionAck, error) {
	defer invocation.Clear()
	if executor == nil || ctx == nil || exchange == nil {
		return nil, errs.New(errs.KindInternal, "Script execution is not configured")
	}
	completed, err := executor.journal.LoadScriptCompletion(ctx, invocation.Attempt)
	if err != nil {
		return nil, err
	}
	if completed != nil {
		if completed.Receipt == nil {
			return executor.complete(ctx, completed.Record, exchange)
		}
		identity, err := workexecution.EncodeAgentAttempt(completed.Record.Attempt)
		if err != nil {
			return nil, err
		}
		digest, err := hex.DecodeString(completed.Receipt.Result.SHA256)
		if err != nil {
			return nil, errs.Wrap(errs.KindInternal, err)
		}
		return &agentpb.ScriptWorkCompletionAck{Attempt: identity, CheckpointSequence: completed.Record.Report.Sequence,
			CheckpointEvidence: &agentpb.WorkInputReference{InputId: completed.Receipt.Result.ID, Sha256: digest},
			Outcome:            string(completed.Receipt.Outcome), FinishedAt: timestamppb.New(*completed.Receipt.FinishedAt)}, nil
	}
	claimed, err := workexecution.EncodeAgentAttempt(invocation.Attempt)
	if err != nil {
		return nil, err
	}
	control, cancelControl := context.WithTimeout(ctx, 30*time.Second)
	defer cancelControl()
	start, err := exchange.Start(control, &agentpb.WorkStartRequest{Attempt: claimed})
	if err != nil {
		return nil, err
	}
	started, _, err := workexecution.DecodeAgentStartAck(start, invocation.Attempt)
	if err != nil {
		return nil, err
	}
	invocation.Attempt.StartedAt = &started
	sequence, err := executor.journal.Sequence(control, invocation.Attempt)
	if err != nil {
		return nil, err
	}
	if sequence < start.CheckpointSequence {
		return nil, errs.New(errs.KindStateConflict, "Controller Script checkpoint is missing from the Agent journal")
	}
	scriptExchange, err := exchange.Script(invocation.Attempt)
	if err != nil {
		return nil, err
	}
	delivery, err := checkpointdelivery.NewScript(invocation.Attempt, executor.journal, scriptExchange)
	if err != nil {
		return nil, err
	}
	if err := delivery.Resume(control); err != nil {
		return nil, err
	}
	record, err := executor.journal.Script(control, invocation.Attempt.ID)
	if err != nil {
		return nil, err
	}
	invocation.Checkpoint, err = resumeCheckpoint(invocation, record)
	if err != nil {
		return nil, err
	}
	bounded, cancel := context.WithDeadline(ctx, invocation.Attempt.Deadline)
	_, runErr := executor.runtime.ExecuteScript(bounded, invocation, delivery.Checkpoint)
	cancel()
	settlement, cancelSettlement := context.WithTimeout(ctx, 30*time.Second)
	defer cancelSettlement()
	record, err = executor.journal.Script(settlement, invocation.Attempt.ID)
	if err != nil {
		return nil, errors.Join(runErr, err)
	}
	if record == nil || record.Validate() != nil || record.Ack == nil ||
		!workexecution.SameAssignment(record.Attempt, invocation.Attempt) ||
		record.Report.ScriptExecutionId != invocation.Run.ScriptExecutionId ||
		record.Report.State != agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_CLEANUP_PROVEN {
		return nil, errors.Join(runErr, errs.New(errs.KindStateConflict, "Script has no acknowledged cleanup; retain its attempt for reconciliation"))
	}
	return executor.complete(ctx, *record, exchange)
}

func (executor *Executor) complete(ctx context.Context, record agentworkjournal.ScriptRecord, exchange Exchange) (*agentpb.ScriptWorkCompletionAck, error) {
	claimed, err := workexecution.EncodeAgentAttempt(record.Attempt)
	if err != nil {
		return nil, err
	}
	settlement, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	request := &agentpb.ScriptWorkCompletionRequest{Attempt: claimed, CheckpointSequence: record.Report.Sequence,
		CheckpointEvidence: &agentpb.WorkInputReference{InputId: record.Ack.EvidenceId, Sha256: append([]byte(nil), record.Ack.EvidenceSha256...)}}
	ack, err := exchange.CompleteScript(settlement, request)
	if err != nil {
		return nil, err
	}
	if _, err := executor.journal.AcknowledgeScriptCompletion(settlement, record.Attempt, ack); err != nil {
		return nil, err
	}
	return ack, nil
}
