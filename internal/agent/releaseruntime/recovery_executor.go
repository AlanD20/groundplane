package releaseruntime

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/agent/checkpointdelivery"
	"github.com/AlanD20/groundplane/internal/agent/materialization"
	"github.com/AlanD20/groundplane/internal/agent/scriptruntime"
	"github.com/AlanD20/groundplane/internal/common/environmentpath"
	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/agentownership"
	"github.com/AlanD20/groundplane/internal/infra/agentworkjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type RecoveryExchange interface {
	ConfigurationExchange
	Hook(context.Context, *agentpb.ReleaseHookRequest) (*agentpb.ReleaseHookDelivery, error)
	Start(context.Context, *agentpb.WorkStartRequest) (*agentpb.WorkStartAck, error)
	RecoveryCheckpoint(context.Context, *agentpb.ReleaseRecoveryCheckpointRequest) (*agentpb.ReleaseRecoveryCheckpointAck, error)
	EffectEvidence(context.Context, *agentpb.WorkAttemptIdentity, []byte, workexecution.Input) (workexecution.Input, error)
	CompleteRelease(context.Context, *agentpb.ReleaseWorkCompletionRequest) (*agentpb.ReleaseWorkCompletionAck, error)
	FailRelease(context.Context, *agentpb.ReleaseWorkFailureRequest) (*agentpb.ReleaseWorkFailureAck, error)
}

type RecoveryHostHelper interface {
	RecoveryHelper
	InterruptedReleaseHelper
	StopRecoveryHelper(context.Context, workexecution.Attempt) error
}

// RecoveryExecutor binds the captured cleanup to its own start receipt and
// durable journal. A final report is returned only after Controller settlement
// and local persistence of its receipt; transport loss replays that completion.
type RecoveryExecutor struct {
	volumeRoot   string
	journal      *agentworkjournal.Journal
	materializer *materialization.Runtime
	helper       RecoveryHostHelper
	ownership    *agentownership.Lease
	scripts      *scriptruntime.Runtime
}

func NewRecoveryExecutor(volumeRoot string, journal *agentworkjournal.Journal, materializer *materialization.Runtime, scripts *scriptruntime.Runtime, helper RecoveryHostHelper, ownership *agentownership.Lease) (*RecoveryExecutor, error) {
	if environmentpath.ValidateRoot(volumeRoot) != nil || journal == nil || materializer == nil || scripts == nil || helper == nil {
		return nil, errs.New(errs.KindInternal, "Release recovery executor dependencies are incomplete")
	}
	if err := ownership.Check(); err != nil {
		return nil, err
	}
	return &RecoveryExecutor{volumeRoot: volumeRoot, journal: journal, materializer: materializer, scripts: scripts, helper: helper, ownership: ownership}, nil
}

func (executor *RecoveryExecutor) execute(ctx context.Context, input releaseexecution.RecoveryInput, exchange RecoveryExchange) (*releaseexecution.RecoveryReport, error) {
	if executor == nil || ctx == nil || exchange == nil {
		return nil, errs.New(errs.KindInternal, "Release recovery execution is not configured")
	}
	// A completed local cursor can be redelivered after the effect deadline.
	// It grants no new effects and must not be replaced by a fresh start.
	input, saved, err := executor.journal.LoadRecovery(ctx, input)
	if err != nil {
		return nil, err
	}
	if saved != nil && saved.Progress.Phase == releaseexecution.RecoveryComplete {
		return executor.resumeCompleted(ctx, input, *saved, exchange)
	}
	bounded, cancel := context.WithDeadline(ctx, input.Input.Attempt.Deadline)
	defer cancel()
	if err := bounded.Err(); err != nil {
		return nil, err
	}
	claimed, err := workexecution.EncodeAgentAttempt(input.Input.Attempt)
	if err != nil {
		return nil, err
	}
	start, err := exchange.Start(bounded, &agentpb.WorkStartRequest{Attempt: claimed})
	if err != nil {
		return nil, err
	}
	input.Input, _, err = input.Input.WithStart(start)
	if err != nil {
		return nil, err
	}
	sequence, err := executor.journal.Sequence(bounded, input.Input.Attempt)
	if err != nil {
		return nil, err
	}
	if sequence < start.CheckpointSequence {
		return nil, errs.New(errs.KindStateConflict, "Controller has recovery checkpoints missing from the Agent journal; reconcile the saved attempt before further effects")
	}
	if err := executor.ownership.CheckRestoration(input); err != nil {
		return nil, err
	}
	if err := executor.helper.StopRecoveryHelper(bounded, input.Input.Attempt); err != nil {
		return nil, err
	}
	checkpoints, err := checkpointdelivery.NewRecovery(input, executor.journal, exchange.RecoveryCheckpoint)
	if err != nil {
		return nil, err
	}
	evidence, err := checkpointdelivery.NewRecoveryEffect(input, executor.journal, exchange.EffectEvidence)
	if err != nil {
		return nil, err
	}
	source, err := NewChannelConfigurationSource(exchange)
	if err != nil {
		return nil, err
	}
	configuration, err := NewRecoveryConfiguration(executor.materializer, source, executor.ownership)
	if err != nil {
		return nil, err
	}
	hooks, err := NewHookLoader(exchange.Hook)
	if err != nil {
		return nil, err
	}
	quiescence, err := NewRecoveryQuiescence(executor.ownership, executor.helper, executor.materializer, executor.scripts, hooks)
	if err != nil {
		return nil, err
	}
	driver, err := NewRecoveryDriver(quiescence, configuration, executor.helper, evidence)
	if err != nil {
		return nil, err
	}
	report, err := RunRecovery(ctx, input, checkpoints, driver)
	if err != nil {
		return nil, err
	}
	return executor.resumeCompleted(ctx, input, *report, exchange)
}

func (executor *RecoveryExecutor) resumeCompleted(ctx context.Context, input releaseexecution.RecoveryInput, report releaseexecution.RecoveryReport, exchange RecoveryExchange) (*releaseexecution.RecoveryReport, error) {
	if report.Ack != nil {
		return executor.completeRecovery(ctx, input, report, exchange)
	}
	request, err := releaseexecution.EncodeRecoveryRequest(input, report)
	if err != nil {
		return nil, err
	}
	bounded, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	response, err := exchange.RecoveryCheckpoint(bounded, request)
	if err != nil {
		return nil, err
	}
	ack, err := releaseexecution.DecodeRecoveryAck(input, report, response)
	if err != nil {
		return nil, err
	}
	if err := executor.journal.AcknowledgeRecovery(bounded, ack); err != nil {
		return nil, err
	}
	report.Ack = &ack
	return executor.completeRecovery(ctx, input, report, exchange)
}

func (executor *RecoveryExecutor) completeRecovery(ctx context.Context, input releaseexecution.RecoveryInput, report releaseexecution.RecoveryReport, exchange RecoveryExchange) (*releaseexecution.RecoveryReport, error) {
	bounded, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	receipt, err := executor.journal.RecoveryCompletion(bounded, input)
	if err != nil {
		return nil, err
	}
	if receipt != nil {
		return &report, nil
	}
	request, err := releaseexecution.EncodeRecoveryCompletionRequest(input, report)
	if err != nil {
		return nil, err
	}
	ack, err := exchange.CompleteRelease(bounded, request)
	if err != nil {
		return nil, err
	}
	if err := executor.journal.AcknowledgeRecoveryCompletion(bounded, input, ack); err != nil {
		return nil, err
	}
	return &report, nil
}
