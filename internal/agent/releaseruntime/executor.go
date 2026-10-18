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
	"github.com/AlanD20/groundplane/internal/infra/agentworkjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Exchange belongs to one authenticated session; it never outlives reconnect.
// Host runtimes and the durable journal outlive that transport session.
type Exchange interface {
	ConfigurationExchange
	Start(context.Context, *agentpb.WorkStartRequest) (*agentpb.WorkStartAck, error)
	CompleteRelease(context.Context, *agentpb.ReleaseWorkCompletionRequest) (*agentpb.ReleaseWorkCompletionAck, error)
	FailRelease(context.Context, *agentpb.ReleaseWorkFailureRequest) (*agentpb.ReleaseWorkFailureAck, error)
	ReleaseCheckpoint(context.Context, *agentpb.ReleaseWorkCheckpointRequest) (*agentpb.ReleaseWorkCheckpointAck, error)
	Hook(context.Context, *agentpb.ReleaseHookRequest) (*agentpb.ReleaseHookDelivery, error)
	EffectEvidence(context.Context, *agentpb.WorkAttemptIdentity, []byte, workexecution.Input) (workexecution.Input, error)
	Script(workexecution.Attempt) (checkpointdelivery.ScriptExchange, error)
}

type Executor struct {
	volumeRoot   string
	journal      *agentworkjournal.Journal
	materializer *materialization.Runtime
	scripts      *scriptruntime.Runtime
	helper       MutationHelper
	observer     Observer
}

func NewExecutor(volumeRoot string, journal *agentworkjournal.Journal, materializer *materialization.Runtime, scripts *scriptruntime.Runtime, helper MutationHelper, observer Observer) (*Executor, error) {
	if environmentpath.ValidateRoot(volumeRoot) != nil || journal == nil || materializer == nil || scripts == nil || helper == nil || observer == nil {
		return nil, errs.New(errs.KindInternal, "native Release executor dependencies are incomplete")
	}
	return &Executor{volumeRoot: volumeRoot, journal: journal, materializer: materializer, scripts: scripts, helper: helper, observer: observer}, nil
}

// execute runs one assigned forward attempt. The caller owns exclusive scheduling
// of that attempt and subsequent result settlement/recovery. This method never
// retires the journal, allocates retries or turns a cleanup result into success.
func (executor *Executor) execute(ctx context.Context, input releaseexecution.Input, exchange Exchange) (*releaseexecution.CheckpointReport, error) {
	if executor == nil || ctx == nil || exchange == nil {
		return nil, errs.New(errs.KindInternal, "native Release execution is not configured")
	}
	if input.Attempt.Direction != workexecution.Forward {
		return nil, errs.New(errs.KindStateConflict, "recovery assignments require the Release recovery executor")
	}
	completed, err := executor.journal.LoadReleaseCompletion(ctx, input)
	if err != nil {
		return nil, err
	}
	if completed != nil {
		input.Attempt = completed.Attempt
		if completed.Receipt == nil {
			if err := executor.complete(ctx, input, completed.Report, exchange); err != nil {
				return nil, err
			}
		}
		return &completed.Report, nil
	}
	bounded, cancel := context.WithDeadline(ctx, input.Attempt.Deadline)
	defer cancel()
	if err := bounded.Err(); err != nil {
		return nil, err
	}
	claimed, err := workexecution.EncodeAgentAttempt(input.Attempt)
	if err != nil {
		return nil, err
	}
	start, err := exchange.Start(bounded, &agentpb.WorkStartRequest{Attempt: claimed})
	if err != nil {
		return nil, err
	}
	input, _, err = input.WithStart(start)
	if err != nil {
		return nil, err
	}
	sequence, err := executor.journal.Sequence(bounded, input.Attempt)
	if err != nil {
		return nil, err
	}
	if sequence < start.CheckpointSequence {
		return nil, errs.New(errs.KindStateConflict, "Controller has execution checkpoints missing from the Agent journal; reconcile the saved attempt before further effects")
	}
	checkpoints, err := checkpointdelivery.NewRelease(input, executor.journal, exchange.ReleaseCheckpoint)
	if err != nil {
		return nil, err
	}
	evidence, err := checkpointdelivery.NewEffect(input, executor.journal, exchange.EffectEvidence)
	if err != nil {
		return nil, err
	}
	source, err := NewChannelConfigurationSource(exchange)
	if err != nil {
		return nil, err
	}
	configuration, err := NewConfiguration(executor.materializer, source, evidence)
	if err != nil {
		return nil, err
	}
	loader, err := NewHookLoader(exchange.Hook)
	if err != nil {
		return nil, err
	}
	scriptExchange, err := exchange.Script(input.Attempt)
	if err != nil {
		return nil, err
	}
	hooks, err := NewHooks(executor.scripts, executor.journal, loader, scriptExchange)
	if err != nil {
		return nil, err
	}
	driver, err := NewDriver(configuration, hooks, executor.helper, executor.observer, evidence)
	if err != nil {
		return nil, err
	}
	report, err := Run(bounded, input, checkpoints, driver)
	if err != nil {
		return report, err
	}
	if report == nil {
		return nil, errs.New(errs.KindStateConflict, "Release execution returned no completed checkpoint")
	}
	if err := executor.complete(ctx, input, *report, exchange); err != nil {
		return report, err
	}
	return report, nil
}

// Settlement may be delivered after the effect deadline; it cannot grant new
// effects. A bounded transport failure leaves the completed journal for replay.
func (executor *Executor) complete(ctx context.Context, input releaseexecution.Input, report releaseexecution.CheckpointReport, exchange Exchange) error {
	request, err := releaseexecution.EncodeCompletionRequest(input, report)
	if err != nil {
		return err
	}
	bounded, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	ack, err := exchange.CompleteRelease(bounded, request)
	if err != nil {
		return err
	}
	_, err = executor.journal.AcknowledgeReleaseCompletion(bounded, input, ack)
	return err
}
