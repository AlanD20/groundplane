package releaseruntime

import (
	"context"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/agent/checkpointdelivery"
	"github.com/AlanD20/groundplane/internal/agent/scriptruntime"
	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/agentworkjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// HookLoader loads the exact captured Script input and its latest Controller
// checkpoint, after pending local reports have been replayed. Returned plaintext
// buffers are owned by the caller and cleared when the invocation finishes.
type HookLoader interface {
	LoadHook(context.Context, releaseexecution.Input, *agentpb.ReleaseWorkHook) (scriptruntime.Invocation, error)
}

type Hooks struct {
	runtime  *scriptruntime.Runtime
	journal  *agentworkjournal.Journal
	loader   HookLoader
	exchange checkpointdelivery.ScriptExchange
}

func NewHooks(runtime *scriptruntime.Runtime, journal *agentworkjournal.Journal, loader HookLoader, exchange checkpointdelivery.ScriptExchange) (*Hooks, error) {
	if runtime == nil || journal == nil || loader == nil || exchange == nil {
		return nil, errs.New(errs.KindInternal, "Release hooks require Script runtime, journal, captured input loader and checkpoint exchange")
	}
	return &Hooks{runtime: runtime, journal: journal, loader: loader, exchange: exchange}, nil
}

func (hooks *Hooks) Execute(ctx context.Context, input releaseexecution.Input, report releaseexecution.CheckpointReport, hook *agentpb.ReleaseWorkHook) (workexecution.Input, error) {
	return hooks.run(ctx, input, report, hook)
}

// Inspect resumes the Script owner's durable state machine, not a new shell
// invocation. That runtime inspects the captured container and consumes its exit
// result if already exited; it never restarts an exited Script container.
func (hooks *Hooks) Inspect(ctx context.Context, input releaseexecution.Input, report releaseexecution.CheckpointReport, hook *agentpb.ReleaseWorkHook) (workexecution.Input, error) {
	return hooks.run(ctx, input, report, hook)
}

func (hooks *Hooks) run(ctx context.Context, input releaseexecution.Input, report releaseexecution.CheckpointReport, hook *agentpb.ReleaseWorkHook) (workexecution.Input, error) {
	if hooks == nil || ctx == nil || report.Ack == nil || report.ValidateEnvelope(input.Attempt) != nil {
		return workexecution.Input{}, errs.New(errs.KindStateConflict, "Release hook has no acknowledged boundary")
	}
	active, err := report.Progress.ActiveHook(input)
	if err != nil || !proto.Equal(active, hook) {
		return workexecution.Input{}, errs.New(errs.KindStateConflict, "Release hook differs from its captured selection")
	}
	bounded, cancel := context.WithDeadline(ctx, input.Attempt.Deadline)
	defer cancel()
	delivery, err := checkpointdelivery.NewScript(input.Attempt, hooks.journal, hooks.exchange)
	if err != nil {
		return workexecution.Input{}, err
	}
	if err := delivery.Resume(bounded); err != nil {
		return workexecution.Input{}, err
	}
	invocation, err := hooks.loader.LoadHook(bounded, input, active)
	defer clearHookInvocation(invocation)
	if err != nil {
		return workexecution.Input{}, err
	}
	expected := workexecution.Input{ID: active.Input.InputId, SHA256: hex.EncodeToString(active.Input.Sha256)}
	if !workexecution.SameAssignment(invocation.Attempt, input.Attempt) || invocation.OperationID != input.Release.OperationId || invocation.Input != expected ||
		invocation.Run == nil || invocation.Run.ScriptExecutionId != active.ExecutionId || invocation.Run.ScriptId != active.ScriptId ||
		invocation.Checkpoint == nil || invocation.Checkpoint.ScriptExecutionId != active.ExecutionId {
		return workexecution.Input{}, errs.New(errs.KindStateConflict, "loaded Script invocation differs from the captured Release hook")
	}
	if _, err := hooks.runtime.ExecuteScript(bounded, invocation, delivery.Checkpoint); err != nil {
		return workexecution.Input{}, err
	}
	record, err := hooks.journal.Script(bounded, input.Attempt.ID)
	if err != nil {
		return workexecution.Input{}, err
	}
	return HookCleanupReceipt(input, report, record)
}

func clearHookInvocation(invocation scriptruntime.Invocation) {
	if invocation.Body != nil {
		clear(invocation.Body.Body)
	}
	for _, entry := range invocation.Entries {
		if entry != nil {
			clear(entry.Value)
		}
	}
}
