package scriptruntime

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/scriptexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// QuiesceHook is deliberately separate from ExecuteScript: cleanup cannot
// prepare a body, create/start a container, or replay an arbitrary hook.
// The caller supplies the authenticated captured projection and holds process
// ownership throughout this operation.
func (runtime *Runtime) QuiesceHook(
	ctx context.Context,
	input releaseexecution.RecoveryInput,
	report releaseexecution.RecoveryReport,
	hook *agentpb.ReleaseWorkHook,
	projection *agentpb.ScriptRunnerProjection,
) (scriptexecution.QuiescenceProof, error) {
	if err := runtime.validateRecoveryHook(ctx, input, report, hook, projection); err != nil {
		return scriptexecution.QuiescenceProof{}, err
	}
	bounded, cancel := context.WithDeadline(ctx, input.Input.Attempt.Deadline)
	defer cancel()
	return runtime.engine.Quiesce(bounded, scriptexecution.QuiescenceRequest{
		TaskID: input.Forward.TaskID, WorkItemID: input.Forward.WorkItemID, OperationID: input.Input.Release.OperationId,
		ExecutionID: hook.ExecutionId, Projection: projection,
	})
}

func (runtime *Runtime) validateRecoveryHook(
	ctx context.Context,
	input releaseexecution.RecoveryInput,
	report releaseexecution.RecoveryReport,
	hook *agentpb.ReleaseWorkHook,
	projection *agentpb.ScriptRunnerProjection,
) error {
	if runtime == nil || runtime.engine == nil || ctx == nil || report.Validate(input) != nil || report.Ack == nil ||
		!report.Progress.InFlight || report.Progress.Phase != releaseexecution.QuiesceForward || hook == nil || projection == nil {
		return errs.New(errs.KindStateConflict, "Script cleanup requires an acknowledged recovery boundary")
	}
	selected := false
	for _, captured := range input.Input.Release.Hooks {
		if proto.Equal(captured, hook) {
			selected = true
			break
		}
	}
	if !selected {
		return errs.New(errs.KindStateConflict, "Script cleanup is outside the captured Release hooks")
	}
	return ctx.Err()
}
