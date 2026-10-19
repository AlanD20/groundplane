package releaseruntime

import (
	"context"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/agent/materialization"
	"github.com/AlanD20/groundplane/internal/agent/scriptruntime"
	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/agentownership"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type InterruptedReleaseHelper interface {
	StopRestorationHelpers(context.Context, releaseexecution.RecoveryInput) error
}

type RecoveryQuiescence struct {
	ownership *agentownership.Lease
	helper    InterruptedReleaseHelper
	files     *materialization.Runtime
	scripts   *scriptruntime.Runtime
	hooks     *ChannelHookLoader
}

func NewRecoveryQuiescence(
	ownership *agentownership.Lease,
	helper InterruptedReleaseHelper,
	files *materialization.Runtime,
	scripts *scriptruntime.Runtime,
	hooks *ChannelHookLoader,
) (*RecoveryQuiescence, error) {
	if err := ownership.Check(); err != nil {
		return nil, err
	}
	if helper == nil || files == nil || scripts == nil || hooks == nil {
		return nil, errs.New(errs.KindInternal, "Release recovery requires interrupted helper cleanup")
	}
	return &RecoveryQuiescence{ownership: ownership, helper: helper, files: files, scripts: scripts, hooks: hooks}, nil
}

// Observe can resume an interrupted quiescence boundary: stopping an exactly
// identified helper is safe to repeat, whereas a workload mutation is not.
// Inspection of accepted Docker effects remains a separate recovery action.
func (quiescence *RecoveryQuiescence) Observe(
	ctx context.Context,
	input releaseexecution.RecoveryInput,
	report releaseexecution.RecoveryReport,
) (releaseexecution.QuiescenceProof, error) {
	if quiescence == nil || ctx == nil || report.Validate(input) != nil || report.Ack == nil ||
		!report.Progress.InFlight || report.Progress.Phase != releaseexecution.QuiesceForward {
		return releaseexecution.QuiescenceProof{}, errs.New(
			errs.KindStateConflict,
			"Release quiescence requires an acknowledged recovery boundary",
		)
	}
	bounded, cancel := context.WithDeadline(ctx, input.Input.Attempt.Deadline)
	defer cancel()
	if err := bounded.Err(); err != nil {
		return releaseexecution.QuiescenceProof{}, err
	}
	if err := quiescence.ownership.CheckRestoration(input); err != nil {
		return releaseexecution.QuiescenceProof{}, err
	}
	if err := quiescence.helper.StopRestorationHelpers(bounded, input); err != nil {
		return releaseexecution.QuiescenceProof{}, err
	}
	if err := quiescence.files.StopForwardFiles(bounded, input, report); err != nil {
		return releaseexecution.QuiescenceProof{}, err
	}
	if err := quiescence.files.StopInterruptedRestorationFiles(bounded, input, report); err != nil {
		return releaseexecution.QuiescenceProof{}, err
	}
	proof := releaseexecution.QuiescenceProof{
		ForwardAttemptID:  input.Forward.ID,
		RecoveryAttemptID: input.Input.Attempt.ID,
		Executor:          input.Input.Attempt.Executor,
		HelperAbsentAt:    time.Now().UTC(),
	}
	if input.Interruption != nil {
		proof.InterruptedAttemptID = input.Interruption.Attempt.ID
	}
	for _, hook := range input.Input.Release.Hooks {
		captured, err := quiescence.hooks.LoadRecoveryHook(bounded, input, hook)
		if err != nil {
			return releaseexecution.QuiescenceProof{}, err
		}
		stopped, err := quiescence.scripts.QuiesceHook(bounded, input, report, hook, captured.Projection)
		if err != nil {
			return releaseexecution.QuiescenceProof{}, err
		}
		if err := quiescence.ownership.CheckRestoration(input); err != nil {
			return releaseexecution.QuiescenceProof{}, err
		}
		cleanup, err := quiescence.scripts.CleanupHook(
			bounded,
			input,
			report,
			hook,
			captured.Projection,
			captured.Artifacts,
		)
		if err != nil {
			return releaseexecution.QuiescenceProof{}, err
		}
		proof.Hooks = append(proof.Hooks, releaseexecution.HookQuiescenceProof{
			Cleanup:     cleanup,
			ExecutionID: hook.ExecutionId, Input: workexecution.Input{ID: hook.Input.InputId, SHA256: hex.EncodeToString(hook.Input.Sha256)},
			ContainerID: stopped.ContainerID, Absent: true, ObservedAt: time.Now().UTC(),
		})
	}
	if err := quiescence.ownership.CheckRestoration(input); err != nil {
		return releaseexecution.QuiescenceProof{}, err
	}
	proof.OwnerCheckedAt = time.Now().UTC()
	if err := proof.Validate(input, report); err != nil {
		return releaseexecution.QuiescenceProof{}, err
	}
	return proof, nil
}
