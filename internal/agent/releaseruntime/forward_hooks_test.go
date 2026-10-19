package releaseruntime

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/AlanD20/groundplane/internal/agent/checkpointdelivery"
	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// TASK-96 / SCRIPT-06: a real forward pre-hook must finish its Script cleanup
// and receive the exact receipt before the Release can reach workload rollout.
// On explicit reentry, retry cleanup/report delivery, never the command. The
// configuration phase and Controller replies are supplied, not qualified here.
func TestNativeForwardHookBlocksRolloutUntilCleanupIsAcknowledged(t *testing.T) {
	for _, loseAck := range []bool{false, true} {
		f := newFailureHookFixture(t, true)
		input := f.input.Input
		input.Release = proto.CloneOf(input.Release)
		input.Attempt = f.input.Forward
		input.Attempt.FinishedAt, input.Attempt.Result, input.Attempt.Outcome = nil, nil, ""
		input.Release.Hooks[0].Phase = "pre-deploy"
		identity, err := workexecution.EncodeAgentAttempt(input.Attempt)
		if err != nil {
			t.Fatal(err)
		}
		f.delivery.Attempt = identity
		f.engine.before = func(state agentpb.ScriptExecutionState) {
			record, err := f.journal.Script(t.Context(), input.Attempt.ID)
			if err != nil || record == nil || record.Ack == nil || record.Report.State != state {
				t.Fatalf("forward hook effect crossed an unacknowledged Script boundary: %v", err)
			}
		}
		if loseAck {
			f.loseCleanupAck = true
		} else {
			f.engine.cleanupErr = errors.New("hook cleanup is incomplete")
		}
		checkpoints, err := checkpointdelivery.NewRelease(
			input,
			f.journal,
			func(_ context.Context, request *agentpb.ReleaseWorkCheckpointRequest) (*agentpb.ReleaseWorkCheckpointAck, error) {
				report, err := releaseexecution.DecodeCheckpointRequest(input, request)
				if err != nil {
					return nil, err
				}
				return releaseexecution.EncodeCheckpointAck(input, report)
			},
		)
		if err != nil {
			t.Fatal(err)
		}
		effects := &nativeForwardHookEffects{
			hooks:       f.hooks(t),
			rolloutStop: errors.New("stop before supplied workload effects"),
		}
		report, err := Run(t.Context(), input, checkpoints, effects)
		if err == nil || report == nil || report.Progress.Phase != releaseexecution.PreHooks ||
			!report.Progress.InFlight || effects.rollouts != 0 ||
			!slices.Equal(f.engine.events, []string{"prepare", "recover", "create", "run", "cleanup"}) {
			t.Fatalf("rollout advanced before the hook cleanup receipt: %v", err)
		}
		if loseAck && !errors.Is(err, errHookCleanupAckLost) || !loseAck && !errors.Is(err, f.engine.cleanupErr) {
			t.Fatalf("original hook failure was hidden: %v", err)
		}
		f.engine.cleanupErr = nil
		report, err = Run(t.Context(), input, checkpoints, effects)
		if !errors.Is(err, effects.rolloutStop) || report == nil || report.Progress.Phase != releaseexecution.Rollout ||
			effects.rollouts != 1 {
			t.Fatalf("explicit resume did not reach rollout after acknowledged cleanup: %v", err)
		}
		want := []string{"prepare", "recover", "create", "run", "cleanup"}
		if !loseAck {
			want = append(want, "cleanup")
		}
		if !slices.Equal(f.engine.events, want) {
			t.Fatalf("forward hook command/cleanup replayed unexpectedly: %v", f.engine.events)
		}
		if loseAck && f.cleanupSends != 2 {
			t.Fatal("lost Script acknowledgement was not replayed exactly once")
		}
	}
}

type nativeForwardHookEffects struct {
	hooks       *Hooks
	rolloutStop error
	rollouts    int
}

func (effects *nativeForwardHookEffects) Execute(ctx context.Context, input releaseexecution.Input,
	report releaseexecution.CheckpointReport,
) (workexecution.Input, error) {
	if report.Progress.Phase == releaseexecution.Configuration {
		return input.Attempt.Input, nil // Earlier configuration proof is supplied.
	}
	if report.Progress.Phase == releaseexecution.PreHooks {
		hook, err := report.Progress.ActiveHook(input)
		if err != nil {
			return workexecution.Input{}, err
		}
		return effects.hooks.Execute(ctx, input, report, hook)
	}
	effects.rollouts++
	return workexecution.Input{}, effects.rolloutStop
}

func (effects *nativeForwardHookEffects) Inspect(ctx context.Context, input releaseexecution.Input,
	report releaseexecution.CheckpointReport,
) (workexecution.Input, error) {
	hook, err := report.Progress.ActiveHook(input)
	if err != nil {
		return workexecution.Input{}, err
	}
	return effects.hooks.Inspect(ctx, input, report, hook)
}
