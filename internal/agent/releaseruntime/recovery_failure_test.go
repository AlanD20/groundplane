package releaseruntime

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"slices"
	"testing"

	"github.com/AlanD20/groundplane/internal/agent/checkpointdelivery"
	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/agentworkjournal"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// TASK-95 / SVC-15: a failed restorative action or final verification must
// stop the actual recovery loop before failure hooks. Explicit reentry inspects
// the same in-flight boundary; it cannot erase the original failure, repeat a
// mutation automatically or manufacture terminal recovery. Host proofs before
// the injected fault and Controller acknowledgements are supplied.
func TestNativeRecoveryFailureBlocksHooksAndRetainsItsBoundary(t *testing.T) {
	for _, phase := range []releaseexecution.RecoveryPhase{
		releaseexecution.RestoreServingRuntime, releaseexecution.VerifyRestoration,
	} {
		t.Run(string(phase), func(t *testing.T) {
			input := failureHookInput(t, false)
			original := input.Failure
			journal, err := agentworkjournal.Open(filepath.Join(t.TempDir(), "journal"))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := journal.Close(); err != nil {
					t.Error(err)
				}
			})
			checkpoints, err := checkpointdelivery.NewRecovery(
				input,
				journal,
				func(_ context.Context, request *agentpb.ReleaseRecoveryCheckpointRequest) (*agentpb.ReleaseRecoveryCheckpointAck, error) {
					report, err := releaseexecution.DecodeRecoveryRequest(input, request)
					if err != nil {
						return nil, err
					}
					return releaseexecution.EncodeRecoveryAck(input, report)
				},
			)
			if err != nil {
				t.Fatal(err)
			}
			evidence, err := checkpointdelivery.NewRecoveryEffect(
				input,
				journal,
				func(ctx context.Context, _ *agentpb.WorkAttemptIdentity, raw []byte, reference workexecution.Input) (workexecution.Input, error) {
					current, err := checkpoints.Current(ctx)
					if err != nil {
						return workexecution.Input{}, err
					}
					if _, err := releaseexecution.DecodeRecoveryEffect(input, *current, raw, reference); err != nil {
						return workexecution.Input{}, err
					}
					return reference, nil
				},
			)
			if err != nil {
				t.Fatal(err)
			}
			effects := &nativeFailedRestoration{t: t, evidence: evidence, phase: phase,
				failure: errors.New("injected unproven restoration")}
			first, err := RunRecovery(t.Context(), input, checkpoints, effects)
			if !errors.Is(err, effects.failure) || first == nil || first.Ack == nil ||
				!first.Progress.InFlight || first.Progress.Phase != phase ||
				slices.Contains(effects.executed, releaseexecution.FailureHooks) {
				t.Fatalf("failed restoration advanced recovery or reached hooks: %v", err)
			}
			before := slices.Clone(effects.executed)
			again, err := RunRecovery(t.Context(), input, checkpoints, effects)
			if !errors.Is(err, effects.failure) || again == nil ||
				again.StageID != first.StageID || again.Sequence != first.Sequence ||
				!slices.Equal(
					effects.executed,
					before,
				) || !slices.Equal(effects.inspected, []releaseexecution.RecoveryPhase{phase}) ||
				!reflect.DeepEqual(input.Failure, original) || input.Forward.Outcome != workexecution.Failed {
				t.Fatalf("reentry replayed mutation, changed the boundary or lost the original failure: %v", err)
			}
		})
	}
}

type nativeFailedRestoration struct {
	t         *testing.T
	evidence  *checkpointdelivery.RecoveryEffect
	phase     releaseexecution.RecoveryPhase
	failure   error
	executed  []releaseexecution.RecoveryPhase
	inspected []releaseexecution.RecoveryPhase
}

func (effects *nativeFailedRestoration) ExecuteRecovery(ctx context.Context, input releaseexecution.RecoveryInput,
	report releaseexecution.RecoveryReport,
) (workexecution.Input, error) {
	effects.executed = append(effects.executed, report.Progress.Phase)
	if report.Progress.Phase == effects.phase {
		return workexecution.Input{}, effects.failure
	}
	return effects.evidence.Record(ctx, report, suppliedHookPrecondition(effects.t, input, report))
}

func (effects *nativeFailedRestoration) InspectRecovery(_ context.Context, _ releaseexecution.RecoveryInput,
	report releaseexecution.RecoveryReport,
) (workexecution.Input, error) {
	effects.inspected = append(effects.inspected, report.Progress.Phase)
	return workexecution.Input{}, effects.failure
}
