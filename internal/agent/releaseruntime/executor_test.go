package releaseruntime

import (
	"errors"
	"slices"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"google.golang.org/protobuf/proto"
)

// TASK-98: drive the public native Executor through real decoding, disk journal,
// checkpoint/effect delivery, readiness and terminal receipt. A simulated host
// changes independently of transport replies; this is not real Docker or the
// production Controller. No files/hooks are declared in this first-Deploy case.
func TestNativeReleaseExecutorPreservesEffectsAndTerminalReplay(t *testing.T) {
	for _, scenario := range []string{"success", "completion ACK lost", "startup result lost", "Start rejected", "checkpoint rejected", "stale Start acknowledgement", "stale checkpoint acknowledgement"} {
		t.Run(scenario, func(t *testing.T) {
			f := newNativeReleaseExecutorFixture(t, scenario)
			result, err := f.executor.Execute(t.Context(), proto.CloneOf(f.assignment), f.input.Attempt.Executor, f)
			if scenario != "success" && scenario != "completion ACK lost" {
				if err != nil || result.Failure == nil || result.Report != nil ||
					result.Failure.Outcome != workexecution.Failed ||
					f.completions != 0 {
					t.Fatalf("native failure was hidden or became completion: %#v, %v", result, err)
				}
				wantStarts := 0
				if scenario == "startup result lost" {
					wantStarts = 1
				}
				if f.hostStarts != wantStarts || f.present != (wantStarts == 1) || f.observations != 0 {
					t.Fatal("a refused/uncertain operation acquired new effects or claimed cleanup")
				}
			} else {
				if scenario == "completion ACK lost" {
					if !errors.Is(err, f.transport) {
						t.Fatalf("lost completion was reclassified: %v", err)
					}
				} else if err != nil {
					t.Fatal(err)
				}
				if result.Report == nil || result.Report.Progress.Phase != releaseexecution.Complete || result.Failure != nil ||
					!f.present || f.hostStarts != 1 || f.observations == 0 || f.failures != 0 ||
					!slices.Equal(f.hostActions, []releaseexecution.RolloutAction{
						releaseexecution.StartCandidate, releaseexecution.VerifyServing}) {
					t.Fatalf("native execution did not establish the simulated workload: %#v, %v; present=%v starts=%d observations=%d failures=%d actions=%v", result, err, f.present, f.hostStarts, f.observations, f.failures, f.hostActions)
				}
			}
			starts, checkpoints, mutations, failures, observations := f.starts, f.checkpoints, f.hostStarts, f.failures, f.observations
			if err := f.journal.Close(); err != nil {
				t.Fatal(err)
			}
			f.open()
			replayed, err := f.executor.Execute(t.Context(), proto.CloneOf(f.assignment), f.input.Attempt.Executor, f)
			if err != nil || f.starts != starts || f.checkpoints != checkpoints || f.hostStarts != mutations ||
				f.failures != failures || f.observations != observations || len(f.files.events) != 0 {
				t.Fatalf("terminal replay requested Start/checkpoints or repeated effects: %#v, %v", replayed, err)
			}
			if result.Failure != nil {
				if replayed.Failure == nil || replayed.Failure.Evidence != result.Failure.Evidence ||
					replayed.Failure.Validate(
						f.input.Attempt,
						result.Failure.Failure.Reason,
					) != nil || f.completions != 0 {
					t.Fatal("failure replay lost the original captured outcome")
				}
			} else if replayed.Report == nil || replayed.Report.Progress.Phase != releaseexecution.Complete {
				t.Fatal("completion replay did not retain its final cursor")
			}
		})
	}
}
