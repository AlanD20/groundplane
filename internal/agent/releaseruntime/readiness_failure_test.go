package releaseruntime

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/jcs"
	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"google.golang.org/protobuf/proto"
)

// A known failed observation survives lost upload and process restart. Its
// saved proof is replayed before failure settlement, without restarting or
// re-observing the workload, and cannot authorize a successful checkpoint.
func TestReadinessFailureSurvivesUploadLossAndJournalReopen(t *testing.T) {
	f := newNativeReleaseExecutorFixture(t, "readiness upload fails")
	result, err := f.executor.Execute(t.Context(), proto.CloneOf(f.assignment), f.input.Attempt.Executor, f)
	if !errors.Is(err, f.transport) || result.Failure != nil || f.failures != 0 || f.completions != 0 ||
		f.hostStarts != 1 ||
		f.observations != 1 {
		t.Fatalf("known failure settled before its proof: %#v %v", result, err)
	}
	if err := f.journal.Close(); err != nil {
		t.Fatal(err)
	}
	f.open()
	f.scenario = "candidate exits"
	result, err = f.executor.Execute(t.Context(), proto.CloneOf(f.assignment), f.input.Attempt.Executor, f)
	if err != nil || result.Failure == nil || f.completions != 0 || f.hostStarts != 1 || f.observations != 1 ||
		len(f.failureProof) == 0 {
		t.Fatalf("failed observation was lost or repeated: %#v %v", result, err)
	}
	evidence, err := jcs.Decode[releaseexecution.EffectEvidence](f.failureProof)
	if err != nil || evidence.Result.ReadinessFailure == nil {
		t.Fatalf("failure proof absent: %v", err)
	}
	attempt := f.input.Attempt
	attempt.FinishedAt = &result.Failure.Failure.RecordedAt
	stage, err := evidence.Result.ReadinessFailure.DiagnosticStage(attempt, f.last.Progress, evidence.RecordedAt)
	if err != nil || stage.State != workexecution.StageFailed || stage.FailureCode != "release.candidate_stopped" ||
		!bytes.Contains([]byte(stage.Observed), []byte("42")) {
		t.Fatalf("operator failed stage missing: %#v %v", stage, err)
	}
	// The native proof decoder rejects another stage/cursor or altered image.
	reference, err := f.journal.Effect(t.Context(), f.input, *f.last)
	if err != nil || reference == nil {
		t.Fatal(err)
	}
	wrong := *f.last
	wrong.Sequence++
	if _, err := releaseexecution.DecodeEffectEvidence(f.input, wrong, f.failureProof, reference.Reference); err == nil {
		t.Fatal("failure proof crossed checkpoint identity")
	}
	evidence.Result.ReadinessFailure.Container.ImageID = "foreign"
	if _, _, err := releaseexecution.EncodeEffectEvidence(f.input, *f.last, evidence.Result, time.Now().UTC()); err == nil {
		t.Fatal("failure proof accepted another image")
	}
	starts, observations := f.hostStarts, f.observations
	if err := f.journal.Close(); err != nil {
		t.Fatal(err)
	}
	f.open()
	replay, err := f.executor.Execute(t.Context(), proto.CloneOf(f.assignment), f.input.Attempt.Executor, f)
	if err != nil || replay.Failure == nil || replay.Failure.Evidence != result.Failure.Evidence ||
		f.hostStarts != starts ||
		f.observations != observations {
		t.Fatalf("terminal replay changed failure: %#v %v", replay, err)
	}
}

func TestReadinessUncertaintyDoesNotBecomeFailedObservation(t *testing.T) {
	input, report, observed := nativeReadinessBoundary(t, true)
	if _, err := releaseexecution.ReadinessFailureResult(input, report.Progress, nil); err == nil {
		t.Fatal("missing observation fabricated failure")
	}
	observed.ProjectName = "foreign"
	if _, err := releaseexecution.ReadinessFailureResult(input, report.Progress, observed); err == nil {
		t.Fatal("foreign observation fabricated failure")
	}
}
