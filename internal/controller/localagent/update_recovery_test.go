package localagent

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: a restarted Task has no old token in memory. Before issuing a new
// credential it must fence any transaction still carrying the old revision.
func TestRecoverUpdateFencesColdAttemptBeforeNewCredential(t *testing.T) {
	trace := &traceLog{}
	repository := seededRepository(trace, PhaseReady)
	old := cloneStored(repository.record)
	harness := newTestManager(t, repository, trace)
	barrier := &coldAttemptRepository{fakeRepository: repository, old: old}
	harness.manager.repository = barrier
	result, err := harness.manager.RecoverUpdate(context.Background(), recoveryRequest(), UpdateFinish)
	if err != nil || result != UpdateReadyDesired {
		t.Fatalf("cold recovery = %q, %v", result, err)
	}
	if !barrier.fenced || !barrier.lateRejected || repository.record.Record.Generation != initialGeneration+1 {
		t.Fatal("cold recovery did not fence the delayed predecessor transaction")
	}
}

// Rationale: native Controller rollback must restore its pinned Agent image
// even if the candidate Agent already reached Ready before Controller failure.
func TestRecoverUpdateRestoresReadyCandidateExactlyOnce(t *testing.T) {
	trace := &traceLog{}
	repository := seededRepository(trace, PhaseReady)
	repository.record.Record.Generation++
	repository.record.Record.Image = replacementTestImage
	harness := newTestManager(t, repository, trace)
	for attempt := 0; attempt < 2; attempt++ {
		result, err := harness.manager.RecoverUpdate(context.Background(), recoveryRequest(), UpdateRestore)
		if err != nil || result != UpdateReadyPrevious {
			t.Fatalf("restore attempt %d = %q, %v", attempt, result, err)
		}
	}
	if repository.record.Record.Generation != initialGeneration+2 ||
		repository.record.Record.Image != testImage || harness.runtime.generateCalls != 1 {
		t.Fatal("predecessor recovery changed authority more than once")
	}
}

// Rationale: expiry before a replacement commits is a no-op recovery, not an
// excuse to rotate the Agent merely to produce a terminal Task result.
func TestRecoverUpdateUnchangedGenerationOnlyFencesAndWaits(t *testing.T) {
	trace := &traceLog{}
	repository := seededRepository(trace, PhaseReady)
	oldRevision := repository.record.Revision
	harness := newTestManager(t, repository, trace)
	result, err := harness.manager.RecoverUpdate(context.Background(), recoveryRequest(), UpdateRestore)
	if err != nil || result != UpdateReadyPrevious || repository.record.Revision <= oldRevision ||
		harness.runtime.generateCalls != 0 || repository.record.Record.Generation != initialGeneration {
		t.Fatalf("unchanged recovery = %q, %v, generation %d", result, err, repository.record.Record.Generation)
	}
}

// Rationale: a ready durable phase from the prior process is not current
// authenticated readiness; cancelled recovery must not claim it is settled.
func TestRecoverUpdateWaitsForAuthenticatedReady(t *testing.T) {
	trace := &traceLog{}
	repository := seededRepository(trace, PhaseReady)
	harness := newTestManager(t, repository, trace)
	harness.sessions.ready = make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		result, err := harness.manager.RecoverUpdate(ctx, recoveryRequest(), UpdateRestore)
		if result != "" {
			done <- errors.New("unready recovery returned a settled result")
			return
		}
		done <- err
	}()
	harness.clock.awaitTimer(t)
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled readiness = %v", err)
	}
}

// Rationale: recovery cannot cross into another operation's later generation
// or replace busy application work after an unexpected admission failure.
func TestRecoverUpdateRejectsChangedAuthorityAndBusyCandidate(t *testing.T) {
	for _, scenario := range []string{"changed", "busy", "overflow"} {
		t.Run(scenario, func(t *testing.T) {
			trace := &traceLog{}
			repository := seededRepository(trace, PhaseReady)
			repository.record.Record.Generation++
			repository.record.Record.Image = replacementTestImage
			harness := newTestManager(t, repository, trace)
			request := recoveryRequest()
			switch scenario {
			case "changed":
				repository.record.Record.Generation += 2
			case "busy":
				harness.tasks.idleError = errs.New(errs.KindResourceInUse, "active work")
			case "overflow":
				request.StartingGeneration = ^uint64(0) - 1
			}
			result, err := harness.manager.RecoverUpdate(context.Background(), request, UpdateRestore)
			if err == nil || result != "" || harness.runtime.generateCalls != 0 {
				t.Fatalf("unsafe restore = %q, %v", result, err)
			}
		})
	}
}

func recoveryRequest() UpdateRequest {
	return UpdateRequest{AgentID: testAgentID, PreviousImage: testImage,
		DesiredImage: replacementTestImage, StartingGeneration: initialGeneration}
}

type coldAttemptRepository struct {
	*fakeRepository
	old          StoredRecord
	fenced       bool
	lateRejected bool
}

func (repository *coldAttemptRepository) FenceReplacementAttempt(
	ctx context.Context, id string, generation uint64, revision int64,
) (StoredRecord, error) {
	stored, err := repository.fakeRepository.FenceReplacementAttempt(ctx, id, generation, revision)
	if err == nil {
		repository.fenced = true
	}
	return stored, err
}

func (repository *coldAttemptRepository) BeginReplacement(
	ctx context.Context, current StoredRecord, image string, credential Credential, at time.Time,
) (StoredRecord, error) {
	_, lateErr := repository.fakeRepository.BeginReplacement(ctx, repository.old, image, credential, at)
	repository.lateRejected = errors.Is(lateErr, errs.New(errs.KindStateConflict, ""))
	return repository.fakeRepository.BeginReplacement(ctx, current, image, credential, at)
}
