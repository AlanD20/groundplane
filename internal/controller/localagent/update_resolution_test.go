package localagent

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/controller/agentchannel"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: after a successful storage barrier, an uncommitted replacement
// must resume dispatch immediately; unavailable storage must recover on the
// next reconciliation rather than retaining an orphaned in-memory pause.
func TestUncertainUncommittedUpdateEventuallyResumesDispatch(t *testing.T) {
	for _, deferred := range []bool{false, true} {
		t.Run(map[bool]string{false: "immediate", true: "reconcile"}[deferred], func(t *testing.T) {
			trace := &traceLog{}
			repository := seededRepository(trace, PhaseReady)
			harness := newTestManager(t, repository, trace)
			uncertain := &uncertainReplacementRepository{
				fakeRepository:        repository,
				resolutionUnavailable: deferred,
			}
			harness.manager.repository = uncertain
			registry := agentchannel.NewRegistry()
			harness.manager.sessions = &registryLifecycleSessions{Registry: registry}
			session, err := registry.Open(context.Background(), testAgentID, initialGeneration)
			if err != nil {
				t.Fatal(err)
			}
			defer session.Close()
			err = harness.manager.Update(
				context.Background(),
				UpdateRequest{AgentID: testAgentID, PreviousImage: testImage,
					DesiredImage: replacementTestImage, StartingGeneration: initialGeneration},
			)
			if !errors.Is(err, errs.New(errs.KindStorageUnavailable, "")) {
				t.Fatalf("update error=%v", err)
			}
			if deferred {
				if session.AssignmentsAllowed() {
					t.Fatal("unresolved publication reopened dispatch")
				}
				uncertain.resolutionUnavailable = false
				if err := harness.manager.Reconcile(context.Background()); err != nil {
					t.Fatal(err)
				}
			}
			if !session.AssignmentsAllowed() {
				t.Fatal("resolved uncommitted update retained its pause")
			}
			if repository.record.Record.Generation != initialGeneration {
				t.Fatal("uncommitted update changed generation")
			}
		})
	}
}

// Rationale: a lost committed response must continue the same replacement,
// never create another generation or report an unapplied update.
func TestUncertainCommittedUpdateCompletesSameGeneration(t *testing.T) {
	trace := &traceLog{}
	repository := seededRepository(trace, PhaseReady)
	harness := newTestManager(t, repository, trace)
	harness.manager.repository = &uncertainReplacementRepository{
		fakeRepository: repository,
		committed:      true,
	}
	err := harness.manager.Update(
		context.Background(),
		UpdateRequest{AgentID: testAgentID, PreviousImage: testImage,
			DesiredImage: replacementTestImage, StartingGeneration: initialGeneration},
	)
	if err != nil {
		t.Fatal(err)
	}
	if repository.record.Record.Generation != initialGeneration+1 ||
		repository.record.Record.Phase != PhaseReady ||
		repository.record.Record.Image != replacementTestImage {
		t.Fatal("committed replacement was not resumed exactly")
	}
}

// Rationale: cancellation must not prevent the independent bounded barrier
// from proving non-commit and releasing only this update's preparation hold.
func TestCancelledUnknownPublicationReleasesProvenUncommittedPause(t *testing.T) {
	trace := &traceLog{}
	repository := seededRepository(trace, PhaseReady)
	harness := newTestManager(t, repository, trace)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	harness.manager.repository = &cancelledPublicationRepository{
		uncertainReplacementRepository: &uncertainReplacementRepository{
			fakeRepository: repository,
		}, cancel: cancel,
	}
	registry := agentchannel.NewRegistry()
	harness.manager.sessions = &registryLifecycleSessions{Registry: registry}
	session, err := registry.Open(context.Background(), testAgentID, initialGeneration)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	err = harness.manager.Update(ctx, UpdateRequest{AgentID: testAgentID, PreviousImage: testImage,
		DesiredImage: replacementTestImage, StartingGeneration: initialGeneration})
	if !errors.Is(err, context.Canceled) || !session.AssignmentsAllowed() ||
		harness.manager.pending != nil {
		t.Fatalf(
			"cancelled resolution: err=%v allowed=%t pending=%t",
			err,
			session.AssignmentsAllowed(),
			harness.manager.pending != nil,
		)
	}
}

type cancelledPublicationRepository struct {
	*uncertainReplacementRepository
	cancel context.CancelFunc
}

// Rationale: recovery retains both pinned images until readiness settles. A
// lost rollback response must resume that rollback rather than clear recovery
// too early, rotate again or strand later lifecycle work.
func TestDeferredCommittedRecoveryRetainsRollbackIdentity(t *testing.T) {
	trace := &traceLog{}
	repository := seededRepository(trace, PhaseReady)
	harness := newTestManager(t, repository, trace)
	uncertain := &uncertainReplacementRepository{
		fakeRepository:        repository,
		committed:             true,
		resolutionUnavailable: true,
	}
	harness.manager.repository = uncertain
	request := UpdateRequest{
		AgentID:            testAgentID,
		PreviousImage:      testImage,
		DesiredImage:       replacementTestImage,
		StartingGeneration: initialGeneration,
	}
	if err := harness.manager.Update(context.Background(), request); !errors.Is(
		err,
		errs.New(errs.KindStorageUnavailable, ""),
	) {
		t.Fatalf("initial update=%v", err)
	}
	uncertain.resolutionUnavailable = false
	harness.sessions.readySequence = []<-chan struct{}{make(chan struct{}), closedSignal()}
	result := make(chan error, 1)
	go func() { result <- harness.manager.Reconcile(context.Background()) }()
	harness.clock.awaitTimer(t).fire(testNow.Add(ReadyTimeout))
	if err := <-result; !errors.Is(err, errs.New(errs.KindStorageUnavailable, "")) {
		t.Fatalf("unknown rollback=%v", err)
	}
	if harness.manager.pending == nil {
		t.Fatal("unsettled rollback identity was discarded")
	}
	if err := harness.manager.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if harness.manager.pending != nil || repository.record.Record.Generation != initialGeneration+2 ||
		repository.record.Record.Image != testImage ||
		repository.record.Record.Phase != PhaseReady {
		t.Fatal("rollback did not settle exactly once")
	}
}

func (repository *cancelledPublicationRepository) BeginReplacement(
	ctx context.Context,
	current StoredRecord,
	image string,
	credential Credential,
	at time.Time,
) (StoredRecord, error) {
	stored, err := repository.uncertainReplacementRepository.BeginReplacement(
		ctx,
		current,
		image,
		credential,
		at,
	)
	repository.cancel()
	return stored, err
}

func (repository *fakeRepository) FenceReplacementAttempt(
	ctx context.Context,
	id string,
	generation uint64,
	revision int64,
) (StoredRecord, error) {
	if err := ctx.Err(); err != nil {
		return StoredRecord{}, err
	}
	repository.mu.Lock()
	defer repository.mu.Unlock()
	if repository.record.Record.ID != id || repository.record.Record.Generation < generation ||
		repository.record.Revision < revision {
		return StoredRecord{}, errs.New(errs.KindStateConflict, "changed")
	}
	if repository.record.Revision == revision {
		repository.record.Revision++
	}
	return cloneStored(repository.record), nil
}

func (repository *uncertainReplacementRepository) FenceReplacementAttempt(
	ctx context.Context,
	id string,
	generation uint64,
	revision int64,
) (StoredRecord, error) {
	if repository.resolutionUnavailable {
		return StoredRecord{}, errs.New(errs.KindStorageUnavailable, "barrier unavailable")
	}
	return repository.fakeRepository.FenceReplacementAttempt(ctx, id, generation, revision)
}

func (repository *configTestRepository) FenceReplacementAttempt(
	context.Context, string,

	uint64,
	int64,
) (StoredRecord, error) {
	return StoredRecord{}, errs.New(errs.KindInternal, "unused replacement resolution")
}
