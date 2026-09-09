package localagent

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/controller/agentchannel"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: the previous busy-update test used a no-op session fake and missed
// the permanent dispatch fence that starved later work even after reconnect.
func TestUpdateBusyAgentRestoresRealRegistryDispatch(t *testing.T) {
	t.Parallel()
	trace := &traceLog{}
	repository := seededRepository(trace, PhaseReady)
	harness := newTestManager(t, repository, trace)
	registry := agentchannel.NewRegistry()
	harness.manager.sessions = &registryLifecycleSessions{Registry: registry}
	session, err := registry.Open(context.Background(), testAgentID, initialGeneration)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	harness.tasks.idleError = errs.New(errs.KindResourceInUse, "active Task assignment")
	err = harness.manager.Update(context.Background(), UpdateRequest{
		AgentID: testAgentID, PreviousImage: testImage, DesiredImage: replacementTestImage,
		StartingGeneration: initialGeneration,
	})
	if !errors.Is(err, errs.New(errs.KindResourceInUse, "")) {
		t.Fatalf("busy update = %v, want resource.in_use", err)
	}
	if !session.AssignmentsAllowed() {
		t.Fatal("rejected busy update left ordinary dispatch paused")
	}
	select {
	case <-session.Done():
		t.Fatal("rejected update interrupted the active Agent session")
	default:
	}
	if harness.runtime.generateCalls != 0 || harness.container.convergeCalls != 0 ||
		repository.record.Record.Generation != initialGeneration {
		t.Fatal("busy update changed runtime identity")
	}
	reconnected, err := registry.Open(context.Background(), testAgentID, initialGeneration)
	if err != nil {
		t.Fatal(err)
	}
	defer reconnected.Close()
	if !reconnected.AssignmentsAllowed() {
		t.Fatal("rejected update retained a lifecycle fence across reconnect")
	}
}

// Rationale: every failure before the publication attempt must release update
// preparation even when the caller's context is already cancelled.
func TestUpdatePreparationFailureRestoresRealRegistryDispatch(t *testing.T) {
	for _, failure := range []string{"credential-generation", "invalid-credential", "cancelled"} {
		t.Run(failure, func(t *testing.T) {
			trace := &traceLog{}
			repository := seededRepository(trace, PhaseReady)
			harness := newTestManager(t, repository, trace)
			registry := agentchannel.NewRegistry()
			harness.manager.sessions = &registryLifecycleSessions{Registry: registry}
			session, err := registry.Open(context.Background(), testAgentID, initialGeneration)
			if err != nil {
				t.Fatal(err)
			}
			defer session.Close()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			harness.manager.runtime = &failedPreparationRuntime{
				fakeRuntime: harness.runtime, failure: failure, cancel: cancel,
			}
			err = harness.manager.Update(ctx, UpdateRequest{
				AgentID: testAgentID, PreviousImage: testImage, DesiredImage: replacementTestImage,
				StartingGeneration: initialGeneration,
			})
			if err == nil || !session.AssignmentsAllowed() {
				t.Fatalf("failed preparation: error=%v, dispatch allowed=%t", err, session.AssignmentsAllowed())
			}
			if repository.record.Record.Generation != initialGeneration ||
				harness.runtime.materializeCalls != 0 || harness.container.convergeCalls != 0 {
				t.Fatal("failed preparation changed runtime identity")
			}
		})
	}
}

// Rationale: transport failure is not proof that a replacement transaction did
// not commit. Both outcomes must retain admission until durable recovery fences
// the predecessor; a blind defer-resume would reopen stale authority.
func TestUpdatePublicationFailureRetainsUncertainGenerationPause(t *testing.T) {
	for _, committed := range []bool{false, true} {
		t.Run(map[bool]string{false: "not-committed", true: "committed"}[committed], func(t *testing.T) {
			trace := &traceLog{}
			repository := seededRepository(trace, PhaseReady)
			harness := newTestManager(t, repository, trace)
			harness.manager.repository = &uncertainReplacementRepository{
				fakeRepository: repository,
				committed:      committed,
			}
			registry := agentchannel.NewRegistry()
			harness.manager.sessions = &registryLifecycleSessions{Registry: registry}
			session, err := registry.Open(context.Background(), testAgentID, initialGeneration)
			if err != nil {
				t.Fatal(err)
			}
			defer session.Close()
			err = harness.manager.Update(context.Background(), UpdateRequest{
				AgentID: testAgentID, PreviousImage: testImage, DesiredImage: replacementTestImage,
				StartingGeneration: initialGeneration,
			})
			if err == nil || session.AssignmentsAllowed() {
				t.Fatalf("uncertain publication: error=%v, dispatch allowed=%t", err, session.AssignmentsAllowed())
			}
		})
	}
}

type failedPreparationRuntime struct {
	*fakeRuntime
	failure string
	cancel  context.CancelFunc
}

func (runtime *failedPreparationRuntime) GenerateCredential(ctx context.Context, _ string) (Credential, error) {
	switch runtime.failure {
	case "credential-generation":
		return Credential{}, errors.New("credential source unavailable")
	case "cancelled":
		runtime.cancel()
		return Credential{}, ctx.Err()
	default:
		return Credential{}, nil
	}
}

type uncertainReplacementRepository struct {
	*fakeRepository
	committed bool
}

func (repository *uncertainReplacementRepository) BeginReplacement(
	ctx context.Context, current StoredRecord, image string, credential Credential, at time.Time,
) (StoredRecord, error) {
	if repository.committed {
		if _, err := repository.fakeRepository.BeginReplacement(ctx, current, image, credential, at); err != nil {
			return StoredRecord{}, err
		}
	}
	return StoredRecord{}, errs.New(errs.KindStorageUnavailable, "replacement outcome is unknown")
}

// The real Registry supplies lifecycle behavior; only its snapshot is translated
// at this test seam, just as the production composition adapter does.
type registryLifecycleSessions struct {
	*agentchannel.Registry
}

func (sessions *fakeSessions) PauseAssignments(ctx context.Context, agentID string, generation uint64) (func(), error) {
	if err := sessions.StopAssignments(ctx, agentID, generation); err != nil {
		return nil, err
	}
	return func() { sessions.trace.add("resume_assignments") }, nil
}

func (configTestSessions) PauseAssignments(context.Context, string, uint64) (func(), error) {
	return func() {}, nil
}

func (sessions *registryLifecycleSessions) Snapshot(agentID string) (SessionSnapshot, bool) {
	snapshot, ok := sessions.Registry.Snapshot(agentID)
	return SessionSnapshot{
		Generation: snapshot.Generation,
		Online:     snapshot.Online,
		Revoked:    snapshot.Revoked,
		LastReady:  snapshot.LastReady,
		Capacity:   snapshot.Capacity,
		Version:    snapshot.Version,
	}, ok
}
