package etcd

import (
	"context"
	"testing"

	"github.com/AlanD20/groundplane/internal/core"
	domain "github.com/AlanD20/groundplane/internal/core/release"
)

// Rationale: observation must fence the runtime sidecar, including change-back,
// while preserving fixed-revision reads and implicit running when it is absent.
func TestServiceObservationReadPinsRuntimeRevision(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	repository, store, environment, _ := serviceRepositoryTestHierarchy(t)
	desired := serviceRepositoryTestDesired(910, "worker")
	seedServiceRepositoryTestDesiredProjection(t, store, serviceRecordTestProjection(t, environment.Record.ID, desired))
	initial := readObservationTestService(ctx, t, repository, environment.Record.ID, 0)
	if ServiceRuntimeRevision(initial) != 0 ||
		initial.Record.Runtime.RuntimeIntent != core.ServiceRuntimeIntentRunning {
		t.Fatalf("initial joined runtime = %+v", initial)
	}
	runtime := ServiceRuntimeRecord{
		EnvironmentID: environment.Record.ID,
		ServiceID:     desired.ID,
		Runtime:       initial.Record.Runtime,
	}
	runtime.Runtime.RuntimeIntent = core.ServiceRuntimeIntentStopped
	seedServiceRepositoryTestRuntime(t, store, runtime)
	runtime.Runtime.RuntimeIntent = core.ServiceRuntimeIntentRunning
	revision := seedServiceRepositoryTestRuntime(t, store, runtime)
	current := readObservationTestService(ctx, t, repository, environment.Record.ID, 0)
	if current.Record.Runtime != initial.Record.Runtime || ServiceRuntimeRevision(current) != revision ||
		ServiceRuntimeRevision(current) == ServiceRuntimeRevision(initial) || current.Revision != initial.Revision {
		t.Fatalf("runtime change-back lost its independent fence: %+v", current)
	}
	historical := readObservationTestService(ctx, t, repository, environment.Record.ID, initial.ReadRevision)
	if historical.ReadRevision != initial.ReadRevision || ServiceRuntimeRevision(historical) != 0 {
		t.Fatalf("historical runtime changed: %+v", historical)
	}
}

func readObservationTestService(
	ctx context.Context, t *testing.T, repository *ServiceRepository, environmentID string, revision int64,
) Versioned[ServiceRecord] {
	t.Helper()
	page, err := repository.ListServices(ctx, environmentID, PageRequest{Limit: 1, Revision: revision})
	if err != nil || len(page.Items) != 1 {
		t.Fatalf("Service page = %+v, %v", page, err)
	}
	return page.Items[0]
}

// Rationale: the observation binding must reproduce the publication digest from
// the decoded immutable input without rendering Compose or resolving secrets.
func TestServiceObservationRenderDigestMatchesStoredEncoding(t *testing.T) {
	t.Parallel()
	input := portlessReleaseRenderInput(domain.StrategyRecreate)
	encoded, err := EncodeReleaseRenderInput(input)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeReleaseRenderInput(encoded)
	if err != nil {
		t.Fatal(err)
	}
	want, err := domain.Digest(encoded)
	if err != nil {
		t.Fatal(err)
	}
	got, err := domain.Digest(decoded)
	if err != nil || got != want {
		t.Fatalf("decoded digest %q differs from published %q: %v", got, want, err)
	}
}
