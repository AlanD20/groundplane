package etcd

import (
	"context"
	"io"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: the 128-target bound applies after filtering undeployed Services,
// so a larger desired inventory with no serving Releases is a valid empty stream.
func TestResolveEnvironmentLogTargetsFiltersBeforeMaximum(t *testing.T) {
	t.Parallel()

	_, memory, environment, project := serviceRepositoryTestHierarchy(t)
	names := make([]string, 129)
	for index := range 129 {
		names[index] = "service-" + strconv.FormatInt(int64(index), 10)
	}
	stageReleaseLogDesiredProjection(t, memory, environment, project, names)
	ledger := testReleaseLogLedger(t, &releaseLogMemoryStore{memoryHierarchyStore: memory})
	targets, err := ledger.ResolveEnvironmentLogTargets(context.Background(), environment.Record.ID, 128)
	if err != nil || len(targets) != 0 {
		t.Fatalf("ResolveEnvironmentLogTargets() = %#v, %v; want empty", targets, err)
	}
}

// Rationale: desired Services may be profile-disabled or otherwise absent
// from effective Release state; log targets must contain only applied Services
// with a serving Release, not every desired identity.
func TestResolveEnvironmentLogTargetsReturnsOnlyEffectiveAppliedServices(t *testing.T) {
	t.Parallel()

	_, memory, environment, project := serviceRepositoryTestHierarchy(t)
	fixtures := stageReleaseLogDesiredProjection(t, memory, environment, project, []string{
		"disabled", "effective", "suppressed",
	})
	installServingRelease(t, memory, environment.Record.ID, project.Record, fixtures[1].ID, 2201)

	ledger := testReleaseLogLedger(t, &releaseLogMemoryStore{memoryHierarchyStore: memory})
	targets, err := ledger.ResolveEnvironmentLogTargets(context.Background(), environment.Record.ID, 128)
	if err != nil || len(targets) != 1 {
		t.Fatalf("ResolveEnvironmentLogTargets() = %#v, %v; want one effective target", targets, err)
	}
	if targets[0].ServiceID != fixtures[1].ID || targets[0].ServiceName != "effective" {
		t.Fatalf("effective target = %#v, want %q/effective", targets[0], fixtures[1].ID)
	}
}

// Rationale: parent-last deletion after the Environment anchor must not turn a
// previously deployed snapshot into mixed-revision empty success.
func TestResolveEnvironmentLogTargetsUsesOneFixedRevisionThroughDeletion(t *testing.T) {
	t.Parallel()

	_, memory, environment, project := serviceRepositoryTestHierarchy(t)
	fixtures := stageReleaseLogDesiredProjection(t, memory, environment, project, []string{
		"worker", "api", "scheduler",
	})
	for index, fixture := range fixtures {
		installServingRelease(t, memory, environment.Record.ID, project.Record, fixture.ID, int64(1200+index))
	}
	base := &releaseLogMemoryStore{memoryHierarchyStore: memory}
	race := &releaseLogDeletionRaceStore{releaseLogMemoryStore: base}
	race.afterEnvironmentRead = func() {
		mutations := []Mutation{
			{Type: MutationDelete, Key: environmentKey(environment.Record.ID)},
			{Type: MutationDelete, Key: environmentBlueprintHeadKey(environment.Record.ID)},
		}
		for index, fixture := range fixtures {
			releaseID := ids.NewAt(ids.KindDeployment, serviceRecordTestTime(), int64(1200+index))
			mutations = append(mutations,
				Mutation{Type: MutationDelete, Key: releaseProjectionKey(fixture.ID)},
				Mutation{Type: MutationDelete, Key: releaseIntentStagingKey("", releaseID)},
			)
		}
		if _, err := memory.Transact(context.Background(), nil, mutations); err != nil {
			t.Fatalf("delete snapshot after anchor: %v", err)
		}
	}
	ledger := testReleaseLogLedger(t, race)
	targets, err := ledger.ResolveEnvironmentLogTargets(context.Background(), environment.Record.ID, 128)
	if err != nil || len(targets) != len(fixtures) {
		t.Fatalf("ResolveEnvironmentLogTargets() = %#v, %v", targets, err)
	}
	for index := 1; index < len(targets); index++ {
		if targets[index-1].ServiceID >= targets[index].ServiceID {
			t.Fatalf("targets are not sorted by stable Service id: %#v", targets)
		}
	}
	if _, err := ledger.ResolveEnvironmentLogTargets(context.Background(), environment.Record.ID, 128); !isKind(
		err,
		errs.KindEnvironmentNotFound,
	) {
		t.Fatalf("post-deletion snapshot error = %v, want Environment not found", err)
	}
}

type releaseLogServiceFixture struct {
	ID   string
	Name string
}

func stageReleaseLogDesiredProjection(
	t *testing.T,
	store *memoryHierarchyStore,
	environment Versioned[EnvironmentRecord],
	project Versioned[ProjectRecord],
	names []string,
) []releaseLogServiceFixture {
	t.Helper()
	now := serviceRecordTestTime()
	sortedNames := append([]string(nil), names...)
	sort.Strings(sortedNames)
	task := environmentBlueprintTestTask(t, project.Record, environment.Record, 4000)
	projection := EnvironmentComposeProjection{
		EnvironmentID:    environment.Record.ID,
		RevisionID:       task.ID,
		RenderGeneration: 1,
		DesiredServices:  make([]EnvironmentServiceProjection, len(sortedNames)),
	}
	fixtures := make([]releaseLogServiceFixture, len(sortedNames))
	for index, name := range sortedNames {
		serviceID := ids.NewAt(ids.KindService, now, int64(300+index))
		fixtures[index] = releaseLogServiceFixture{ID: serviceID, Name: name}
		projection.DesiredServices[index] = EnvironmentServiceProjection{
			EnvironmentID: environment.Record.ID,
			Desired: core.Service{
				ID:       serviceID,
				Name:     name,
				Image:    "app:latest",
				Strategy: core.StrategyRecreate,
			},
		}
	}
	projection = withTestEnvironmentComposeArtifact(projection)
	hierarchy, err := newHierarchyRepository(store)
	if err != nil {
		t.Fatalf("newHierarchyRepository() error = %v", err)
	}
	revision := environmentBlueprintTestRevision(environment.Record.ID, task, "services: {}\n")
	marker := environmentBlueprintTestMarker(task, environment.Record.ID)
	stageEnvironmentBlueprintForPublicationTest(t, hierarchy, 0, revision, projection, marker)
	headValue, err := encodeTaskReference(task.ID)
	if err != nil {
		t.Fatalf("encodeTaskReference() error = %v", err)
	}
	if _, err := store.Transact(context.Background(), nil, []Mutation{{
		Type:  MutationPut,
		Key:   environmentBlueprintHeadKey(environment.Record.ID),
		Value: headValue,
	}}); err != nil {
		t.Fatalf("Put(Environment blueprint head) error = %v", err)
	}
	return fixtures
}

func installServingRelease(
	t *testing.T,
	store *memoryHierarchyStore,
	environmentID string,
	project ProjectRecord,
	serviceID string,
	offset int64,
) {
	t.Helper()
	now := serviceRecordTestTime()
	releaseID := ids.NewAt(ids.KindDeployment, now, offset)
	intent := domain.Intent{
		ID: releaseID, EnvironmentID: environmentID, ServiceID: serviceID,
		OperationID: ids.NewAt(ids.KindOperation, now, offset), OperationKind: domain.OperationDeploy,
		CandidateWorkload: releaseTestWorkloadSeal("app:latest"), Tag: "stable", Strategy: domain.StrategyRecreate,
		OnFailure:     domain.OnFailureLeaveActive,
		RenderInputID: ids.NewAt(ids.KindConfig, now, offset), RenderInputDigest: strings.Repeat("a", 64),
		CreatedAt: now, Actor: "operator", OriginatingTaskID: ids.NewAt(ids.KindTask, now, offset),
		Workspace: domain.Workspace{
			Kind: domain.WorkspaceTenant, TenantID: project.TenantID,
			ProjectID: project.ID, EnvironmentID: environmentID,
		},
	}
	if err := domain.ValidateIntent(intent); err != nil {
		t.Fatalf("ValidateIntent() error = %v", err)
	}
	projection := domain.ServiceProjection{
		EnvironmentID: environmentID, ServiceID: serviceID,
		ServingReleaseID: releaseID, CurrentSuccessfulReleaseID: releaseID, Revision: 1,
	}
	intentValue, err := encodeReleaseRecord("release-intent", intent)
	if err != nil {
		t.Fatalf("encode intent: %v", err)
	}
	projectionValue, err := encodeReleaseRecord("service-release-projection", projection)
	if err != nil {
		t.Fatalf("encode projection: %v", err)
	}
	if _, err := store.Transact(context.Background(), nil, []Mutation{
		{Type: MutationPut, Key: releaseIntentStagingKey("", releaseID), Value: intentValue},
		{Type: MutationPut, Key: releaseProjectionKey(serviceID), Value: projectionValue},
	}); err != nil {
		t.Fatalf("install serving Release: %v", err)
	}
}

func testReleaseLogLedger(t *testing.T, store Store) *ReleaseLedger {
	t.Helper()
	tasks, err := newTaskRepository(store)
	if err != nil {
		t.Fatalf("NewTaskRepository() error = %v", err)
	}
	ledger, err := NewReleaseLedger(store, tasks)
	if err != nil {
		t.Fatalf("NewReleaseLedger() error = %v", err)
	}
	return ledger
}

type releaseLogMemoryStore struct {
	*memoryHierarchyStore
}

func (store *releaseLogMemoryStore) Health(context.Context) error { return nil }
func (store *releaseLogMemoryStore) Put(ctx context.Context, key string, value []byte) (int64, error) {
	result, err := store.Transact(ctx, nil, []Mutation{{Type: MutationPut, Key: key, Value: value}})
	return result.Revision, err
}
func (store *releaseLogMemoryStore) Delete(ctx context.Context, key string) (int64, error) {
	result, err := store.Transact(ctx, nil, []Mutation{{Type: MutationDelete, Key: key}})
	return result.Revision, err
}
func (store *releaseLogMemoryStore) Watch(context.Context, string, int64) (*WatchStream, error) {
	return nil, errs.New(errs.KindInternal, "memory log target store does not implement watches")
}
func (store *releaseLogMemoryStore) Snapshot(context.Context, io.Writer) error {
	return errs.New(errs.KindInternal, "memory log target store does not implement snapshots")
}
func (store *releaseLogMemoryStore) Close() error { return nil }

type releaseLogDeletionRaceStore struct {
	*releaseLogMemoryStore
	afterEnvironmentRead func()
	injected             bool
}

func (store *releaseLogDeletionRaceStore) Get(ctx context.Context, key string) (*GetResult, error) {
	result, err := store.releaseLogMemoryStore.Get(ctx, key)
	if err == nil && !store.injected && store.afterEnvironmentRead != nil {
		store.injected = true
		store.afterEnvironmentRead()
	}
	return result, err
}

var _ Store = (*releaseLogMemoryStore)(nil)
var _ Store = (*releaseLogDeletionRaceStore)(nil)
