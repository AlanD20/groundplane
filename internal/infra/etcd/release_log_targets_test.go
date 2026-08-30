package etcd

import (
	"context"
	"io"
	"strconv"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: the 128-target bound applies after filtering undeployed Services,
// so a larger desired inventory with no serving Releases is a valid empty stream.
func TestResolveEnvironmentLogTargetsFiltersBeforeMaximum(t *testing.T) {
	t.Parallel()

	repository, memory, environment, project := serviceRepositoryTestHierarchy(t)
	for index := range 129 {
		record := serviceRepositoryTestRecord(
			t,
			environment.Record.ID,
			int64(1000+index),
			"service-"+strconv.Itoa(index),
		)
		if _, err := repository.CreateService(context.Background(), environment, project, record); err != nil {
			t.Fatalf("CreateService(%d) error = %v", index, err)
		}
	}
	ledger := testReleaseLogLedger(t, &releaseLogMemoryStore{memoryHierarchyStore: memory})
	targets, err := ledger.ResolveEnvironmentLogTargets(context.Background(), environment.Record.ID, 128)
	if err != nil || len(targets) != 0 {
		t.Fatalf("ResolveEnvironmentLogTargets() = %#v, %v; want empty", targets, err)
	}
}

// Rationale: parent-last deletion after the Environment anchor must not turn a
// previously deployed snapshot into mixed-revision empty success.
func TestResolveEnvironmentLogTargetsUsesOneFixedRevisionThroughDeletion(t *testing.T) {
	t.Parallel()

	repository, memory, environment, project := serviceRepositoryTestHierarchy(t)
	records := []ServiceRecord{
		serviceRepositoryTestRecord(t, environment.Record.ID, 1112, "worker"),
		serviceRepositoryTestRecord(t, environment.Record.ID, 1110, "api"),
		serviceRepositoryTestRecord(t, environment.Record.ID, 1111, "scheduler"),
	}
	for index := range records {
		if _, err := repository.CreateService(context.Background(), environment, project, records[index]); err != nil {
			t.Fatalf("CreateService(%d) error = %v", index, err)
		}
		installServingRelease(t, memory, environment.Record.ID, project.Record, records[index], int64(1200+index))
	}
	base := &releaseLogMemoryStore{memoryHierarchyStore: memory}
	race := &releaseLogDeletionRaceStore{releaseLogMemoryStore: base}
	race.afterEnvironmentRead = func() {
		mutations := []Mutation{
			{Type: MutationDelete, Key: environmentKey(environment.Record.ID)},
			{Type: MutationDelete, Key: serviceOwnerPrefix(environment.Record.ID), Prefix: true},
		}
		for index, record := range records {
			releaseID := ids.NewAt(ids.KindDeployment, serviceRecordTestTime(), int64(1200+index))
			mutations = append(mutations,
				Mutation{Type: MutationDelete, Key: serviceKey(record.Desired.ID)},
				Mutation{Type: MutationDelete, Key: releaseProjectionKey(record.Desired.ID)},
				Mutation{Type: MutationDelete, Key: releaseIntentStagingKey("", releaseID)},
			)
		}
		if _, err := memory.Transact(context.Background(), nil, mutations); err != nil {
			t.Fatalf("delete snapshot after anchor: %v", err)
		}
	}
	ledger := testReleaseLogLedger(t, race)
	targets, err := ledger.ResolveEnvironmentLogTargets(context.Background(), environment.Record.ID, 128)
	if err != nil || len(targets) != len(records) {
		t.Fatalf("ResolveEnvironmentLogTargets() = %#v, %v", targets, err)
	}
	for index := 1; index < len(targets); index++ {
		if targets[index-1].ServiceID >= targets[index].ServiceID {
			t.Fatalf("targets are not sorted by stable Service id: %#v", targets)
		}
	}
	if _, err := ledger.ResolveEnvironmentLogTargets(context.Background(), environment.Record.ID, 128); !isKind(err, errs.KindEnvironmentNotFound) {
		t.Fatalf("post-deletion snapshot error = %v, want Environment not found", err)
	}
}

func installServingRelease(
	t *testing.T,
	store *memoryHierarchyStore,
	environmentID string,
	project ProjectRecord,
	service ServiceRecord,
	offset int64,
) {
	t.Helper()
	now := serviceRecordTestTime()
	releaseID := ids.NewAt(ids.KindDeployment, now, offset)
	intent := domain.Intent{
		ID: releaseID, EnvironmentID: environmentID, ServiceID: service.Desired.ID,
		OperationID: ids.NewAt(ids.KindOperation, now, offset), OperationKind: domain.OperationDeploy,
		Image: "app:latest", Tag: "stable", Strategy: domain.StrategyRecreate,
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
		EnvironmentID: environmentID, ServiceID: service.Desired.ID,
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
		{Type: MutationPut, Key: releaseProjectionKey(service.Desired.ID), Value: projectionValue},
	}); err != nil {
		t.Fatalf("install serving Release: %v", err)
	}
}

func testReleaseLogLedger(t *testing.T, store Store) *ReleaseLedger {
	t.Helper()
	tasks, err := NewTaskRepository(store)
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
