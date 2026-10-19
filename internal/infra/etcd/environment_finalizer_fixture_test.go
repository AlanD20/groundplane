package etcd

import (
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/networkreservations"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
)

type environmentFinalizerFixture struct {
	store       *memoryHierarchyStore
	environment keyvalue.Versioned[hierarchy.EnvironmentRecord]
	operation   hierarchydeletion.HierarchyDeletionOperation
	now         time.Time
}

func newEnvironmentFinalizerFixture(t *testing.T) environmentFinalizerFixture {
	t.Helper()
	store := newMemoryHierarchyStore()
	repository, err := newHierarchyRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	_, environment := createEnvironmentBlueprintOwners(t, repository)
	fixture := environmentFinalizerFixture{
		store:       store,
		environment: environment,
		now:         environment.Record.CreatedAt.Add(time.Minute),
	}
	pool, err := recordcodec.Encode("environment_pool_registry", networkreservations.EnvironmentPoolRegistry{
		Reservations: map[string]string{environment.Record.ID: environment.Record.NetworkPool},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer clear(pool)
	fixture.putRaw(t, networkreservations.EnvironmentPoolRegistryKey, pool)
	operations, err := newHierarchyDeletionRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	begin := hierarchyDeletionCreationTestBegin(fixture.now, hierarchydeletion.HierarchyDeletionTargetEnvironment,
		environment.Record.ID, hierarchydeletion.HierarchyDeletionOperationEnvironment, "6")
	created, err := operations.Begin(t.Context(), begin)
	if err != nil {
		t.Fatal(err)
	}
	fixture.operation = created.Operation
	fixture.environment, err = repository.GetEnvironment(t.Context(), environment.Record.ID)
	if err != nil {
		t.Fatal(err)
	}
	return fixture
}

func (fixture environmentFinalizerFixture) putRaw(t *testing.T, key string, value []byte) {
	t.Helper()
	result, err := fixture.store.Transact(
		t.Context(),
		nil,
		[]keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: key, Value: value}},
	)
	if err != nil || !result.Succeeded {
		t.Fatalf("seed metadata %s: %v", key, err)
	}
}

func assertEnvironmentMetadataPresence(t *testing.T, store *memoryHierarchyStore, key string, present bool) {
	t.Helper()
	result, err := store.Get(t.Context(), key)
	if err != nil || result == nil || (result.Entry != nil) != present {
		t.Fatalf("metadata %s presence differs from %t: %v", key, present, err)
	}
}
