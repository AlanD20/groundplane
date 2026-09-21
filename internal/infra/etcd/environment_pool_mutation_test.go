package etcd

import (
	"context"
	"net/http"
	"net/netip"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/environmentpath"
	"github.com/AlanD20/groundplane/internal/common/ids"
	testdeletions "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testidempotency "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testnetworkreservations "github.com/AlanD20/groundplane/internal/infra/etcd/networkreservations"
	testrecordcodec "github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestHierarchyEnvironmentPoolReplacementIsAtomicAndReplays(t *testing.T) {
	t.Parallel()
	repository, store, current, zoneRevision := environmentPoolMutationFixture(t, nil)
	replacement := current.Record
	replacement.NetworkPool = "10.40.0.0/15"
	marker := environmentPoolMutationTestMarker(current.Record.ID, "environment-edit-key-000001")

	result, err := repository.ReplaceEnvironmentPoolIdempotent(
		context.Background(), netip.MustParsePrefix("10.0.0.0/8"), current, replacement, marker,
	)
	if err != nil || result.kind != idempotencyTransactionApplied {
		t.Fatalf("ReplaceEnvironmentPoolIdempotent() = %#v, %v", result, err)
	}
	stored, err := repository.GetEnvironment(context.Background(), current.Record.ID)
	if err != nil || stored.Record.NetworkPool != replacement.NetworkPool || stored.Revision != result.revision {
		t.Fatalf("stored Environment = %#v, %v", stored, err)
	}
	registry, err := repository.GetEnvironmentPoolRegistry(context.Background())
	if err != nil || registry.Record.Reservations[current.Record.ID] != replacement.NetworkPool ||
		registry.Revision != result.revision {
		t.Fatalf("stored Environment pool registry = %#v, %v", registry, err)
	}
	epoch, err := store.Get(context.Background(), testhierarchy.EnvironmentMutationEpochKey(current.Record.ID))
	if err != nil || epoch.Entry == nil || epoch.Entry.ModRevision != result.revision {
		t.Fatalf("stored mutation epoch = %#v, %v", epoch, err)
	}
	markerKey, err := testidempotency.IdempotencyMarkerKey(marker.Locator)
	if err != nil {
		t.Fatalf("idempotencyMarkerKey() error = %v", err)
	}
	markerValue, err := store.Get(context.Background(), markerKey)
	if err != nil || markerValue.Entry == nil || markerValue.Entry.ModRevision != result.revision {
		t.Fatalf("stored replay marker = %#v, %v", markerValue, err)
	}
	zones, err := store.Get(context.Background(), testnetworkreservations.ZonePoolRegistryKey(current.Record.ID))
	if err != nil || zones.Entry == nil || zones.Entry.ModRevision != zoneRevision {
		t.Fatalf("compare-only Zone registry = %#v, %v", zones, err)
	}

	replayed, err := repository.ReplaceEnvironmentPoolIdempotent(
		context.Background(), netip.MustParsePrefix("10.0.0.0/8"), current, replacement, marker,
	)
	if err != nil || replayed.kind != idempotencyTransactionExisting {
		t.Fatalf("ReplaceEnvironmentPoolIdempotent(replay) = %#v, %v", replayed, err)
	}
}

func TestHierarchyEnvironmentPoolReplacementRejectsZoneExclusionAndGlobalOverlap(t *testing.T) {
	t.Parallel()
	repository, _, current, _ := environmentPoolMutationFixture(t, map[string]string{
		hierarchyTestID(ids.KindEnvironment, 709): "10.42.0.0/16",
	})

	excludesZone := current.Record
	excludesZone.NetworkPool = "10.40.2.0/24"
	_, err := repository.ReplaceEnvironmentPoolIdempotent(
		context.Background(),
		netip.MustParsePrefix("10.0.0.0/8"),
		current,
		excludesZone,
		environmentPoolMutationTestMarker(current.Record.ID, "environment-edit-key-000002"),
	)
	if !isKind(err, errs.KindValidationFailed) {
		t.Fatalf("ReplaceEnvironmentPoolIdempotent(excludes Zone) error = %v", err)
	}

	overlapsEnvironment := current.Record
	overlapsEnvironment.NetworkPool = "10.42.0.0/15"
	_, err = repository.ReplaceEnvironmentPoolIdempotent(
		context.Background(),
		netip.MustParsePrefix("10.0.0.0/8"),
		current,
		overlapsEnvironment,
		environmentPoolMutationTestMarker(current.Record.ID, "environment-edit-key-000003"),
	)
	if !isKind(err, errs.KindStateConflict) {
		t.Fatalf("ReplaceEnvironmentPoolIdempotent(overlap) error = %v", err)
	}
}

func TestHierarchyEnvironmentPoolReplacementLosesConcurrentZoneReservation(t *testing.T) {
	t.Parallel()
	base := newMemoryHierarchyStore()
	racing := &environmentPoolMutationRaceStore{memoryHierarchyStore: base}
	repository, _, current, _ := environmentPoolMutationFixtureWithStore(t, racing, nil)
	racing.key = testnetworkreservations.ZonePoolRegistryKey(current.Record.ID)
	racing.value = environmentPoolMutationZoneValue(t, map[string]string{
		hierarchyTestID(ids.KindNetwork, 708): "10.40.1.0/24",
		hierarchyTestID(ids.KindNetwork, 710): "10.41.0.0/24",
	})
	racing.armed = true
	replacement := current.Record
	replacement.NetworkPool = "10.40.0.0/15"
	marker := environmentPoolMutationTestMarker(current.Record.ID, "environment-edit-key-000004")

	result, err := repository.ReplaceEnvironmentPoolIdempotent(
		context.Background(), netip.MustParsePrefix("10.0.0.0/8"), current, replacement, marker,
	)
	if err != nil || result.kind != idempotencyTransactionConflict ||
		!isKind(result.conflict, errs.KindStateConflict) || !racing.injected {
		t.Fatalf("ReplaceEnvironmentPoolIdempotent(Zone race) = %#v, %v", result, err)
	}
	stored, err := repository.GetEnvironment(context.Background(), current.Record.ID)
	if err != nil || stored.Record.NetworkPool != current.Record.NetworkPool {
		t.Fatalf("Environment after Zone race = %#v, %v", stored, err)
	}
	registry, err := repository.GetEnvironmentPoolRegistry(context.Background())
	if err != nil || registry.Record.Reservations[current.Record.ID] != current.Record.NetworkPool {
		t.Fatalf("pool registry after Zone race = %#v, %v", registry, err)
	}
	markerKey, err := testidempotency.IdempotencyMarkerKey(marker.Locator)
	if err != nil {
		t.Fatalf("idempotencyMarkerKey() error = %v", err)
	}
	markerValue, err := base.Get(context.Background(), markerKey)
	if err != nil || markerValue.Entry != nil {
		t.Fatalf("replay marker after Zone race = %#v, %v", markerValue, err)
	}
}

func TestHierarchyEnvironmentPoolReplacementLosesGlobalRegistryCAS(t *testing.T) {
	t.Parallel()
	base := newMemoryHierarchyStore()
	racing := &environmentPoolMutationRaceStore{memoryHierarchyStore: base}
	repository, _, current, _ := environmentPoolMutationFixtureWithStore(t, racing, nil)
	globalValue, err := testrecordcodec.Encode(
		"environment_pool_registry",
		testnetworkreservations.EnvironmentPoolRegistry{
			Reservations: map[string]string{
				current.Record.ID:                         current.Record.NetworkPool,
				hierarchyTestID(ids.KindEnvironment, 711): "10.42.0.0/16",
			},
		},
	)
	if err != nil {
		t.Fatalf("encode raced global registry error = %v", err)
	}
	racing.key = testnetworkreservations.EnvironmentPoolRegistryKey
	racing.value = globalValue
	racing.armed = true
	replacement := current.Record
	replacement.NetworkPool = "10.40.0.0/15"
	marker := environmentPoolMutationTestMarker(current.Record.ID, "environment-edit-global-race-0001")

	result, err := repository.ReplaceEnvironmentPoolIdempotent(
		context.Background(), netip.MustParsePrefix("10.0.0.0/8"), current, replacement, marker,
	)
	if err != nil || result.kind != idempotencyTransactionConflict ||
		!isKind(result.conflict, errs.KindStateConflict) || !racing.injected {
		t.Fatalf("ReplaceEnvironmentPoolIdempotent(global race) = %#v, %v", result, err)
	}
	stored, err := repository.GetEnvironment(context.Background(), current.Record.ID)
	if err != nil || stored.Record.NetworkPool != current.Record.NetworkPool {
		t.Fatalf("Environment after global registry race = %#v, %v", stored, err)
	}
	markerKey, err := testidempotency.IdempotencyMarkerKey(marker.Locator)
	if err != nil {
		t.Fatalf("idempotencyMarkerKey() error = %v", err)
	}
	markerValue, err := base.Get(context.Background(), markerKey)
	if err != nil || markerValue.Entry != nil {
		t.Fatalf("replay marker after global registry race = %#v, %v", markerValue, err)
	}
}

func TestHierarchyEnvironmentPoolReplacementLosesTenantDeletionFence(t *testing.T) {
	t.Parallel()
	base := newMemoryHierarchyStore()
	racing := &environmentPoolMutationRaceStore{memoryHierarchyStore: base}
	repository, _, current, _ := environmentPoolMutationFixtureWithStore(t, racing, nil)
	racing.key = testdeletions.TombstoneKey("tenant", hierarchyTestID(ids.KindTenant, 705))
	racing.value = []byte(`{"phase":"requested"}`)
	racing.armed = true
	replacement := current.Record
	replacement.NetworkPool = "10.40.0.0/15"
	marker := environmentPoolMutationTestMarker(current.Record.ID, "environment-edit-delete-race-0001")

	result, err := repository.ReplaceEnvironmentPoolIdempotent(
		context.Background(), netip.MustParsePrefix("10.0.0.0/8"), current, replacement, marker,
	)
	if err != nil || result.kind != idempotencyTransactionConflict ||
		!isKind(result.conflict, errs.KindResourceInUse) || !racing.injected {
		t.Fatalf("ReplaceEnvironmentPoolIdempotent(Tenant deletion race) = %#v, %v", result, err)
	}
	stored, err := repository.GetEnvironment(context.Background(), current.Record.ID)
	if err != nil || stored.Record.NetworkPool != current.Record.NetworkPool {
		t.Fatalf("Environment after Tenant deletion race = %#v, %v", stored, err)
	}
	markerKey, err := testidempotency.IdempotencyMarkerKey(marker.Locator)
	if err != nil {
		t.Fatalf("idempotencyMarkerKey() error = %v", err)
	}
	markerValue, err := base.Get(context.Background(), markerKey)
	if err != nil || markerValue.Entry != nil {
		t.Fatalf("replay marker after Tenant deletion race = %#v, %v", markerValue, err)
	}
}

func environmentPoolMutationFixture(
	t *testing.T,
	additional map[string]string,
) (*HierarchyRepository, *memoryHierarchyStore, testkeyvalue.Versioned[testhierarchy.EnvironmentRecord], int64) {
	t.Helper()
	return environmentPoolMutationFixtureWithStore(t, newMemoryHierarchyStore(), additional)
}

func environmentPoolMutationFixtureWithStore(
	t *testing.T,
	store hierarchyStore,
	additional map[string]string,
) (*HierarchyRepository, *memoryHierarchyStore, testkeyvalue.Versioned[testhierarchy.EnvironmentRecord], int64) {
	t.Helper()
	base, ok := store.(*memoryHierarchyStore)
	if !ok {
		base = store.(*environmentPoolMutationRaceStore).memoryHierarchyStore
	}
	repository, err := newHierarchyRepository(store)
	if err != nil {
		t.Fatalf("newHierarchyRepository() error = %v", err)
	}
	tenant := testhierarchy.TenantRecord{ID: hierarchyTestID(ids.KindTenant, 705), Slug: "acme", Name: "Acme"}
	if _, err := repository.CreateTenant(context.Background(), tenant); err != nil {
		t.Fatalf("CreateTenant() error = %v", err)
	}
	project := testhierarchy.ProjectRecord{
		ID: hierarchyTestID(ids.KindProject, 706), TenantID: tenant.ID,
		Slug: "console", Name: "Console", Kind: testhierarchy.ProjectKindTenant,
	}
	if _, err := repository.CreateProject(context.Background(), project); err != nil {
		t.Fatalf("CreateProject() error = %v", err)
	}
	record, err := testhierarchy.NewProvisioningEnvironment(
		environmentpath.DefaultVolumeRoot,
		project,
		hierarchyTestID(ids.KindEnvironment, 707),
		"production",
		"10.40.0.0/16",
		hierarchyTestID(ids.KindTask, 707),
		time.Date(2026, 8, 25, 8, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatalf("NewProvisioningEnvironment() error = %v", err)
	}
	if _, err := repository.CreateEnvironment(context.Background(), record); err != nil {
		t.Fatalf("CreateEnvironment() error = %v", err)
	}
	reservations := map[string]string{record.ID: record.NetworkPool}
	for id, pool := range additional {
		reservations[id] = pool
	}
	globalValue, err := testrecordcodec.Encode(
		"environment_pool_registry",
		testnetworkreservations.EnvironmentPoolRegistry{
			Reservations: reservations,
		},
	)
	if err != nil {
		t.Fatalf("encode Environment pool registry error = %v", err)
	}
	zoneValue := environmentPoolMutationZoneValue(t, map[string]string{
		hierarchyTestID(ids.KindNetwork, 708): "10.40.1.0/24",
	})
	seeded, err := base.Transact(context.Background(), nil, []testkeyvalue.Mutation{
		{Type: testkeyvalue.MutationPut, Key: testnetworkreservations.EnvironmentPoolRegistryKey, Value: globalValue},
		{Type: testkeyvalue.MutationPut, Key: testnetworkreservations.ZonePoolRegistryKey(record.ID), Value: zoneValue},
	})
	if err != nil || !seeded.Succeeded {
		t.Fatalf("seed pool registries = %#v, %v", seeded, err)
	}
	current, err := repository.GetEnvironment(context.Background(), record.ID)
	if err != nil {
		t.Fatalf("GetEnvironment() error = %v", err)
	}
	return repository, base, current, seeded.Revision
}

func environmentPoolMutationZoneValue(t *testing.T, reservations map[string]string) []byte {
	t.Helper()
	value, err := testrecordcodec.Encode(
		"zone_pool_registry",
		testnetworkreservations.ZonePoolRegistry{Reservations: reservations},
	)
	if err != nil {
		t.Fatalf("encode Zone pool registry error = %v", err)
	}
	return value
}

func environmentPoolMutationTestMarker(environmentID string, key string) testidempotency.IdempotencyMarker {
	marker := environmentMutationTestMarker(environmentID, key)
	marker.Locator.Method = http.MethodPatch
	marker.Locator.Route = "/environments/{id}"
	return marker
}

type environmentPoolMutationRaceStore struct {
	*memoryHierarchyStore
	key      string
	value    []byte
	armed    bool
	injected bool
}

func (store *environmentPoolMutationRaceStore) Transact(
	ctx context.Context,
	conditions []testkeyvalue.Condition,
	mutations []testkeyvalue.Mutation,
) (testkeyvalue.TransactionResult, error) {
	if store.armed && !store.injected {
		store.injected = true
		if _, err := store.memoryHierarchyStore.Transact(ctx, nil, []testkeyvalue.Mutation{{
			Type: testkeyvalue.MutationPut, Key: store.key, Value: store.value,
		}}); err != nil {
			return testkeyvalue.TransactionResult{}, err
		}
	}
	return store.memoryHierarchyStore.Transact(ctx, conditions, mutations)
}
