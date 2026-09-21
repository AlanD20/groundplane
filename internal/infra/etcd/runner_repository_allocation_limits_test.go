package etcd

import (
	context "context"
	errors "errors"
	ids "github.com/AlanD20/groundplane/internal/common/ids"
	runnerallocation "github.com/AlanD20/groundplane/internal/common/runnerallocation"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testrecordcodec "github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	testrunners "github.com/AlanD20/groundplane/internal/infra/etcd/runners"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	errs "github.com/AlanD20/groundplane/pkg/errs"
	netip "net/netip"
	testing "testing"
)

// Rationale: allocation exhaustion is a use conflict while malformed durable
// slot ownership is corruption; callers must never receive the same class.
func TestRunnerAllocationExhaustionAndCorruptionAreExplicit(t *testing.T) {
	t.Run("host slots", func(t *testing.T) {
		ctx := context.Background()
		store, repository, tenantID, _ := newRunnerRepositoryFixture(t)
		config := runnerTestAllocationConfig()
		config.HostPool.HostUIDEnd = config.HostPool.HostUIDStart + 4
		config.HostPool.SubUIDEnd = config.HostPool.SubUIDStart + 5*runnerallocation.RunnerSubordinateBlockSize - 1
		config.HostPool.SubGIDEnd = config.HostPool.SubGIDStart + 5*runnerallocation.RunnerSubordinateBlockSize - 1
		for index := 0; index < 5; index++ {
			desired := runnerTestDesired(100+index, testrunners.RunnerOwnerTenant, tenantID, tenantID)
			task := runnerTestTask(
				desired,
				testtaskjournal.TaskCreate,
				110+index,
				"runner-host-capacity-key-0"+string(rune('0'+index)),
			)
			if _, err := repository.CreateRunnerWithTask(
				ctx, config, desired, task, runnerTestMarker(task, desired),
			); err != nil {
				t.Fatalf("CreateRunnerWithTask(%d) error = %v", index, err)
			}
		}
		hierarchy, err := newHierarchyRepository(store)
		if err != nil {
			t.Fatalf("newHierarchyRepository() error = %v", err)
		}
		otherTenantID := ids.NewAt(ids.KindTenant, taskJournalTime(), 1800)
		if _, err := hierarchy.CreateTenant(ctx, testhierarchy.TenantRecord{
			ID: otherTenantID, Slug: "other", Name: "Other",
		}); err != nil {
			t.Fatalf("CreateTenant(other) error = %v", err)
		}
		desired := runnerTestDesired(199, testrunners.RunnerOwnerTenant, otherTenantID, otherTenantID)
		task := runnerTestTask(desired, testtaskjournal.TaskCreate, 199, "runner-host-capacity-over")
		if _, err := repository.CreateRunnerWithTask(
			ctx, config, desired, task, runnerTestMarker(task, desired),
		); !errors.Is(err, errs.New(errs.KindResourceInUse, "")) {
			t.Fatalf("CreateRunnerWithTask(host exhausted) error = %v", err)
		}
	})

	t.Run("network pool", func(t *testing.T) {
		ctx := context.Background()
		store, repository, tenantID, _ := newRunnerRepositoryFixture(t)
		config := runnerTestAllocationConfig()
		registry := runnerallocation.SystemPoolRegistry{
			RunnerNetworkPool: config.RunnerPool.String(),
			Reservations:      map[string]string{},
		}
		for index := 0; index < 32; index++ {
			prefix := netip.PrefixFrom(
				netip.AddrFrom4([4]byte{10, 240, 0, byte(index * 8)}), runnerallocation.RunnerSubnetBits,
			)
			registry.Reservations["component/"+string(rune('a'+index))] = prefix.String()
		}
		value, err := testrecordcodec.Encode("system_pool_registry", registry)
		if err != nil {
			t.Fatalf("encode system pool error = %v", err)
		}
		defer clear(value)
		current := mustOptionalKey(t, store, testrunners.SystemPoolRegistryKey)
		result, err := store.Transact(
			ctx,
			[]testkeyvalue.Condition{{Key: testrunners.SystemPoolRegistryKey, ModRevision: current.ModRevision}},
			[]testkeyvalue.Mutation{
				{Type: testkeyvalue.MutationPut, Key: testrunners.SystemPoolRegistryKey, Value: value},
			},
		)
		if err != nil || !result.Succeeded {
			t.Fatalf("seed system pool = %#v, %v", result, err)
		}
		desired := runnerTestDesired(200, testrunners.RunnerOwnerTenant, tenantID, tenantID)
		task := runnerTestTask(desired, testtaskjournal.TaskCreate, 201, "runner-network-capacity-key")
		if _, err := repository.CreateRunnerWithTask(
			ctx, config, desired, task, runnerTestMarker(task, desired),
		); !errors.Is(err, errs.New(errs.KindInternal, "")) {
			t.Fatalf("CreateRunnerWithTask(non-Runner overlap) error = %v", err)
		}
	})

	t.Run("corrupt slot", func(t *testing.T) {
		ctx := context.Background()
		store, repository, tenantID, _ := newRunnerRepositoryFixture(t)
		ownerID := ids.NewAt(ids.KindRunner, taskJournalTime(), 1900)
		value, err := testrunners.EncodeRunnerHostSlotRecord(
			testrunners.RunnerHostSlotRecord{Slot: 1, RunnerID: ownerID},
		)
		if err != nil {
			t.Fatalf("encodeRunnerHostSlotRecord() error = %v", err)
		}
		defer clear(value)
		result, err := store.Transact(
			ctx,
			[]testkeyvalue.Condition{{Key: testrunners.RunnerHostSlotKey(0)}},
			[]testkeyvalue.Mutation{
				{Type: testkeyvalue.MutationPut, Key: testrunners.RunnerHostSlotKey(0), Value: value},
			},
		)
		if err != nil || !result.Succeeded {
			t.Fatalf("seed corrupt host slot = %#v, %v", result, err)
		}
		desired := runnerTestDesired(202, testrunners.RunnerOwnerTenant, tenantID, tenantID)
		task := runnerTestTask(desired, testtaskjournal.TaskCreate, 203, "runner-corrupt-slot-key")
		if _, err := repository.CreateRunnerWithTask(
			ctx, runnerTestAllocationConfig(), desired, task, runnerTestMarker(task, desired),
		); !errors.Is(err, errs.New(errs.KindInternal, "")) {
			t.Fatalf("CreateRunnerWithTask(corrupt slot) error = %v", err)
		}
	})
}

func TestRunnerNetworkPoolBootstrapReservationIsExactAndReplayable(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store, repository, _, _ := newRunnerRepositoryFixture(t)
	config := runnerTestAllocationConfig()
	if err := repository.EnsureRunnerNetworkPool(ctx, config); err != nil {
		t.Fatalf("EnsureRunnerNetworkPool(replay) error = %v", err)
	}
	entry := mustOptionalKey(t, store, testrunners.SystemPoolRegistryKey)
	registry, err := testrunners.DecodeSystemPoolRegistry(entry.Value)
	if err != nil || registry.RunnerNetworkPool != config.RunnerPool.String() || registry.Reservations == nil {
		t.Fatalf("bootstrap registry = %#v, %v", registry, err)
	}
	mismatch := config
	mismatch.RunnerPool = netip.MustParsePrefix("10.242.0.0/16")
	if err := repository.EnsureRunnerNetworkPool(ctx, mismatch); !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("EnsureRunnerNetworkPool(mismatch) error = %v", err)
	}
}
