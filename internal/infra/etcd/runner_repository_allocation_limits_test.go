package etcd

import (
	context "context"
	errors "errors"
	netip "net/netip"
	testing "testing"

	ids "github.com/AlanD20/groundplane/internal/common/ids"
	runnerallocation "github.com/AlanD20/groundplane/internal/common/runnerallocation"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testrecordcodec "github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	testrunners "github.com/AlanD20/groundplane/internal/infra/etcd/runners"
	errs "github.com/AlanD20/groundplane/pkg/errs"
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
			fixture, err := prepareRunnerCreationFixture(t, repository, config, desired, 110+index,
				"runner-host-capacity-key-0"+string(rune('0'+index)))
			if err != nil {
				t.Fatal(err)
			}
			result := fixture.publish(t, repository)
			if outcome, _, conflict, err := result.Classify(); err != nil || conflict != nil ||
				outcome != IdempotencyKnownApplied {
				t.Fatalf("native Runner allocation = %v, %v, %v", outcome, conflict, err)
			}
		}
		hierarchy, err := newHierarchyRepository(store)
		if err != nil {
			t.Fatalf("newHierarchyRepository() error = %v", err)
		}
		otherTenantID := ids.NewAt(ids.KindTenant, runnerStorageTime(), 1800)
		if _, err := hierarchy.CreateTenant(ctx, testhierarchy.TenantRecord{
			ID: otherTenantID, Slug: "other", Name: "Other",
		}); err != nil {
			t.Fatalf("CreateTenant(other) error = %v", err)
		}
		desired := runnerTestDesired(199, testrunners.RunnerOwnerTenant, otherTenantID, otherTenantID)
		if _, err := repository.PrepareRunnerCreation(ctx, config, desired,
			ids.NewAt(ids.KindTask, runnerStorageTime(), 199), runnerStorageTime()); !errors.Is(err, errs.New(errs.KindResourceInUse, "")) {
			t.Fatalf("PrepareRunnerCreation(host exhausted) error = %v", err)
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
		if _, err := repository.PrepareRunnerCreation(ctx, config, desired,
			ids.NewAt(ids.KindTask, runnerStorageTime(), 199), runnerStorageTime()); !errors.Is(err, errs.New(errs.KindInternal, "")) {
			t.Fatalf("PrepareRunnerCreation(non-Runner overlap) error = %v", err)
		}
	})

	t.Run("corrupt slot", func(t *testing.T) {
		ctx := context.Background()
		store, repository, tenantID, _ := newRunnerRepositoryFixture(t)
		ownerID := ids.NewAt(ids.KindRunner, runnerStorageTime(), 1900)
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
		if _, err := repository.PrepareRunnerCreation(ctx, runnerTestAllocationConfig(), desired,
			ids.NewAt(ids.KindTask, runnerStorageTime(), 199), runnerStorageTime()); !errors.Is(err, errs.New(errs.KindInternal, "")) {
			t.Fatalf("PrepareRunnerCreation(corrupt slot) error = %v", err)
		}
	})
}

// RUN-03: a large occupied pool selects the first free slot, but publication
// cannot overwrite another Runner that wins that slot after the source read.
func TestRunnerNativeCreationRejectsSelectedSlotRace(t *testing.T) {
	for _, race := range []bool{false, true} {
		t.Run(map[bool]string{false: "lowest free slot", true: "concurrent winner"}[race], func(t *testing.T) {
			base, _, tenantID, _ := newRunnerRepositoryFixture(t)
			seedRunnerHostSlots(t, base, 80)
			store := &runnerTransactionAuditStore{memoryHierarchyStore: base}
			winner := ids.NewAt(ids.KindRunner, runnerStorageTime(), 6200)
			if race {
				value, err := testrunners.EncodeRunnerHostSlotRecord(
					testrunners.RunnerHostSlotRecord{Slot: 80, RunnerID: winner},
				)
				if err != nil {
					t.Fatal(err)
				}
				store.allocationRaceKey, store.allocationRaceValue = testrunners.RunnerHostSlotKey(80), value
			}
			repository, err := newRunnerRepository(store)
			if err != nil {
				t.Fatal(err)
			}
			desired := runnerTestDesired(302, testrunners.RunnerOwnerTenant, tenantID, tenantID)
			fixture, err := prepareRunnerCreationFixture(
				t,
				repository,
				runnerTestAllocationConfigWithSlots(96),
				desired,
				303,
				"runner-native-slot-race",
			)
			if err != nil {
				t.Fatal(err)
			}
			result := fixture.publish(t, repository)
			outcome, _, conflict, err := result.Classify()
			if race {
				if err != nil || outcome != IdempotencyKnownConflict || !isKind(conflict, errs.KindStateConflict) {
					t.Fatalf("raced allocation = %v, %v, %v", outcome, conflict, err)
				}
				entry := mustOptionalKey(t, base, testrunners.RunnerHostSlotKey(80))
				owned, err := testrunners.DecodeRunnerHostSlotRecord(entry.Value)
				if err != nil || owned.RunnerID != winner {
					t.Fatalf("selected slot winner = %#v, %v", owned, err)
				}
				if _, err := repository.GetRunner(t.Context(), desired.ID); !isKind(err, errs.KindRunnerNotFound) {
					t.Fatalf("raced creation left a Runner: %v", err)
				}
			} else {
				if err != nil || conflict != nil || outcome != IdempotencyKnownApplied {
					t.Fatalf("allocation = %v, %v, %v", outcome, conflict, err)
				}
				current, err := repository.GetRunner(t.Context(), desired.ID)
				if err != nil || current.Record.Allocation.Slot != 80 {
					t.Fatalf("lowest free slot = %#v, %v", current, err)
				}
			}
		})
	}
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
