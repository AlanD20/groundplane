package runnerallocation

import (
	"errors"
	"net/netip"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: machine startup inputs must fail before any scarce Runner
// allocation can be published, while valid inputs deterministically map every
// host slot to its fixed UID/GID ranges and dedicated /29.
func TestRunnerAllocationConfigValidationAndAllocation(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		mutate func(*RunnerAllocationConfig)
		kind   errs.Kind
	}{
		{name: "valid"},
		{
			name: "runner pool is not a child",
			mutate: func(config *RunnerAllocationConfig) {
				config.RunnerPool = netip.MustParsePrefix("192.0.2.0/24")
			},
			kind: errs.KindValidationFailed,
		},
		{
			name: "runner pool is more specific than allocations",
			mutate: func(config *RunnerAllocationConfig) {
				config.RunnerPool = netip.MustParsePrefix("10.240.0.0/30")
			},
			kind: errs.KindValidationFailed,
		},
		{
			name: "runner pool has fewer networks than host slots",
			mutate: func(config *RunnerAllocationConfig) {
				config.RunnerPool = netip.MustParsePrefix("10.240.0.0/29")
			},
			kind: errs.KindValidationFailed,
		},
		{
			name: "host range is reversed",
			mutate: func(config *RunnerAllocationConfig) {
				config.HostPool.HostUIDEnd = config.HostPool.HostUIDStart - 1
			},
			kind: errs.KindValidationFailed,
		},
		{
			name: "host range has fewer than five slots",
			mutate: func(config *RunnerAllocationConfig) {
				config.HostPool.HostUIDEnd = config.HostPool.HostUIDStart + 3
				config.HostPool.SubUIDEnd = config.HostPool.SubUIDStart + 4*RunnerSubordinateBlockSize - 1
				config.HostPool.SubGIDEnd = config.HostPool.SubGIDStart + 4*RunnerSubordinateBlockSize - 1
			},
			kind: errs.KindValidationFailed,
		},
		{
			name: "subordinate range is not an exact slot multiple",
			mutate: func(config *RunnerAllocationConfig) {
				config.HostPool.SubUIDEnd--
			},
			kind: errs.KindValidationFailed,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			config := testAllocationConfig()
			if test.mutate != nil {
				test.mutate(&config)
			}
			slots, err := config.Validate()
			if test.kind == 0 {
				if err != nil || slots != 8 {
					t.Fatalf("Validate() = %d, %v, want 8, nil", slots, err)
				}
				return
			}
			if !errors.Is(err, errs.New(test.kind, "")) || slots != 0 {
				t.Fatalf("Validate() = %d, %v, want 0, %v", slots, err, test.kind)
			}
		})
	}

	allocationTests := []struct {
		name    string
		slot    uint32
		network netip.Prefix
		kind    errs.Kind
	}{
		{name: "deterministic second slot", slot: 1, network: netip.MustParsePrefix("10.240.0.8/29")},
		{
			name: "exhausted host slots", slot: 8, network: netip.MustParsePrefix("10.240.0.64/29"),
			kind: errs.KindResourceInUse,
		},
		{
			name: "invalid network size", slot: 0, network: netip.MustParsePrefix("10.240.0.0/28"),
			kind: errs.KindValidationFailed,
		},
	}
	for _, test := range allocationTests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			allocation, err := testAllocationConfig().HostPool.Allocation(test.slot, test.network)
			if test.kind != 0 {
				if !errors.Is(err, errs.New(test.kind, "")) {
					t.Fatalf("Allocation() error = %v, want %v", err, test.kind)
				}
				return
			}
			if err != nil || allocation.Slot != 1 || allocation.HostUID != 200001 ||
				allocation.SubUIDStart != 365536 || allocation.SubUIDCount != RunnerSubordinateBlockSize ||
				allocation.SubGIDStart != 965536 || allocation.SubGIDCount != RunnerSubordinateBlockSize ||
				allocation.NetworkCIDR != test.network.String() {
				t.Fatalf("Allocation() = %#v, %v", allocation, err)
			}
		})
	}
}

// Rationale: the system registry must always choose the lowest available /29,
// replay an existing ownership claim exactly, and reject release by any other
// subnet identity.
func TestSystemPoolRegistryReserveAndReleaseRunner(t *testing.T) {
	t.Parallel()
	runnerPool := netip.MustParsePrefix("10.240.0.0/16")
	firstID := testRunnerID(1)
	secondID := testRunnerID(2)
	registry := SystemPoolRegistry{
		RunnerNetworkPool: runnerPool.String(),
		Reservations:      map[string]string{},
	}

	first, firstPrefix, err := registry.ReserveRunner(runnerPool, firstID)
	if err != nil || firstPrefix.String() != "10.240.0.0/29" ||
		first.Reservations[RunnerReservationOwner(firstID)] != firstPrefix.String() {
		t.Fatalf("ReserveRunner(first) = %#v, %s, %v", first, firstPrefix, err)
	}
	second, secondPrefix, err := first.ReserveRunner(runnerPool, secondID)
	if err != nil || secondPrefix.String() != "10.240.0.8/29" ||
		second.Reservations[RunnerReservationOwner(firstID)] != firstPrefix.String() ||
		second.Reservations[RunnerReservationOwner(secondID)] != secondPrefix.String() {
		t.Fatalf("ReserveRunner(second) = %#v, %s, %v", second, secondPrefix, err)
	}
	replay, replayPrefix, err := second.ReserveRunner(runnerPool, firstID)
	if err != nil || replayPrefix != firstPrefix ||
		replay.Reservations[RunnerReservationOwner(firstID)] != firstPrefix.String() ||
		replay.Reservations[RunnerReservationOwner(secondID)] != secondPrefix.String() {
		t.Fatalf("ReserveRunner(replay) = %#v, %s, %v", replay, replayPrefix, err)
	}
	if _, err := second.ReleaseRunner(
		firstID,
		secondPrefix.String(),
	); !errors.Is(
		err,
		errs.New(errs.KindStateConflict, ""),
	) {
		t.Fatalf("ReleaseRunner(wrong subnet) error = %v", err)
	}
	released, err := second.ReleaseRunner(firstID, firstPrefix.String())
	if err != nil || released.Reservations[RunnerReservationOwner(firstID)] != "" ||
		released.Reservations[RunnerReservationOwner(secondID)] != secondPrefix.String() {
		t.Fatalf("ReleaseRunner() = %#v, %v", released, err)
	}
}

// Rationale: durable reservation corruption must fail closed before allocation
// logic can select a prefix from malformed or overlapping ownership evidence.
func TestSystemPoolRegistryRejectsInvalidReservations(t *testing.T) {
	t.Parallel()
	root := netip.MustParsePrefix("10.128.0.0/9")
	runnerID := testRunnerID(10)
	tests := []struct {
		name     string
		registry SystemPoolRegistry
	}{
		{
			name: "nil reservations",
			registry: SystemPoolRegistry{
				RunnerNetworkPool: "10.240.0.0/16",
			},
		},
		{
			name: "invalid runner pool",
			registry: SystemPoolRegistry{
				RunnerNetworkPool: "10.240.0.1/16", Reservations: map[string]string{},
			},
		},
		{
			name: "empty owner",
			registry: SystemPoolRegistry{
				RunnerNetworkPool: "10.240.0.0/16", Reservations: map[string]string{"": "10.200.0.0/24"},
			},
		},
		{
			name: "invalid runner owner",
			registry: SystemPoolRegistry{
				RunnerNetworkPool: "10.240.0.0/16",
				Reservations:      map[string]string{"runner/not-an-id": "10.240.0.0/29"},
			},
		},
		{
			name: "runner reservation outside pool",
			registry: SystemPoolRegistry{
				RunnerNetworkPool: "10.240.0.0/16",
				Reservations: map[string]string{
					RunnerReservationOwner(runnerID): "10.241.0.0/29",
				},
			},
		},
		{
			name: "non-runner overlaps runner pool",
			registry: SystemPoolRegistry{
				RunnerNetworkPool: "10.240.0.0/16", Reservations: map[string]string{"component/dns": "10.240.0.0/29"},
			},
		},
		{
			name: "reservations overlap",
			registry: SystemPoolRegistry{
				RunnerNetworkPool: "10.240.0.0/16",
				Reservations: map[string]string{
					"component/one": "10.200.0.0/24",
					"component/two": "10.200.0.0/24",
				},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if err := test.registry.Validate(root); !errors.Is(err, errs.New(errs.KindInternal, "")) {
				t.Fatalf("Validate() error = %v, want internal", err)
			}
		})
	}
}

// Rationale: the combined Tenant quota must be sorted, idempotent for an
// existing Runner, bounded at five claims, and release only its exact owner.
func TestRunnerTenantQuotaBoundaries(t *testing.T) {
	t.Parallel()
	quota := RunnerTenantQuota{RunnerIDs: []string{}}
	idsInQuota := make([]string, MaximumTenantRunners+1)
	for index := range idsInQuota {
		idsInQuota[index] = testRunnerID(int64(100 + index))
	}
	for index := 0; index < MaximumTenantRunners; index++ {
		var err error
		quota, err = quota.Claim(idsInQuota[index])
		if err != nil || len(quota.RunnerIDs) != index+1 {
			t.Fatalf("Claim(%d) = %#v, %v", index, quota, err)
		}
	}
	if _, err := quota.Claim(idsInQuota[MaximumTenantRunners]); !errors.Is(err, errs.New(errs.KindResourceInUse, "")) {
		t.Fatalf("Claim(over quota) error = %v", err)
	}
	replay, err := quota.Claim(idsInQuota[0])
	if err != nil || len(replay.RunnerIDs) != MaximumTenantRunners {
		t.Fatalf("Claim(replay) = %#v, %v", replay, err)
	}
	released, err := quota.Release(idsInQuota[2])
	if err != nil || len(released.RunnerIDs) != MaximumTenantRunners-1 {
		t.Fatalf("Release() = %#v, %v", released, err)
	}
	if _, err := released.Release(idsInQuota[2]); !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("Release(missing) error = %v", err)
	}
	invalid := []RunnerTenantQuota{
		{RunnerIDs: []string{idsInQuota[1], idsInQuota[0]}},
		{RunnerIDs: []string{idsInQuota[0], idsInQuota[0]}},
		{RunnerIDs: append([]string(nil), idsInQuota...)},
	}
	for index, candidate := range invalid {
		if err := candidate.Validate(); !errors.Is(err, errs.New(errs.KindInternal, "")) {
			t.Fatalf("Validate(invalid %d) error = %v", index, err)
		}
	}
}

func testAllocationConfig() RunnerAllocationConfig {
	return RunnerAllocationConfig{
		SystemPool: netip.MustParsePrefix("10.128.0.0/9"),
		RunnerPool: netip.MustParsePrefix("10.240.0.0/16"),
		HostPool: RunnerHostPoolConfig{
			HostUIDStart: 200000, HostUIDEnd: 200007,
			SubUIDStart: 300000, SubUIDEnd: 824287,
			SubGIDStart: 900000, SubGIDEnd: 1424287,
		},
	}
}

func testRunnerID(entropy int64) string {
	return ids.NewAt(ids.KindRunner, time.Date(2026, time.August, 26, 12, 0, 0, 0, time.UTC), entropy)
}
