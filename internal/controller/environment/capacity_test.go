package environment

import (
	"testing"
)

// Rationale: capacity is a deterministic projection of CIDR reservation size,
// independent of iteration order or host-address policy.
func TestProjectNetworkCapacityCountsReservedCIDRAddresses(t *testing.T) {
	t.Parallel()
	got, err := ProjectNetworkCapacity("10.40.0.0/16", []string{"10.40.8.0/24", "10.40.0.0/23"})
	if err != nil {
		t.Fatalf("ProjectNetworkCapacity() error = %v", err)
	}
	if got.TotalAddresses != 65536 || got.AllocatedAddresses != 768 ||
		got.AvailableAddresses != 64768 || got.ZoneCount != 2 {
		t.Fatalf("ProjectNetworkCapacity() = %#v", got)
	}
}

// Rationale: durable corruption must fail closed instead of producing wrapped
// or underflowed capacity numbers.
func TestProjectNetworkCapacityRejectsReservationOutsidePool(t *testing.T) {
	t.Parallel()
	if _, err := ProjectNetworkCapacity("10.40.0.0/16", []string{"10.41.0.0/24"}); err == nil {
		t.Fatal("ProjectNetworkCapacity() error = nil")
	}
}

func TestProjectNetworkCapacityRejectsOverlappingReservations(t *testing.T) {
	t.Parallel()
	if _, err := ProjectNetworkCapacity(
		"10.40.0.0/16",
		[]string{"10.40.0.0/24", "10.40.0.0/25"},
	); err == nil {
		t.Fatal("ProjectNetworkCapacity() error = nil")
	}
}
