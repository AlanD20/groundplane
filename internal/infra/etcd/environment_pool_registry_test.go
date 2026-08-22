package etcd

import (
	"errors"
	"net/netip"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: Environment allocations are operator-selected, so the durable
// uniqueness index must reject both overlaps and pools outside the machine root.
func TestEnvironmentPoolRegistryEnforcesGlobalDisjointReservations(t *testing.T) {
	t.Parallel()
	root := netip.MustParsePrefix("10.0.0.0/8")
	firstID := ids.NewAt(ids.KindEnvironment, time.Unix(1, 0).UTC(), 1)
	secondID := ids.NewAt(ids.KindEnvironment, time.Unix(2, 0).UTC(), 2)
	registry, canonical, err := (EnvironmentPoolRegistry{Reservations: map[string]string{}}).Reserve(
		root,
		firstID,
		"10.200.0.0/16",
	)
	if err != nil || canonical != "10.200.0.0/16" {
		t.Fatalf("Reserve(first) = %#v, %q, %v", registry, canonical, err)
	}
	if _, _, err := registry.Reserve(
		root,
		secondID,
		"10.200.8.0/21",
	); !errors.Is(
		err,
		errs.New(errs.KindStateConflict, ""),
	) {
		t.Fatalf("Reserve(overlap) error = %v", err)
	}
	if _, _, err := registry.Reserve(
		root,
		secondID,
		"192.168.0.0/24",
	); !errors.Is(
		err,
		errs.New(errs.KindValidationFailed, ""),
	) {
		t.Fatalf("Reserve(outside root) error = %v", err)
	}
}
