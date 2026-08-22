package ipam

import (
	"net/netip"
	"testing"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: every persisted CIDR needs one canonical representation so
// logically identical reservations cannot bypass uniqueness checks.
func TestParseIPv4PrefixCanonicalizesNetworkAddress(t *testing.T) {
	got, err := ParseIPv4Prefix("10.0.1.19/23")
	if err != nil {
		t.Fatalf("ParseIPv4Prefix error: %v", err)
	}
	if want := netip.MustParsePrefix("10.0.0.0/23"); got != want {
		t.Fatalf("prefix = %s, want %s", got, want)
	}
}

// Rationale: the MVP allocation contract is IPv4-only and must reject
// malformed or IPv6 input before any durable reservation is attempted.
func TestParseIPv4PrefixRejectsInvalidInput(t *testing.T) {
	for _, value := range []string{"", "not-a-cidr", "fd00::/64"} {
		_, err := ParseIPv4Prefix(value)
		if kind, ok := errs.KindOf(err); !ok || kind != errs.KindValidationFailed {
			t.Errorf("ParseIPv4Prefix(%q) error = %v, kind = %d, %t", value, err, kind, ok)
		}
	}
}

// Rationale: machine environment and system roots share one host routing
// table and therefore must never overlap, including containment overlaps.
func TestValidateRootPairRejectsOverlap(t *testing.T) {
	err := ValidateRootPair(
		netip.MustParsePrefix("10.0.0.0/16"),
		netip.MustParsePrefix("10.0.8.0/21"),
	)
	if kind, ok := errs.KindOf(err); !ok || kind != errs.KindValidationFailed {
		t.Fatalf("ValidateRootPair error = %v, kind = %d, %t", err, kind, ok)
	}

	if err := ValidateRootPair(
		netip.MustParsePrefix("10.0.0.0/16"),
		netip.MustParsePrefix("10.1.0.0/16"),
	); err != nil {
		t.Fatalf("disjoint roots rejected: %v", err)
	}
}

// Rationale: an Environment or Zone reservation must be both inside its
// allocation parent and disjoint from every already committed reservation.
func TestValidateChildEnforcesContainmentAndNonOverlap(t *testing.T) {
	parent := netip.MustParsePrefix("10.0.0.0/23")
	reserved := []netip.Prefix{netip.MustParsePrefix("10.0.0.0/25")}

	if err := ValidateChild(parent, netip.MustParsePrefix("10.0.1.0/25"), reserved); err != nil {
		t.Fatalf("valid child rejected: %v", err)
	}
	if err := ValidateChild(parent, netip.MustParsePrefix("10.0.2.0/25"), reserved); err == nil {
		t.Fatal("out-of-pool child accepted")
	}
	err := ValidateChild(parent, netip.MustParsePrefix("10.0.0.64/26"), reserved)
	if kind, ok := errs.KindOf(err); !ok || kind != errs.KindStateConflict {
		t.Fatalf("overlap error = %v, kind = %d, %t", err, kind, ok)
	}
}

// Rationale: Console suggestions and generated system allocations must choose
// the same lowest free child regardless of repository listing order.
func TestFirstAvailableChildIsDeterministicAcrossReservationOrder(t *testing.T) {
	parent := netip.MustParsePrefix("10.0.0.0/24")
	reserved := []netip.Prefix{
		netip.MustParsePrefix("10.0.0.64/26"),
		netip.MustParsePrefix("10.0.0.0/27"),
	}
	got, err := FirstAvailableChild(parent, 27, reserved)
	if err != nil {
		t.Fatalf("FirstAvailableChild error: %v", err)
	}
	if want := netip.MustParsePrefix("10.0.0.32/27"); got != want {
		t.Fatalf("prefix = %s, want %s", got, want)
	}
}

// Rationale: exhausted pools must fail as durable state conflicts rather than
// looping over the address space or returning an overlapping reservation.
func TestFirstAvailableChildRejectsExhaustion(t *testing.T) {
	parent := netip.MustParsePrefix("10.0.0.0/30")
	_, err := FirstAvailableChild(parent, 30, []netip.Prefix{parent})
	if kind, ok := errs.KindOf(err); !ok || kind != errs.KindStateConflict {
		t.Fatalf("exhaustion error = %v, kind = %d, %t", err, kind, ok)
	}
}

// Rationale: Caddy allocation must choose the first usable bridge address,
// remain independent of reservation order, and never return reserved bridge
// identities.
func TestFirstAvailableUsableIPv4UsesLowestFreeAddress(t *testing.T) {
	prefix := netip.MustParsePrefix("10.200.30.0/29")
	got, err := FirstAvailableUsableIPv4(prefix, []netip.Addr{
		netip.MustParseAddr("10.200.30.4"),
		netip.MustParseAddr("10.200.30.2"),
	})
	if err != nil {
		t.Fatalf("FirstAvailableUsableIPv4 error: %v", err)
	}
	if want := netip.MustParseAddr("10.200.30.3"); got != want {
		t.Fatalf("address = %s, want %s", got, want)
	}
	for _, reserved := range []string{"10.200.30.0", "10.200.30.1", "10.200.30.7"} {
		if err := ValidateUsableIPv4(prefix, netip.MustParseAddr(reserved)); err == nil {
			t.Fatalf("ValidateUsableIPv4 accepted reserved address %s", reserved)
		}
	}
}

// Rationale: an exhausted Zone must fail without wrapping into its network,
// gateway, or broadcast address.
func TestFirstAvailableUsableIPv4RejectsExhaustion(t *testing.T) {
	prefix := netip.MustParsePrefix("10.200.30.0/30")
	_, err := FirstAvailableUsableIPv4(prefix, []netip.Addr{netip.MustParseAddr("10.200.30.2")})
	if kind, ok := errs.KindOf(err); !ok || kind != errs.KindStateConflict {
		t.Fatalf("exhaustion error = %v, kind = %d, %t", err, kind, ok)
	}
}
