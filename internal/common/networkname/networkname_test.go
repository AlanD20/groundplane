package networkname

import "testing"

func TestNewPreservesCanonicalNetworkID(t *testing.T) {
	const networkID = "net_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	name, err := New(networkID)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if name != "gp_net_"+networkID {
		t.Fatalf("New() = %q, want gp_net_%s", name, networkID)
	}
}

func TestNewRejectsNoncanonicalNetworkID(t *testing.T) {
	for _, networkID := range []string{
		"net_01arz3ndektsv4rrffq69g5fav",
		"operator-selected",
	} {
		if _, err := New(networkID); err == nil {
			t.Fatalf("New(%q) error = nil", networkID)
		}
	}
}
