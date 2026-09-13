package cli

import "testing"

// Delivery: locked CLI flag namespace only; no Route request or routing effect is exercised.
// Rationale: Route creation must retain the global Controller --host while
// accepting a distinct public hostname, including when both flags are used.
func TestRouteAddDoesNotShadowControllerHost(t *testing.T) {
	root := NewRootCmd(Dependencies{})
	add, _, err := root.Find([]string{"route", "add"})
	if err != nil {
		t.Fatalf("find route add: %v", err)
	}
	if add.LocalNonPersistentFlags().Lookup("host") != nil {
		t.Fatal("route add shadows global --host")
	}
	if add.LocalNonPersistentFlags().Lookup("hostname") == nil {
		t.Fatal("route add is missing --hostname")
	}
	if root.PersistentFlags().Lookup("host") == nil {
		t.Fatal("root is missing global --host")
	}
}
