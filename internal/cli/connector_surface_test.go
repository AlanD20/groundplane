package cli

import "testing"

// Rationale: immutable Connector names and no CRUD connectivity probe leave
// exactly list/add/show/remove; edit, rename, and check must not appear as
// accidental operator commands.
func TestC15ConnectorCLIHasExactPublicOperations(t *testing.T) {
	want := map[string]bool{"list": true, "add": true, "show": true, "remove": true}
	command := newConnectorCmd()
	if len(command.Commands()) != len(want) {
		t.Fatalf("Connector primary command count = %d, want %d", len(command.Commands()), len(want))
	}
	for _, child := range command.Commands() {
		if !want[child.Name()] {
			t.Errorf("unexpected Connector command %q", child.Name())
		}
	}
}
