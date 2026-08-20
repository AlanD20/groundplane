package cli

import (
	"testing"

	"github.com/spf13/cobra"
)

func TestChangedStringFieldsIncludesOnlyExplicitFlags(t *testing.T) {
	t.Parallel()

	command := &cobra.Command{Use: "edit"}
	command.Flags().String("name", "", "")
	command.Flags().String("description", "default", "")
	if err := command.Flags().Set("name", ""); err != nil {
		t.Fatalf("set name: %v", err)
	}

	fields := changedStringFields(command, map[string]string{
		"name":        "",
		"description": "default",
	})
	if len(fields) != 1 {
		t.Fatalf("fields = %#v, want only explicitly changed name", fields)
	}
	if name, ok := fields["name"]; !ok || name != "" {
		t.Fatalf("name = %q, present = %t; want an explicitly empty field", name, ok)
	}
}
