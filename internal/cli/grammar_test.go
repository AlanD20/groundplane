package cli

import (
	"strings"
	"testing"

	"github.com/spf13/cobra"
)

func TestRunnableZeroArgumentLeavesRejectOperands(t *testing.T) {
	t.Parallel()

	root := NewRootCmd(Dependencies{})
	walkCommands(root, func(command *cobra.Command) {
		if command.Run == nil && command.RunE == nil {
			return
		}
		if len(command.Commands()) != 0 {
			return
		}
		if command.Args == nil {
			t.Errorf("runnable leaf %q has no argument validator", command.CommandPath())
			return
		}
		if hasPositionalOperand(command.Use) {
			return
		}
		if err := command.Args(command, []string{"unexpected-operand"}); err == nil {
			t.Errorf("zero-argument leaf %q accepted an operand", command.CommandPath())
		}
	})
}

func TestRepresentativeExactOneArgumentLeavesRemainExact(t *testing.T) {
	t.Parallel()

	root := NewRootCmd(Dependencies{})
	for _, path := range [][]string{
		{"tenant", "show"},
		{"agent", "remove"},
		{"task", "retry"},
	} {
		command, _, err := root.Find(path)
		if err != nil {
			t.Fatalf("find %v: %v", path, err)
		}
		if err := command.Args(command, []string{"target"}); err != nil {
			t.Errorf("exact-one leaf %q rejected one operand: %v", command.CommandPath(), err)
		}
		if err := command.Args(command, nil); err == nil {
			t.Errorf("exact-one leaf %q accepted zero operands", command.CommandPath())
		}
		if err := command.Args(command, []string{"one", "two"}); err == nil {
			t.Errorf("exact-one leaf %q accepted two operands", command.CommandPath())
		}
	}
}

func walkCommands(command *cobra.Command, visit func(*cobra.Command)) {
	visit(command)
	for _, child := range command.Commands() {
		walkCommands(child, visit)
	}
}

func hasPositionalOperand(use string) bool {
	for _, field := range strings.Fields(use) {
		if strings.HasPrefix(field, "<") || strings.HasPrefix(field, "[") {
			return true
		}
	}
	return false
}
