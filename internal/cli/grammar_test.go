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

func TestParityOperandLeavesEnforceExactArity(t *testing.T) {
	t.Parallel()

	root := NewRootCmd(Dependencies{})
	for _, test := range []struct {
		path []string
		want int
	}{
		{path: []string{"tenant", "show"}, want: 1},
		{path: []string{"agent", "config", "show"}, want: 1},
		{path: []string{"agent", "remove"}, want: 1},
		{path: []string{"component", "show"}, want: 1},
		{path: []string{"task", "events"}, want: 1},
		{path: []string{"task", "retry"}, want: 1},
		{path: []string{"service", "attach"}, want: 2},
		{path: []string{"attach", "rename"}, want: 1},
	} {
		command, _, err := root.Find(test.path)
		if err != nil {
			t.Fatalf("find %v: %v", test.path, err)
		}
		operands := make([]string, test.want)
		for index := range operands {
			operands[index] = "target"
		}
		if err := command.Args(command, operands); err != nil {
			t.Errorf("leaf %q rejected %d operands: %v", command.CommandPath(), test.want, err)
		}
		if err := command.Args(command, operands[:test.want-1]); err == nil {
			t.Errorf("leaf %q accepted %d operands", command.CommandPath(), test.want-1)
		}
		if err := command.Args(command, append(operands, "extra")); err == nil {
			t.Errorf("leaf %q accepted %d operands", command.CommandPath(), test.want+1)
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
