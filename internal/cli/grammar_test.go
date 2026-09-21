package cli

import "testing"

// QA: UI-03; local Cobra argument admission only, not action dispatch or effects.
// Rationale: representative operator commands must enforce their documented
// positional arity instead of dropping or reinterpreting targets.
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
