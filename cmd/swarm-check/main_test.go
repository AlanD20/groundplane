package main

import (
	"bytes"
	"context"
	"testing"

	"github.com/AlanD20/groundplane/internal/swarmcheck"
)

// Rationale: SIGINT/SIGTERM cancellation must reach manifest and repository
// operations instead of being replaced with a background context in the CLI.
func TestRunPropagatesCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	exitCode := run(
		ctx,
		[]string{"-root", t.TempDir(), "-manifest", ".tmp/swarm/wave-1/manifest.json"},
		&stdout,
		&stderr,
	)
	if exitCode != 1 || stderr.Len() == 0 {
		t.Fatalf("run(canceled) exit/stderr = %d/%q", exitCode, stderr.String())
	}
}

// Rationale: both closed command parsers must reject trailing and removed positional commands.
func TestCLIRejectsTrailingArgumentsForBothCommands(t *testing.T) {
	for _, args := range [][]string{
		{"-manifest", "manifest.json", "removed-command"},
		{"prove-gate", "-manifest", "manifest.json", "-gate", "repository-ci", "extra"},
	} {
		var stdout bytes.Buffer
		var stderr bytes.Buffer
		if exitCode := run(context.Background(), args, &stdout, &stderr); exitCode != 2 {
			t.Fatalf("run(%v) exit = %d, stderr = %q", args, exitCode, stderr.String())
		}
	}
}

// Rationale: lane-scoped validation is meaningful only while writer worktrees are mutable.
func TestLaneSelectionIsRejectedOutsideWriting(t *testing.T) {
	for _, phase := range []swarmcheck.Phase{
		swarmcheck.PhaseDispatch,
		swarmcheck.PhaseReview,
		swarmcheck.PhaseIntegrate,
		swarmcheck.PhaseComplete,
	} {
		if err := validateLaneSelection(phase, "lane-a"); err == nil {
			t.Fatalf("phase %s accepted -lane", phase)
		}
	}
	if err := validateLaneSelection(swarmcheck.PhaseWriting, "lane-a"); err != nil {
		t.Fatalf("writing rejected -lane: %v", err)
	}
}
