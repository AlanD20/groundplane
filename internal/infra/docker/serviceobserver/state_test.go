package serviceobserver

import (
	"testing"

	"github.com/AlanD20/groundplane/internal/common/serviceobservation"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"github.com/moby/moby/api/types/container"
)

// Rationale: transitional states must not masquerade as healthy, and conflicting
// Docker flags must produce unavailable rather than a plausible state bucket.
func TestCountStateRejectsContradictoryFlags(t *testing.T) {
	for _, test := range []struct {
		name  string
		state container.State
		valid bool
	}{
		{"created", container.State{Status: container.StateCreated}, true},
		{"paused", container.State{Status: container.StatePaused, Running: true, Paused: true}, true},
		{"removing", container.State{Status: container.StateRemoving}, true},
		{"dead", container.State{Status: container.StateDead, Dead: true}, true},
		{"created running", container.State{Status: container.StateCreated, Running: true}, false},
		{"paused stopped", container.State{Status: container.StatePaused, Paused: true}, false},
		{"restarting paused", container.State{Status: container.StateRestarting, Running: true, Restarting: true, Paused: true}, false},
		{"dead running", container.State{Status: container.StateDead, Dead: true, Running: true}, false},
		{"exited running", container.State{Status: container.StateExited, Running: true}, false},
		{"running paused", container.State{Status: container.StateRunning, Running: true, Paused: true}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			counts := &agentpb.ServiceReplicaCounts{}
			if got := countState(counts, &test.state); got != test.valid ||
				got && serviceobservation.Total(counts) != 1 ||
				!got && serviceobservation.Total(counts) != 0 {
				t.Fatalf("valid=%v counts=%v", got, counts)
			}
		})
	}
}
