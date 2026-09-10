package controllerupgrade

import (
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
)

// Rationale: journal phases are recovery authority; a stale helper cannot
// resurrect a cancelled/completed trial or skip predecessor restoration.
func TestJournalTransitions(t *testing.T) {
	allowed := map[Phase][]Phase{
		PhasePrepared:    {PhaseActivating, PhaseCancelled, PhaseRollingBack},
		PhaseActivating:  {PhaseTrial, PhaseRollingBack},
		PhaseTrial:       {PhaseStarting, PhaseRollingBack},
		PhaseStarting:    {PhaseHealthy, PhaseRollingBack},
		PhaseRollingBack: {PhaseRolledBack},
		PhaseRolledBack:  {PhaseRecovered},
	}
	phases := []Phase{PhasePrepared, PhaseActivating, PhaseTrial, PhaseStarting, PhaseHealthy,
		PhaseRollingBack, PhaseRolledBack, PhaseRecovered, PhaseCancelled, "invalid"}
	for _, from := range phases {
		for _, to := range phases {
			want := false
			for _, next := range allowed[from] {
				if next == to {
					want = true
				}
			}
			if from.CanAdvance(to) != want {
				t.Fatalf("%s -> %s = %t, want %t", from, to, !want, want)
			}
		}
	}
}

// Rationale: corrupt recovery metadata cannot authorize executing a path,
// changing another Task/Agent or trusting incompatible candidate writes.
func TestJournalValidation(t *testing.T) {
	valid := validJournal(t)
	if err := valid.Validate(); err != nil {
		t.Fatal(err)
	}
	for name, change := range map[string]func(*Journal){
		"task":        func(value *Journal) { value.TaskID = "../../other" },
		"release":     func(value *Journal) { value.Release = "tag:latest" },
		"predecessor": func(value *Journal) { value.PreviousController = "" },
		"phase":       func(value *Journal) { value.Phase = "future" },
		"deadline":    func(value *Journal) { value.Deadline = value.StartedAt.Add(601 * time.Second) },
		"schema":      func(value *Journal) { value.Schema = 2 },
		"agent":       func(value *Journal) { value.Agent = &AgentPredecessor{ID: "other"} },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := valid
			change(&candidate)
			if candidate.Validate() == nil {
				t.Fatal("unsafe journal accepted")
			}
		})
	}
}

func validJournal(t *testing.T) Journal {
	t.Helper()
	now := time.Date(2026, 9, 10, 0, 0, 0, 0, time.UTC)
	return Journal{Schema: 1, TaskID: ids.NewAt(ids.KindTask, now, 1), Phase: PhasePrepared,
		Release: Hash([]byte("release")), PreviousController: Hash([]byte("previous")),
		Manifest: Manifest{
			Schema:            1,
			ControllerSHA256:  Hash([]byte("candidate")),
			ControllerVersion: "0.1.0",
			AgentImage: "registry.example/agent@sha256:" + strings.Repeat(
				"a",
				64,
			),
			StorageEpoch:  1,
			ChannelSchema: 1,
		},
		StartedAt: now, Deadline: now.Add(600 * time.Second)}
}
