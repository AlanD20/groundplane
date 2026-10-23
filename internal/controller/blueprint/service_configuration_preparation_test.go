package blueprint

import (
	"testing"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
)

// Only one Service may capture the shared file baseline before its preceding
// Release settles. A dependency-blocked sibling must not deadlock its producer.
func TestServiceConfigurationPreparationSerializesOnlyEligibleReleases(t *testing.T) {
	current := workexecution.Item{
		ID:        "b",
		TaskID:    "task",
		Operation: workexecution.Release,
		Phase:     workexecution.Waiting,
	}
	for _, test := range []struct {
		name  string
		other workexecution.Item
		wait  bool
	}{
		{"earlier eligible", workexecution.Item{ID: "a", Phase: workexecution.Waiting}, true},
		{"later unprepared", workexecution.Item{ID: "c", Phase: workexecution.Waiting}, false},
		{"earlier depends on current", workexecution.Item{ID: "a", Phase: workexecution.Waiting, Dependencies: []string{"b"}}, false},
		{"later executable", workexecution.Item{ID: "c", Phase: workexecution.Ready, Input: &workexecution.Input{}}, true},
		{"unknown effects", workexecution.Item{ID: "c", Phase: workexecution.Attention, Input: &workexecution.Input{}}, true},
		{"settled", workexecution.Item{ID: "a", Phase: workexecution.Settled, Outcome: workexecution.Succeeded}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			other := test.other
			other.TaskID, other.Operation = current.TaskID, workexecution.Release
			items := map[string]workexecution.Item{current.ID: current, other.ID: other,
				"volume": {
					ID:        "volume",
					TaskID:    current.TaskID,
					Operation: workexecution.Volume,
					Phase:     workexecution.Running,
				}}
			wait, err := serviceConfigurationTurnWaits(current, items)
			if err != nil || wait != test.wait {
				t.Fatalf("wait=%t want=%t error=%v", wait, test.wait, err)
			}
		})
	}
	_, err := serviceConfigurationTurnWaits(current, map[string]workexecution.Item{
		"a": {ID: "a", TaskID: current.TaskID, Operation: workexecution.Release, Dependencies: []string{"missing"}},
	})
	if err == nil {
		t.Fatal("missing dependency was treated as permission to prepare configuration")
	}
}
