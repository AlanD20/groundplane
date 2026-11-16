package agentmanagement

import (
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/localagent"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/api"
)

// Rationale: removal queued behind an update must remove that update's actual
// current generation, with the original Task identity and a bounded work deadline.
func TestAgentRemovalCapturesCurrentGeneration(t *testing.T) {
	at := time.Date(2026, 8, 22, 15, 0, 0, 0, time.UTC)
	task := acceptedAgentFixture(at, workexecution.ActionRemove)
	targets := &fakeAgentUpdateTargets{health: localagent.Health{Agent: localagent.Agent{
		ID: task.TargetID, Image: testAgentUpdateDesiredImage, Generation: 9}}}
	records := &fakeAgentCaptureRepository{}
	service := &agentRemovalService{targets: targets, tasks: records}
	prepared, err := service.prepareAcceptedRemoval(
		t.Context(),
		workqueue.TaskSnapshot{Task: task, Revision: 1},
		1,
		at.Add(time.Minute),
		api.TaskRunAfterRequest{},
	)
	if err != nil {
		t.Fatal(err)
	}
	if records.capture.AgentID != task.TargetID || records.capture.Image != testAgentUpdateDesiredImage ||
		records.capture.StartingGeneration != 9 ||
		prepared.Task.ID != task.ID ||
		!prepared.Task.CreatedAt.Equal(at) ||
		!prepared.Task.Deadline.Equal(task.Deadline) ||
		!prepared.Graph[0].Deadline.Equal(at.Add(3*time.Minute)) {
		t.Fatalf("late removal capture: %#v / %#v", prepared, records.capture)
	}
}
