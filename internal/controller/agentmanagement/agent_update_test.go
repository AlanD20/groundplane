package agentmanagement

import (
	"context"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/localagent"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/api"
)

const (
	testAgentUpdateID            = "agt_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	testAgentUpdatePreviousImage = "ghcr.io/aland20/groundplane-agent@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	testAgentUpdateDesiredImage  = "ghcr.io/aland20/groundplane-agent@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
)

// Rationale: update must capture the generation and predecessor that exist
// after prerequisite completion, not the state that existed at acceptance.
func TestAgentUpdateCapturesCurrentPredecessor(t *testing.T) {
	at := time.Date(2026, 8, 23, 10, 0, 0, 0, time.UTC)
	task := acceptedAgentFixture(at, workexecution.ActionUpdate)
	targets := &fakeAgentUpdateTargets{health: localagent.Health{Agent: localagent.Agent{
		ID: testAgentUpdateID, Image: testAgentUpdatePreviousImage, Generation: 7, Phase: localagent.PhaseReady}}}
	records := &fakeAgentCaptureRepository{}
	service := &agentUpdateService{targets: targets, tasks: records, images: targets}
	prepared, err := service.prepareAcceptedUpdate(
		t.Context(),
		workqueue.TaskSnapshot{Task: task, Revision: 1},
		1,
		at.Add(time.Minute),
		api.AgentUpdate{Image: testAgentUpdateDesiredImage},
	)
	if err != nil {
		t.Fatal(err)
	}
	request := replacement(records.capture)
	if request.AgentID != task.TargetID || request.PreviousImage != testAgentUpdatePreviousImage ||
		request.DesiredImage != testAgentUpdateDesiredImage ||
		request.StartingGeneration != 7 ||
		prepared.Task.ID != task.ID ||
		!prepared.Task.Deadline.Equal(task.Deadline) ||
		!prepared.Graph[0].Deadline.Equal(at.Add(6*time.Minute)) ||
		targets.calls != 1 ||
		targets.verifyCalls != 1 {
		t.Fatalf("late update capture: %#v / %#v", prepared, request)
	}
}

// Rationale: deterministic generation overflow, same-image selection and a
// non-ready predecessor must fail before publishing any execution authority.
func TestAgentUpdateRefusesInvalidReplacementBeforePublication(t *testing.T) {
	for _, scenario := range []string{"generation overflow", "same image", "not ready"} {
		t.Run(scenario, func(t *testing.T) {
			at := time.Date(2026, 8, 23, 10, 0, 0, 0, time.UTC)
			task := acceptedAgentFixture(at, workexecution.ActionUpdate)
			agent := localagent.Agent{
				ID:         task.TargetID,
				Image:      testAgentUpdatePreviousImage,
				Generation: 7,
				Phase:      localagent.PhaseReady,
			}
			image := testAgentUpdateDesiredImage
			switch scenario {
			case "generation overflow":
				agent.Generation = ^uint64(0) - 1
			case "same image":
				image = agent.Image
			case "not ready":
				agent.Phase = localagent.PhaseUpdating
			}
			targets := &fakeAgentUpdateTargets{health: localagent.Health{Agent: agent}}
			records := &fakeAgentCaptureRepository{}
			service := &agentUpdateService{targets: targets, tasks: records, images: targets}
			_, err := service.prepareAcceptedUpdate(
				t.Context(),
				workqueue.TaskSnapshot{Task: task, Revision: 1},
				1,
				at.Add(time.Minute),
				api.AgentUpdate{Image: image},
			)
			if err == nil || records.capture.TaskID != "" {
				t.Fatalf("invalid capture published: %#v / %v", records.capture, err)
			}
		})
	}
}

type fakeAgentUpdateTargets struct {
	health             localagent.Health
	err                error
	calls, verifyCalls int
}

func (targets *fakeAgentUpdateTargets) Verify(context.Context, string, int) error {
	targets.verifyCalls++
	return nil
}
func (targets *fakeAgentUpdateTargets) Health(context.Context, string) (localagent.Health, error) {
	targets.calls++
	return targets.health, targets.err
}
