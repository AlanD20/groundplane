package agentmanagement

import (
	"context"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/localagent"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentoperations"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// Rationale: a queued enrollment must select the qualified image at preparation,
// preserve the original Task/Agent identities and retain its per-operation budget.
func TestAgentEnrollmentCapturesAfterAcceptance(t *testing.T) {
	at := time.Date(2026, 8, 22, 12, 0, 0, 0, time.UTC)
	task := acceptedAgentFixture(at, workexecution.ActionCreate)
	images := &mutableAgentImage{image: testAppAgentImage}
	records := &fakeAgentCaptureRepository{}
	config := localagent.Config{
		PullIntervalSeconds:          2,
		MaxConcurrentTasks:           3,
		MaxParallelOperationsPerTask: 1,
		Labels:                       map[string]string{"arch": "arm64"},
	}
	service := &agentEnrollmentService{images: images, tasks: records}
	images.image = testAgentUpdateDesiredImage
	prepared, err := service.prepareAcceptedEnrollment(
		t.Context(),
		workqueue.TaskSnapshot{Task: task, Revision: 1},
		1,
		at.Add(time.Minute),
		agentEnrollmentRequest{Config: config},
	)
	if err != nil {
		t.Fatal(err)
	}
	request := enrollment(records.capture)
	if prepared.Task.ID != task.ID || request.AgentID != task.TargetID || request.EnrollmentTaskID != task.ID ||
		records.capture.Image != testAgentUpdateDesiredImage || request.Config.Labels["arch"] != "arm64" ||
		!prepared.Task.CreatedAt.Equal(at) || !prepared.Task.Deadline.Equal(task.Deadline) ||
		!prepared.Graph[0].Deadline.Equal(
			at.Add(3*time.Minute),
		) || workexecution.ValidateGraph(prepared.Task, prepared.Graph) != nil {
		t.Fatalf("queued enrollment captured wrong authority: %#v / %#v", prepared, request)
	}
}

// Rationale: equivalent label order must not alter replay identity, while an
// actual changed label must invalidate the captured native input.
func TestAgentEnrollmentReferenceBindsLabelsNotMapOrder(t *testing.T) {
	at := time.Date(2026, 8, 22, 12, 0, 0, 0, time.UTC)
	capture := agentoperations.Capture{
		TaskID:     ids.New(ids.KindTask),
		WorkItemID: ids.New(ids.KindWorkItem),
		PlanID:     ids.New(ids.KindPlan),
		AgentID:    ids.New(ids.KindAgent),
		Action:     workexecution.ActionCreate,
		Image:      testAppAgentImage,
		CreatedAt:  at,
		Enrollment: &agentoperations.Enrollment{
			PullIntervalSeconds:          2,
			MaxConcurrentTasks:           3,
			MaxParallelOperationsPerTask: 1,
			Labels:                       map[string]string{"b": "2", "a": "1"},
		},
	}
	first, err := capture.Reference()
	if err != nil {
		t.Fatal(err)
	}
	capture.Enrollment.Labels = map[string]string{"a": "1", "b": "2"}
	reordered, err := capture.Reference()
	if err != nil || reordered != first {
		t.Fatalf("map ordering changed authority: %v", err)
	}
	capture.Enrollment.Labels["a"] = "changed"
	changed, err := capture.Reference()
	if err != nil || changed == first {
		t.Fatalf("changed label did not alter authority: %v", err)
	}
}

type fakeAgentCaptureRepository struct{ capture agentoperations.Capture }

func (repository *fakeAgentCaptureRepository) PrepareAgentWorkAuthority(
	_ context.Context,
	capture agentoperations.Capture,
) (workqueue.Change, error) {
	repository.capture = capture
	return workqueue.Change{}, nil
}
func acceptedAgentFixture(at time.Time, action workexecution.Action) workexecution.Task {
	task := newAcceptedAgentTask(at, testAgentUpdateID, action, nil)
	task.AcceptedIntent = &workexecution.Input{
		ID:     ids.DeriveAt(ids.KindPlan, at, task.ID, "accepted"),
		SHA256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	}
	return task
}
