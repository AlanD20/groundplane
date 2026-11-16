package agentmanagement

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/localagent"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: an enrollment uses the current qualified release selection, while
// an independently requested update never substitutes that implicit selection.
func TestAgentActionsSelectCurrentReleaseImage(t *testing.T) {
	at := time.Date(2026, 8, 23, 10, 0, 0, 0, time.UTC)
	task := acceptedAgentFixture(at, workexecution.ActionCreate)
	images := &mutableAgentImage{image: testAgentUpdatePreviousImage}
	enrollRecords, updateRecords := &fakeAgentCaptureRepository{}, &fakeAgentCaptureRepository{}
	enroll := &agentEnrollmentService{images: images, tasks: enrollRecords}
	update := &agentUpdateService{targets: &fakeAgentUpdateTargets{health: localagent.Health{Agent: localagent.Agent{
		ID: task.TargetID, Image: testAgentUpdatePreviousImage, Generation: 1, Phase: localagent.PhaseReady}}}, tasks: updateRecords, images: &fakeAgentUpdateTargets{}}
	images.image = testAgentUpdateDesiredImage
	input := agentEnrollmentRequest{
		Config: localagent.Config{PullIntervalSeconds: 2, MaxConcurrentTasks: 3, MaxParallelOperationsPerTask: 1},
	}
	if _, err := enroll.prepareAcceptedEnrollment(t.Context(), workqueue.TaskSnapshot{Task: task, Revision: 1}, 1, at.Add(time.Second), input); err != nil {
		t.Fatal(err)
	}
	task.Action = workexecution.ActionUpdate
	if _, err := update.prepareAcceptedUpdate(t.Context(), workqueue.TaskSnapshot{Task: task, Revision: 1}, 1, at.Add(time.Second), api.AgentUpdate{Image: testAgentUpdateDesiredImage}); err != nil {
		t.Fatal(err)
	}
	if enrollRecords.capture.Image != images.image || updateRecords.capture.Image != images.image {
		t.Fatal("startup image selected")
	}
	images.err = errs.New(errs.KindResourceInUse, "native recovery owns image selection")
	if _, err := enroll.prepareAcceptedEnrollment(t.Context(), workqueue.TaskSnapshot{Task: task, Revision: 1}, 1, at.Add(time.Second), input); !errors.Is(
		err,
		images.err,
	) {
		t.Fatalf("blocked enrollment: %v", err)
	}
	if _, err := update.prepareAcceptedUpdate(t.Context(), workqueue.TaskSnapshot{Task: task, Revision: 1}, 1, at.Add(time.Second), api.AgentUpdate{Image: testAgentUpdateDesiredImage}); err != nil {
		t.Fatalf("independent update: %v", err)
	}
}

type mutableAgentImage struct {
	image string
	err   error
}

func (source *mutableAgentImage) DesiredAgentImage(context.Context) (string, error) {
	return source.image, source.err
}
