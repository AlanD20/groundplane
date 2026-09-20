package app

import (
	"context"
	"errors"
	"testing"

	"github.com/AlanD20/groundplane/internal/controller/idempotentintent"
	"github.com/AlanD20/groundplane/internal/controller/localagent"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: enrollment uses its qualified bootstrap selection, but independent
// Agent updates must pin the operator's image instead of the Controller release.
func TestAgentActionsSelectCurrentReleaseImage(t *testing.T) {
	images := &mutableAgentImage{image: testAgentUpdatePreviousImage}
	enrollTasks, updateTasks := &fakeAgentEnrollmentTasks{}, &fakeAgentUpdateTasks{}
	enroll, err := newAgentEnrollmentService(
		images,
		localagent.Config{PullIntervalSeconds: 2, MaxConcurrentTasks: 3},
		enrollTasks,
		&fakeAgentEnrollmentIdempotency{
			resolution: idempotentintent.Resolution{Kind: idempotentintent.ResolutionApplied},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	update, err := newAgentUpdateService(
		&fakeAgentUpdateTargets{health: localagent.Health{Agent: localagent.Agent{
			ID: testAgentUpdateID, Image: testAgentUpdatePreviousImage, Generation: 1, Phase: localagent.PhaseReady,
		}}},
		updateTasks,
		&fakeAgentUpdateIdempotency{resolution: idempotentintent.Resolution{Kind: idempotentintent.ResolutionApplied}},
	)
	if err != nil {
		t.Fatal(err)
	}
	images.image = testAgentUpdateDesiredImage
	if _, err := enroll.EnrollAgent(context.Background(), "release-enrollment-0001"); err != nil {
		t.Fatal(err)
	}
	if _, err := update.UpdateAgent(context.Background(), testAgentUpdateID, testAgentUpdateDesiredImage, "release-agent-update-0001"); err != nil {
		t.Fatal(err)
	}
	if enrollTasks.task.Params[agentTaskImageKey] != images.image ||
		updateTasks.task.Params[agentTaskImageKey] != images.image {
		t.Fatal("Agent action used startup-captured image")
	}
	images.err = errs.New(errs.KindResourceInUse, "native release recovery owns selection")
	if _, err := enroll.EnrollAgent(context.Background(), "release-enrollment-0002"); !errors.Is(err, images.err) {
		t.Fatalf("blocked enrollment = %v", err)
	}
	if _, err := update.UpdateAgent(context.Background(), testAgentUpdateID, testAgentUpdateDesiredImage, "release-agent-update-0002"); err != nil {
		t.Fatalf("independent Agent selection = %v", err)
	}
}

type mutableAgentImage struct {
	image string
	err   error
}

func (source *mutableAgentImage) DesiredAgentImage(context.Context) (string, error) {
	return source.image, source.err
}

// Rationale: native activation cannot use ordinary retry to clone stale host
// authority. The API must reject before either intent or Task publication.
func TestTaskRetryRejectsNativeControllerUpdate(t *testing.T) {
	const taskID = "task_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	repository := &fakeTaskRetryRepository{}
	repository.task.ID = taskID
	repository.task.Params = map[string]string{"resource_kind": "controller"}
	service, err := newTaskRetryService(repository, &fakeTaskRetryIdempotency{}, &fakeBackupTaskRetryer{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.RetryTask(context.Background(), taskID, "native-retry-refused-0001"); !errors.Is(
		err,
		errs.New(errs.KindTaskNotRetryable, ""),
	) ||
		repository.mutationCalls != 0 {
		t.Fatalf("native retry = %v, writes %d", err, repository.mutationCalls)
	}
}
