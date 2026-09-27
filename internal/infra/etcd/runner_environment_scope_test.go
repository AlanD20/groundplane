package etcd

import (
	"context"
	"errors"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runners"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// RUN-01/05/07: an Environment Runner must keep its full owner chain through
// publication, failed creation, retry and listing, without widening its owner.
func TestRunnerEnvironmentOwnershipSurvivesRetry(t *testing.T) {
	ctx := context.Background()
	store, repository, tenantID, projectID := newRunnerRepositoryFixture(t)
	environmentID := runnerTestEnvironment(t, store, tenantID, projectID)
	desired := runnerTestDesired(800, runners.RunnerOwnerEnvironment, environmentID, tenantID)
	desired.ParentProjectID = projectID
	task := runnerTestTask(desired, taskjournal.TaskCreate, 801, "environment-runner-create")
	if _, err := repository.CreateRunnerWithTask(ctx, runnerTestAllocationConfig(), desired, task, runnerTestMarker(task, desired)); err != nil {
		t.Fatal(err)
	}
	failed := runnerTestFinishCreate(t, store, repository, task, taskjournal.TaskStatusFailed)
	retry := runnerTestTask(desired, taskjournal.TaskCreate, 802, task.IdempotencyKey)
	retry.RetryOf, retry.OperationID = task.ID, task.OperationID
	marker := runnerTestMarker(retry, desired)
	marker.Locator.Key = "environment-runner-retry"
	if _, err := repository.RetryRunnerCreationWithTask(ctx, task.ID, retry, marker); err != nil {
		t.Fatal(err)
	}
	tasks, err := newTaskRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	published, err := tasks.GetTask(ctx, retry.ID)
	if err != nil || published.Record.Owner != (taskjournal.TaskOwner{
		WorkspaceType: taskjournal.TaskWorkspaceTenant, TenantID: tenantID, ProjectID: projectID, EnvironmentID: environmentID,
	}) {
		t.Fatalf("retry owner = %#v, %v", published.Record.Owner, err)
	}
	current, err := repository.GetRunner(ctx, desired.ID)
	if err != nil || current.Record.Allocation != failed.Record.Allocation ||
		current.Record.Desired.ParentProjectID != projectID {
		t.Fatalf("retry lost Environment owner or allocation: %#v, %v", current, err)
	}
	for _, filter := range []runners.RunnerFilter{{EnvironmentID: environmentID}, {TenantID: tenantID}} {
		page, err := repository.ListRunners(ctx, filter, keyvalue.PageRequest{})
		if err != nil || len(page.Items) != 1 || page.Items[0].Record.Desired.ID != desired.ID {
			t.Fatalf("scoped listing = %#v, %v", page, err)
		}
	}
	page, err := repository.ListRunners(
		ctx,
		runners.RunnerFilter{EnvironmentID: ids.New(ids.KindEnvironment)},
		keyvalue.PageRequest{},
	)
	if err != nil || len(page.Items) != 0 {
		t.Fatalf("sibling Environment leaked Runner: %#v, %v", page, err)
	}
}

// RUN-05: deletion racing between hierarchy read and atomic publication must
// leave neither a Runner nor a Task. A forged Environment/Project pair also fails.
func TestRunnerEnvironmentCreationFencesEveryParent(t *testing.T) {
	for _, scope := range []string{"tenant", "project", "environment", "wrong-project"} {
		t.Run(scope, func(t *testing.T) {
			ctx := context.Background()
			base, _, tenantID, projectID := newRunnerRepositoryFixture(t)
			environmentID := runnerTestEnvironment(t, base, tenantID, projectID)
			desired := runnerTestDesired(810, runners.RunnerOwnerEnvironment, environmentID, tenantID)
			desired.ParentProjectID = projectID
			ownerID := tenantID
			if scope == "project" {
				ownerID = projectID
			}
			if scope == "environment" {
				ownerID = environmentID
			}
			store := &runnerTransactionAuditStore{memoryHierarchyStore: base}
			if scope == "wrong-project" {
				otherID := ids.New(ids.KindProject)
				hierarchyRepository, err := newHierarchyRepository(base)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := hierarchyRepository.CreateProject(ctx, hierarchy.ProjectRecord{
					ID: otherID, TenantID: tenantID, Slug: "other", Name: "Other", Kind: hierarchy.ProjectKindTenant,
				}); err != nil {
					t.Fatal(err)
				}
				desired.ParentProjectID = otherID
			} else {
				store.allocationRaceKey = deletions.TombstoneKey(scope, ownerID)
				store.allocationRaceValue = []byte("deleting")
			}
			repository, err := newRunnerRepository(store)
			if err != nil {
				t.Fatal(err)
			}
			task := runnerTestTask(desired, taskjournal.TaskCreate, 811, "environment-runner-fenced")
			result, err := repository.CreateRunnerWithTask(
				ctx,
				runnerTestAllocationConfig(),
				desired,
				task,
				runnerTestMarker(task, desired),
			)
			if scope == "wrong-project" {
				if !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
					t.Fatalf("owner mismatch = %v", err)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				_, _, conflict, classifyErr := result.Classify()
				if classifyErr != nil || !errors.Is(conflict, errs.New(errs.KindResourceInUse, "")) {
					t.Fatalf("parent race = %v, %v", conflict, classifyErr)
				}
			}
			if _, err := repository.GetRunner(ctx, desired.ID); !errors.Is(err, errs.New(errs.KindRunnerNotFound, "")) {
				t.Fatalf("rejected creation left Runner: %v", err)
			}
			read, err := base.Get(ctx, taskjournal.TaskStorageKey(task.ID))
			if err != nil || read.Entry != nil {
				t.Fatalf("rejected creation left Task: %#v, %v", read, err)
			}
		})
	}
}
