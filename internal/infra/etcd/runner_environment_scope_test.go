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
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// RUN-01/05: Environment publication and listing retain the complete owner
// chain. Native Retry is exercised separately with the same captured input.
func TestRunnerEnvironmentOwnershipAndScopedListing(t *testing.T) {
	ctx := context.Background()
	store, repository, tenantID, projectID := newRunnerRepositoryFixture(t)
	environmentID := runnerTestEnvironment(t, store, tenantID, projectID)
	desired := runnerTestDesired(800, runners.RunnerOwnerEnvironment, environmentID, tenantID)
	desired.ParentProjectID = projectID
	fixture, err := prepareRunnerCreationFixture(
		t,
		repository,
		runnerTestAllocationConfig(),
		desired,
		801,
		"environment-runner-create",
	)
	if err != nil {
		t.Fatal(err)
	}
	fixture.publish(t, repository)
	current, err := repository.GetRunner(ctx, desired.ID)
	if err != nil {
		t.Fatal(err)
	}
	assertRunnerNativePublication(t, store, current, fixture)
	for _, filter := range []runners.RunnerFilter{{EnvironmentID: environmentID}, {TenantID: tenantID}} {
		page, err := repository.ListRunners(ctx, filter, keyvalue.PageRequest{})
		if err != nil || len(page.Items) != 1 || page.Items[0].Record.Desired.ID != desired.ID {
			t.Fatalf("scoped listing = %#v, %v", page, err)
		}
	}
	page, err := repository.ListRunners(ctx,
		runners.RunnerFilter{EnvironmentID: ids.New(ids.KindEnvironment)}, keyvalue.PageRequest{})
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
			fixture, err := prepareRunnerCreationFixture(
				t,
				repository,
				runnerTestAllocationConfig(),
				desired,
				811,
				"environment-runner-fenced",
			)
			if scope == "wrong-project" {
				if !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
					t.Fatalf("owner mismatch = %v", err)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				result := fixture.publish(t, repository)
				_, _, conflict, classifyErr := result.Classify()
				if classifyErr != nil || !errors.Is(conflict, errs.New(errs.KindStateConflict, "")) {
					t.Fatalf("parent race = %v, %v", conflict, classifyErr)
				}
			}
			if _, err := repository.GetRunner(ctx, desired.ID); !errors.Is(err, errs.New(errs.KindRunnerNotFound, "")) {
				t.Fatalf("rejected creation left Runner: %v", err)
			}
			read, err := base.Get(
				ctx,
				workqueue.TaskKey(ids.NewAt(ids.KindTask, runnerStorageTime(), runnerTaskEntropyBase+811)),
			)
			if err != nil || read.Entry != nil {
				t.Fatalf("rejected creation left Task: %#v, %v", read, err)
			}
		})
	}
}
