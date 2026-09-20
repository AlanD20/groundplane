package etcd

import (
	"context"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale (OWN-07): an aggregate deletion must not adopt a descendant deletion's
// cleanup authority. The descendant tombstone survives failure for explicit
// retry, so both Project and Tenant publication must leave the aggregate
// untouched while that authority exists.
func TestHierarchyDeletionRejectsDescendantDeletionAuthority(t *testing.T) {
	for _, target := range []struct {
		name       string
		kind       HierarchyDeletionTargetKind
		op         HierarchyDeletionOperationKind
		project    bool
		concurrent bool
		available  bool
	}{
		{name: "project", kind: HierarchyDeletionTargetProject, op: HierarchyDeletionOperationProject},
		{name: "tenant", kind: HierarchyDeletionTargetTenant, op: HierarchyDeletionOperationTenant},
		{name: "tenant_with_deleting_project", kind: HierarchyDeletionTargetTenant,
			op: HierarchyDeletionOperationTenant, project: true},
		{name: "project_race", kind: HierarchyDeletionTargetProject,
			op: HierarchyDeletionOperationProject, concurrent: true},
		{name: "tenant_race", kind: HierarchyDeletionTargetTenant,
			op: HierarchyDeletionOperationTenant, concurrent: true},
		{name: "available_project", kind: HierarchyDeletionTargetProject,
			op: HierarchyDeletionOperationProject, available: true},
		{name: "available_tenant", kind: HierarchyDeletionTargetTenant,
			op: HierarchyDeletionOperationTenant, available: true},
	} {
		t.Run(target.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			store := newMemoryHierarchyStore()
			hierarchy, err := newHierarchyRepository(store)
			if err != nil {
				t.Fatalf("newHierarchyRepository() error = %v", err)
			}
			now := time.Date(2026, 9, 20, 19, 0, 0, 0, time.UTC)
			tenant := TenantRecord{ID: ids.NewAt(ids.KindTenant, now, 1), Slug: "tenant", Name: "Tenant"}
			if _, err = hierarchy.CreateTenant(ctx, tenant); err != nil {
				t.Fatalf("CreateTenant() error = %v", err)
			}
			project := ProjectRecord{
				ID: ids.NewAt(ids.KindProject, now, 2), TenantID: tenant.ID,
				Slug: "project", Name: "Project", Kind: ProjectKindTenant,
			}
			if _, err = hierarchy.CreateProject(ctx, project); err != nil {
				t.Fatalf("CreateProject() error = %v", err)
			}
			environment := EnvironmentRecord{
				ID: ids.NewAt(ids.KindEnvironment, now, 3), ProjectID: project.ID,
				Name: "production", NetworkPool: "10.120.254.0/24",
				VolumeDir: "/var/lib/groundplane/vol/" + tenant.ID + "/" + project.ID + "/" +
					ids.NewAt(ids.KindEnvironment, now, 3),
				ProvisioningState: EnvironmentProvisioningReady,
				CreateTaskID:      ids.NewAt(ids.KindTask, now, 4), CreatedAt: now,
			}
			if _, err = hierarchy.CreateEnvironment(ctx, environment); err != nil {
				t.Fatalf("CreateEnvironment() error = %v", err)
			}
			journal, err := newHierarchyDeletionRepository(store)
			if err != nil {
				t.Fatalf("newHierarchyDeletionRepository() error = %v", err)
			}
			child := hierarchyDeletionCreationTestBegin(now, HierarchyDeletionTargetEnvironment,
				environment.ID, HierarchyDeletionOperationEnvironment, "d")
			if target.project {
				child = hierarchyDeletionCreationTestBegin(now, HierarchyDeletionTargetProject,
					project.ID, HierarchyDeletionOperationProject, "d")
			}
			startChild := func() error {
				_, childErr := journal.Begin(ctx, child)
				return childErr
			}
			raceStore := &descendantDeletionRaceStore{memoryHierarchyStore: store}
			if target.concurrent {
				raceStore.before = startChild
			} else if !target.available {
				if err = startChild(); err != nil {
					t.Fatalf("Begin(child deletion) error = %v", err)
				}
			}
			parentJournal, err := newHierarchyDeletionRepository(raceStore)
			if err != nil {
				t.Fatalf("newHierarchyDeletionRepository(parent) error = %v", err)
			}

			targetID := project.ID
			if target.kind == HierarchyDeletionTargetTenant {
				targetID = tenant.ID
			}
			begin := hierarchyDeletionCreationTestBegin(now.Add(time.Second), target.kind, targetID, target.op, "a")
			_, err = parentJournal.Begin(ctx, begin)
			if target.available {
				if err != nil {
					t.Fatalf("Begin(available parent) error = %v", err)
				}
				return
			}
			if target.concurrent {
				if raceStore.before != nil || !isKind(err, errs.KindStateConflict) {
					t.Fatalf("concurrent child deletion did not fence parent: %v", err)
				}
			} else if !isKind(err, errs.KindResourceInUse) {
				t.Fatalf("Begin(%s deletion) error = %v, want resource.in_use", target.name, err)
			}
			for _, key := range []string{
				HierarchyDeletionTombstoneKey(string(target.kind), targetID), taskKey(begin.TaskID),
			} {
				if result, getErr := store.Get(ctx, key); getErr != nil || result.Entry != nil {
					t.Fatalf("blocked %s deletion wrote %s = %#v/%v", target.name, key, result, getErr)
				}
			}
			if _, err = journal.OperationByTask(ctx, child.TaskID); err != nil {
				t.Fatalf("child deletion authority lost: %v", err)
			}
		})
	}
}

type descendantDeletionRaceStore struct {
	*memoryHierarchyStore
	before func() error
}

func (store *descendantDeletionRaceStore) Transact(
	ctx context.Context, conditions []Condition, mutations []Mutation,
) (TransactionResult, error) {
	if before := store.before; before != nil {
		store.before = nil
		if err := before(); err != nil {
			return TransactionResult{}, err
		}
	}
	return store.memoryHierarchyStore.Transact(ctx, conditions, mutations)
}
