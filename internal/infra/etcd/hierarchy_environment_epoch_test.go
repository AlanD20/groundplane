package etcd

import (
	"context"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: an orphaned canonical mutation epoch is corrupt creation
// evidence, so Environment creation must classify the failed CAS instead of
// reporting a successful hierarchy publication.
func TestHierarchyEnvironmentCreationRejectsExistingMutationEpoch(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	now := time.Date(2026, 8, 24, 19, 0, 0, 0, time.UTC)
	store := newMemoryHierarchyStore()
	repository, err := newHierarchyRepository(store)
	if err != nil {
		t.Fatalf("newHierarchyRepository() error = %v", err)
	}
	tenantID := ids.NewAt(ids.KindTenant, now, 740)
	if _, err := repository.CreateTenant(ctx, TenantRecord{
		ID: tenantID, Slug: "epoch-tenant", Name: "Epoch Tenant",
	}); err != nil {
		t.Fatalf("CreateTenant() error = %v", err)
	}
	project, err := repository.CreateProject(ctx, ProjectRecord{
		ID: ids.NewAt(ids.KindProject, now, 741), TenantID: tenantID,
		Slug: "epoch-project", Name: "Epoch Project", Kind: ProjectKindTenant,
	})
	if err != nil {
		t.Fatalf("CreateProject() error = %v", err)
	}
	environmentID := ids.NewAt(ids.KindEnvironment, now, 742)
	epochValue, err := encodeEnvironmentMutationEpochRecord(EnvironmentMutationEpochRecord{
		EnvironmentID: environmentID,
	})
	if err != nil {
		t.Fatalf("encodeEnvironmentMutationEpochRecord() error = %v", err)
	}
	defer clear(epochValue)
	seeded, err := store.Transact(
		ctx,
		[]Condition{{Key: environmentMutationEpochKey(environmentID)}},
		[]Mutation{{Type: MutationPut, Key: environmentMutationEpochKey(environmentID), Value: epochValue}},
	)
	if err != nil || !seeded.Succeeded {
		t.Fatalf("seed mutation epoch = %#v, %v", seeded, err)
	}
	record := EnvironmentRecord{
		ID: environmentID, ProjectID: project.Record.ID,
		Name: "production", NetworkPool: "10.249.0.0/24",
		VolumeDir:         "/var/lib/groundplane/vol/" + tenantID + "/" + project.Record.ID + "/" + environmentID,
		ProvisioningState: EnvironmentProvisioningReady,
		CreateTaskID:      ids.NewAt(ids.KindTask, now, 743), CreatedAt: now,
	}
	if _, err := repository.CreateEnvironment(ctx, record); !isKind(err, errs.KindInternal) {
		t.Fatalf("CreateEnvironment(existing epoch) error = %v", err)
	}
	if _, err := repository.GetEnvironment(ctx, environmentID); !isKind(err, errs.KindEnvironmentNotFound) {
		t.Fatalf("GetEnvironment(after rejected create) error = %v", err)
	}
	epoch, err := store.Get(ctx, environmentMutationEpochKey(environmentID))
	if err != nil || epoch.Entry == nil || epoch.Entry.ModRevision != seeded.Revision {
		t.Fatalf("stored mutation epoch = %#v, %v", epoch, err)
	}
}
