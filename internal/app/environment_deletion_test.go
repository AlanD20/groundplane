package app

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller"
	"github.com/AlanD20/groundplane/internal/controller/idempotentintent"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
)

type fakeEnvironmentDeletionRepository struct {
	project                   etcd.Versioned[etcd.ProjectRecord]
	environment               etcd.Versioned[etcd.EnvironmentRecord]
	projection                etcd.Versioned[etcd.EnvironmentComposeProjection]
	expectedBlueprintRevision int64
	tombstone                 etcd.DeletionTombstoneRecord
	task                      etcd.TaskRecord
}

func (repository *fakeEnvironmentDeletionRepository) GetProject(
	context.Context,
	string,
) (etcd.Versioned[etcd.ProjectRecord], error) {
	return repository.project, nil
}

func (repository *fakeEnvironmentDeletionRepository) GetEnvironment(
	context.Context,
	string,
) (etcd.Versioned[etcd.EnvironmentRecord], error) {
	return repository.environment, nil
}

func (repository *fakeEnvironmentDeletionRepository) GetEnvironmentComposeProjection(
	context.Context,
	string,
) (etcd.Versioned[etcd.EnvironmentComposeProjection], bool, error) {
	return repository.projection, true, nil
}

func (repository *fakeEnvironmentDeletionRepository) BeginEnvironmentDeletionWithTask(
	_ context.Context,
	_ etcd.Versioned[etcd.ProjectRecord],
	_ etcd.Versioned[etcd.EnvironmentRecord],
	expectedBlueprintRevision int64,
	tombstone etcd.DeletionTombstoneRecord,
	task etcd.TaskRecord,
	_ etcd.IdempotencyMarker,
) (etcd.IdempotencyTransactionResult, error) {
	repository.expectedBlueprintRevision = expectedBlueprintRevision
	repository.tombstone = tombstone
	repository.task = task
	repository.task.Params = make(map[string]string, len(task.Params))
	for key, value := range task.Params {
		repository.task.Params[key] = value
	}
	repository.task.Steps = append([]etcd.TaskStepRecord(nil), task.Steps...)
	return etcd.IdempotencyTransactionResult{}, nil
}

type fakeEnvironmentDeletionPlans struct {
	task etcd.TaskRecord
}

func (plans *fakeEnvironmentDeletionPlans) ResolveExecutionPlan(
	_ context.Context,
	task etcd.TaskRecord,
) (*controller.ExecutionPlan, error) {
	plans.task = task
	return &controller.ExecutionPlan{PlanHash: []byte{0x01, 0x02, 0x03}}, nil
}

type fakeEnvironmentDeletionIdempotency struct{}

func (*fakeEnvironmentDeletionIdempotency) Prepare(
	context.Context,
	string,
) (environmentDeletionEvidence, error) {
	return environmentDeletionEvidence{}, nil
}

func (*fakeEnvironmentDeletionIdempotency) ResolveExisting(
	context.Context,
	etcd.IdempotencyLocator,
	environmentDeletionEvidence,
) (idempotentintent.Resolution, bool, error) {
	return idempotentintent.Resolution{}, false, nil
}

func (*fakeEnvironmentDeletionIdempotency) ResolveKnown(
	context.Context,
	environmentDeletionEvidence,
	etcd.IdempotencyTransactionResult,
) (idempotentintent.Resolution, error) {
	return idempotentintent.Resolution{Kind: idempotentintent.ResolutionApplied}, nil
}

func (*fakeEnvironmentDeletionIdempotency) ResolveUnknown(
	context.Context,
	etcd.IdempotencyLocator,
	environmentDeletionEvidence,
	error,
) (idempotentintent.Resolution, error) {
	return idempotentintent.Resolution{}, nil
}

// Rationale: deleting a materialized Environment must pin the current Blueprint revision and dispatch Compose removal before directory removal.
func TestDeleteEnvironmentBuildsBlueprintAwareRemovalTask(t *testing.T) {
	const (
		tenantID      = "tnt_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		projectID     = "prj_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		environmentID = "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		revisionID    = "task_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	)
	repository := &fakeEnvironmentDeletionRepository{
		project: etcd.Versioned[etcd.ProjectRecord]{
			Record: etcd.ProjectRecord{ID: projectID, TenantID: tenantID}, Revision: 9, ReadRevision: 41,
		},
		environment: etcd.Versioned[etcd.EnvironmentRecord]{
			Record: etcd.EnvironmentRecord{NetworkPool: "10.40.0.0/16",
				ID: environmentID, ProjectID: projectID, VolumeDir: "/var/lib/groundplane/vol/" + environmentID,
				ProvisioningState: etcd.EnvironmentProvisioningReady,
			},
			Revision: 11, ReadRevision: 41,
		},
		projection: etcd.Versioned[etcd.EnvironmentComposeProjection]{
			Record: etcd.EnvironmentComposeProjection{
				EnvironmentID: environmentID, BlueprintRevisionID: revisionID, RenderGeneration: 7,
			},
			Revision: 41, ReadRevision: 41,
		},
	}
	plans := &fakeEnvironmentDeletionPlans{}
	service, err := newEnvironmentDeletionService(
		repository,
		plans,
		&fakeEnvironmentDeletionIdempotency{},
	)
	if err != nil {
		t.Fatalf("newEnvironmentDeletionService() error = %v", err)
	}
	now := time.Date(2026, 8, 22, 17, 0, 0, 0, time.UTC)
	service.now = func() time.Time { return now }
	response, err := service.DeleteEnvironment(context.Background(), environmentID, "environment-delete-key-0001")
	if err != nil {
		t.Fatalf("DeleteEnvironment() error = %v", err)
	}
	task := repository.task
	if response.Status != http.StatusAccepted || repository.expectedBlueprintRevision != 41 ||
		task.Type != etcd.TaskRemove || task.Target != environmentID || task.RenderGeneration != 7 ||
		len(task.Steps) != 2 || len(task.Params) != 4 ||
		task.Params[etcd.EnvironmentBlueprintRevisionParam] != revisionID ||
		task.Params[etcd.TaskMaterializationEnvironmentParam] != environmentID ||
		task.Params[controller.EnvironmentRemoveVolumeDirectoryParam] != repository.environment.Record.VolumeDir ||
		ids.Validate(ids.KindConfig, task.Params[controller.EnvironmentBlueprintArtifactParam]) != nil {
		t.Fatalf("Blueprint-aware Environment deletion Task = %#v", task)
	}
	if plans.task.ID != task.ID || repository.tombstone.TaskID != task.ID ||
		repository.tombstone.TargetRevision != repository.environment.Revision ||
		repository.tombstone.Phase != etcd.DeletionPhaseHostEffects || repository.tombstone.CreatedAt != now {
		t.Fatalf("Environment deletion plan/tombstone = %#v / %#v", plans.task, repository.tombstone)
	}
}
