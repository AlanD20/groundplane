package app

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller"
	"github.com/AlanD20/groundplane/internal/controller/idempotentintent"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type fakeZoneDeletionRepository struct {
	zone        etcd.Versioned[etcd.ZoneRecord]
	environment etcd.Versioned[etcd.EnvironmentRecord]
	project     etcd.Versioned[etcd.ProjectRecord]
	task        etcd.TaskRecord
	marker      etcd.IdempotencyMarker
}

func (fake *fakeZoneDeletionRepository) GetZone(context.Context, string) (etcd.Versioned[etcd.ZoneRecord], error) {
	return fake.zone, nil
}

func (fake *fakeZoneDeletionRepository) GetEnvironment(
	context.Context,
	string,
) (etcd.Versioned[etcd.EnvironmentRecord], error) {
	return fake.environment, nil
}

func (fake *fakeZoneDeletionRepository) GetProject(
	context.Context,
	string,
) (etcd.Versioned[etcd.ProjectRecord], error) {
	return fake.project, nil
}

func (fake *fakeZoneDeletionRepository) BeginZoneDeletionWithTask(
	_ context.Context,
	_ etcd.Versioned[etcd.EnvironmentRecord],
	_ etcd.Versioned[etcd.ProjectRecord],
	_ etcd.Versioned[etcd.ZoneRecord],
	_ etcd.DeletionTombstoneRecord,
	task etcd.TaskRecord,
	marker etcd.IdempotencyMarker,
) (etcd.IdempotencyTransactionResult, error) {
	fake.task = task
	fake.marker = marker
	return etcd.IdempotencyTransactionResult{}, nil
}

type fakeZoneDeletionIdempotency struct{}

func (*fakeZoneDeletionIdempotency) ResolveReplayLocator(
	context.Context,
	etcd.IdempotencyReplayTarget,
	string,
	string,
	string,
) (etcd.IdempotencyLocator, bool, error) {
	return etcd.IdempotencyLocator{}, false, nil
}

func (*fakeZoneDeletionIdempotency) Prepare(
	context.Context,
	etcd.IdempotencyLocator,
	string,
) (zoneDeletionEvidence, error) {
	return zoneDeletionEvidence{}, nil
}

func (*fakeZoneDeletionIdempotency) ResolveExisting(
	context.Context,
	etcd.IdempotencyLocator,
	zoneDeletionEvidence,
) (idempotentintent.Resolution, bool, error) {
	return idempotentintent.Resolution{}, false, nil
}

func (*fakeZoneDeletionIdempotency) ResolveKnown(
	context.Context,
	zoneDeletionEvidence,
	etcd.IdempotencyTransactionResult,
) (idempotentintent.Resolution, error) {
	return idempotentintent.Resolution{Kind: idempotentintent.ResolutionApplied}, nil
}

func (*fakeZoneDeletionIdempotency) ResolveUnknown(
	context.Context,
	etcd.IdempotencyLocator,
	zoneDeletionEvidence,
	error,
) (idempotentintent.Resolution, error) {
	return idempotentintent.Resolution{}, nil
}

type fakeZoneDeletionPlans struct{}

func (*fakeZoneDeletionPlans) ResolveExecutionPlan(
	_ context.Context,
	task etcd.TaskRecord,
) (*controller.ExecutionPlan, error) {
	return &agentpb.ExecutionPlan{PlanHash: make([]byte, 32)}, nil
}

// Rationale: ordinary deletion must publish one replayable Agent Task scoped
// to the owning Environment without erasing the Zone before acknowledgement.
func TestZoneDeletionPublishesExactTaskAndReplayTarget(t *testing.T) {
	now := time.Date(2026, time.August, 23, 12, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, now, 1)
	projectID := ids.NewAt(ids.KindProject, now, 2)
	zoneID := ids.NewAt(ids.KindNetwork, now, 3)
	repository := &fakeZoneDeletionRepository{
		zone: etcd.Versioned[etcd.ZoneRecord]{Record: etcd.ZoneRecord{
			EnvironmentID: environmentID,
			Desired: core.Zone{ID: zoneID, Name: "frontend", Subnet: "10.40.10.0/24",
				OwnerKind: core.ZoneOwnerEnvironment, OwnerID: environmentID},
		}, Revision: 7, ReadRevision: 7},
		environment: etcd.Versioned[etcd.EnvironmentRecord]{Record: etcd.EnvironmentRecord{
			ID: environmentID, ProjectID: projectID, Name: "prod", NetworkPool: "10.40.0.0/16",
			VolumeDir: "/var/lib/groundplane/vol/tenants/ten_01ARZ3NDEKTSV4RRFFQ69G5FAV/projects/" +
				projectID + "/environments/" + environmentID,
			ProvisioningState: etcd.EnvironmentProvisioningReady,
		}, Revision: 8, ReadRevision: 8},
		project: etcd.Versioned[etcd.ProjectRecord]{Record: etcd.ProjectRecord{
			ID: projectID, TenantID: ids.NewAt(ids.KindTenant, now, 4), Kind: etcd.ProjectKindTenant,
			Slug: "web", Name: "Web",
		}, Revision: 9, ReadRevision: 9},
	}
	service, err := newZoneDeletionService(repository, &fakeZoneDeletionPlans{}, &fakeZoneDeletionIdempotency{})
	if err != nil {
		t.Fatalf("newZoneDeletionService() error = %v", err)
	}
	service.now = func() time.Time { return now }
	response, err := service.RemoveZone(context.Background(), zoneID, "zone-remove-key-0001")
	if err != nil || response.Status != http.StatusAccepted {
		t.Fatalf("RemoveZone() = %#v, %v", response, err)
	}
	var accepted apiTypes.TaskAccepted
	if json.Unmarshal(response.Body, &accepted) != nil || accepted.TaskID != repository.task.ID ||
		repository.task.Target != zoneID ||
		repository.task.Params[etcd.TaskZoneEnvironmentParam] != environmentID ||
		repository.marker.ReplayTarget == nil ||
		repository.marker.ReplayTarget.Kind != etcd.IdempotencyReplayTargetZone ||
		repository.marker.ReplayTarget.ID != zoneID {
		t.Fatalf("Zone deletion task/marker = %#v / %#v", repository.task, repository.marker)
	}
}
