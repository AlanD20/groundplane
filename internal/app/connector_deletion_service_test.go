package app

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/idempotentintent"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

// Rationale: first dispatch must derive immutable Environment ownership from
// durable Connector ancestry and publish the exact protected finalizer tuple.
func TestConnectorDeletionDispatchesClosedControllerFinalizer(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 24, 10, 0, 0, 0, time.UTC)
	tenantID := ids.NewAt(ids.KindTenant, now, 1)
	projectID := ids.NewAt(ids.KindProject, now, 2)
	environmentID := ids.NewAt(ids.KindEnvironment, now, 3)
	connectorID := ids.NewAt(ids.KindConnector, now, 4)
	repository := &fakeConnectorDeletionRepository{
		project: etcd.Versioned[etcd.ProjectRecord]{
			Record:   etcd.ProjectRecord{ID: projectID, TenantID: tenantID, Kind: etcd.ProjectKindTenant},
			Revision: 5, ReadRevision: 5,
		},
		environment: etcd.Versioned[etcd.EnvironmentRecord]{
			Record:   etcd.EnvironmentRecord{ID: environmentID, ProjectID: projectID},
			Revision: 6, ReadRevision: 6,
		},
		connector: etcd.Versioned[etcd.ConnectorRecord]{
			Record: etcd.ConnectorRecord{Connector: core.Connector{
				ID: connectorID, EnvironmentID: environmentID, Name: "primary-backups",
			}},
			Revision: 7, ReadRevision: 7,
		},
	}
	idempotency := &fakeConnectorDeletionIdempotency{
		known: idempotentintent.Resolution{Kind: idempotentintent.ResolutionApplied},
	}
	service, err := newConnectorDeletionService(repository, idempotency)
	if err != nil {
		t.Fatalf("newConnectorDeletionService() error = %v", err)
	}
	service.now = func() time.Time { return now }
	response, err := service.DeleteConnector(context.Background(), connectorID, "connector-delete-key-0001")
	if err != nil {
		t.Fatalf("DeleteConnector() error = %v", err)
	}
	var accepted apiTypes.TaskAccepted
	if err := json.Unmarshal(response.Body, &accepted); err != nil {
		t.Fatalf("decode TaskAccepted: %v", err)
	}
	if response.Status != http.StatusAccepted || accepted.TaskID != repository.task.ID ||
		repository.task.Owner.EnvironmentID != environmentID || repository.task.Actor != etcd.TaskActorOperator ||
		repository.task.Executor != etcd.TaskExecutorController || repository.task.Type != etcd.TaskRemove ||
		repository.task.Target != connectorID || repository.task.TimeoutSeconds != connectorDeletionTimeoutSeconds ||
		len(repository.task.Params) != 3 ||
		repository.task.Params[etcd.TaskResourceKindParam] != etcd.TaskResourceConnector ||
		repository.task.Params[etcd.TaskConnectorEnvironmentParam] != environmentID ||
		repository.task.Params[etcd.TaskConnectorNameParam] != "primary-backups" ||
		repository.marker.ReplayTarget == nil ||
		*repository.marker.ReplayTarget != (etcd.IdempotencyReplayTarget{
			Kind: etcd.IdempotencyReplayTargetConnector, ID: connectorID,
		}) || repository.marker.Locator.ScopeKind != etcd.IdempotencyScopeEnvironment ||
		repository.marker.Locator.ScopeID != environmentID ||
		repository.intent.ConnectorRevision != repository.connector.Revision ||
		repository.tombstone.TargetKind != etcd.DeletionTargetConnector ||
		repository.tombstone.Phase != etcd.DeletionPhaseFinalizing {
		t.Fatalf(
			"deletion response/task/marker/intent/tombstone = %#v/%#v/%#v/%#v/%#v",
			response, repository.task, repository.marker, repository.intent, repository.tombstone,
		)
	}
}

// Rationale: an exact retry after successful finalization must resolve through
// the stable target index before the deleted Connector is read.
func TestConnectorDeletionReplaysBeforeTargetLookup(t *testing.T) {
	t.Parallel()
	connectorID := "con_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	want := etcd.IdempotencyResponse{
		Status: http.StatusAccepted, ContentKind: "application/json",
		Body: []byte(`{"task_id":"task_01ARZ3NDEKTSV4RRFFQ69G5FAV"}`),
	}
	repository := &fakeConnectorDeletionRepository{}
	idempotency := &fakeConnectorDeletionIdempotency{
		indexed: true,
		locator: etcd.IdempotencyLocator{
			ScopeKind: etcd.IdempotencyScopeEnvironment,
			ScopeID:   "env_01ARZ3NDEKTSV4RRFFQ69G5FAV",
			Method:    http.MethodDelete,
			Route:     connectorDeletionRoute,
			Key:       "connector-delete-key-0002",
		},
		existing: true,
		existingResolution: idempotentintent.Resolution{
			Kind: idempotentintent.ResolutionReplay, Response: want,
		},
	}
	service, err := newConnectorDeletionService(repository, idempotency)
	if err != nil {
		t.Fatalf("newConnectorDeletionService() error = %v", err)
	}
	got, err := service.DeleteConnector(context.Background(), connectorID, "connector-delete-key-0002")
	if err != nil || got.Status != want.Status || got.ContentKind != want.ContentKind ||
		string(got.Body) != string(want.Body) || repository.readCalls != 0 || repository.beginCalls != 0 {
		t.Fatalf(
			"DeleteConnector(replay) = %#v/%v, repository calls = %d/%d",
			got, err, repository.readCalls, repository.beginCalls,
		)
	}
}

type fakeConnectorDeletionRepository struct {
	project     etcd.Versioned[etcd.ProjectRecord]
	environment etcd.Versioned[etcd.EnvironmentRecord]
	connector   etcd.Versioned[etcd.ConnectorRecord]
	tombstone   etcd.DeletionTombstoneRecord
	intent      etcd.ConnectorRemovalIntent
	task        etcd.TaskRecord
	marker      etcd.IdempotencyMarker
	readCalls   int
	beginCalls  int
}

func (repository *fakeConnectorDeletionRepository) GetConnector(
	_ context.Context,
	_ string,
) (etcd.Versioned[etcd.ConnectorRecord], error) {
	repository.readCalls++
	return repository.connector, nil
}

func (repository *fakeConnectorDeletionRepository) GetEnvironment(
	_ context.Context,
	_ string,
) (etcd.Versioned[etcd.EnvironmentRecord], error) {
	repository.readCalls++
	return repository.environment, nil
}

func (repository *fakeConnectorDeletionRepository) GetProject(
	_ context.Context,
	_ string,
) (etcd.Versioned[etcd.ProjectRecord], error) {
	repository.readCalls++
	return repository.project, nil
}

func (repository *fakeConnectorDeletionRepository) BeginConnectorDeletionWithTask(
	_ context.Context,
	_ etcd.Versioned[etcd.EnvironmentRecord],
	_ etcd.Versioned[etcd.ProjectRecord],
	_ etcd.Versioned[etcd.ConnectorRecord],
	tombstone etcd.DeletionTombstoneRecord,
	intent etcd.ConnectorRemovalIntent,
	task etcd.TaskRecord,
	marker etcd.IdempotencyMarker,
) (etcd.IdempotencyTransactionResult, error) {
	repository.beginCalls++
	repository.tombstone = tombstone
	repository.intent = intent
	repository.task = task
	repository.marker = marker
	return etcd.IdempotencyTransactionResult{}, nil
}

type fakeConnectorDeletionIdempotency struct {
	locator            etcd.IdempotencyLocator
	indexed            bool
	existing           bool
	existingResolution idempotentintent.Resolution
	known              idempotentintent.Resolution
}

func (idempotency *fakeConnectorDeletionIdempotency) ResolveReplayLocator(
	_ context.Context,
	_ etcd.IdempotencyReplayTarget,
	_ string,
	_ string,
	_ string,
) (etcd.IdempotencyLocator, bool, error) {
	return idempotency.locator, idempotency.indexed, nil
}

func (idempotency *fakeConnectorDeletionIdempotency) Prepare(
	_ context.Context,
	_ etcd.IdempotencyLocator,
	_ string,
) (connectorDeletionEvidence, error) {
	return connectorDeletionEvidence{}, nil
}

func (idempotency *fakeConnectorDeletionIdempotency) ResolveExisting(
	_ context.Context,
	_ etcd.IdempotencyLocator,
	_ connectorDeletionEvidence,
) (idempotentintent.Resolution, bool, error) {
	return idempotency.existingResolution, idempotency.existing, nil
}

func (idempotency *fakeConnectorDeletionIdempotency) ResolveKnown(
	_ context.Context,
	_ connectorDeletionEvidence,
	_ etcd.IdempotencyTransactionResult,
) (idempotentintent.Resolution, error) {
	return idempotency.known, nil
}

func (idempotency *fakeConnectorDeletionIdempotency) ResolveUnknown(
	_ context.Context,
	_ etcd.IdempotencyLocator,
	_ connectorDeletionEvidence,
	_ error,
) (idempotentintent.Resolution, error) {
	return idempotency.known, nil
}
