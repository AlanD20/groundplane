package app

import (
	"context"
	"encoding/json"
	"net/http"
	"reflect"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/idempotentintent"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

// Rationale: first dispatch must derive ownership from the durable Secret and
// emit the exact closed Controller Task, replay target, and tombstone contract.
func TestSecretDeletionDispatchesTheClosedControllerFinalizer(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 22, 23, 0, 0, 0, time.UTC)
	projectID := ids.NewAt(ids.KindProject, now, 1)
	tenantID := ids.NewAt(ids.KindTenant, now, 3)
	secretID := ids.NewAt(ids.KindSecret, now, 2)
	record, err := etcd.NewProjectSecretRecord(
		secretID, projectID, "TOKEN", core.SecretKindEnvVar, "", now,
	)
	if err != nil {
		t.Fatalf("NewProjectSecretRecord() error = %v", err)
	}
	repository := &fakeSecretDeletionRepository{
		project: etcd.Versioned[etcd.ProjectRecord]{
			Record: etcd.ProjectRecord{
				ID: projectID, TenantID: tenantID, Kind: etcd.ProjectKindTenant,
			},
			Revision: 5, ReadRevision: 5,
		},
		secret: etcd.Versioned[etcd.SecretRecord]{Record: record, Revision: 7, ReadRevision: 7},
	}
	idempotency := &fakeSecretDeletionIdempotency{
		known: idempotentintent.Resolution{Kind: idempotentintent.ResolutionApplied},
	}
	service, err := newSecretDeletionService(repository, idempotency)
	if err != nil {
		t.Fatalf("newSecretDeletionService() error = %v", err)
	}
	service.now = func() time.Time { return now }
	response, err := service.DeleteSecret(context.Background(), secretID, "secret-delete-key-0001")
	if err != nil {
		t.Fatalf("DeleteSecret() error = %v", err)
	}
	var accepted apiTypes.TaskAccepted
	if err := json.Unmarshal(response.Body, &accepted); err != nil {
		t.Fatalf("decode TaskAccepted: %v", err)
	}
	if response.Status != http.StatusAccepted || accepted.TaskID != repository.task.ID ||
		repository.task.Executor != etcd.TaskExecutorController || repository.task.Type != etcd.TaskRemove ||
		repository.task.Target != secretID || repository.task.TimeoutSeconds != secretDeletionTimeoutSeconds ||
		len(repository.task.Params) != 1 ||
		repository.task.Params[etcd.TaskResourceKindParam] != etcd.TaskResourceSecret ||
		repository.marker.ReplayTarget == nil ||
		*repository.marker.ReplayTarget != (etcd.IdempotencyReplayTarget{
			Kind: etcd.IdempotencyReplayTargetSecret, ID: secretID,
		}) || repository.marker.Locator.ScopeKind != etcd.IdempotencyScopeProject ||
		repository.marker.Locator.ScopeID != projectID ||
		repository.tombstone.TargetKind != etcd.DeletionTargetSecret ||
		repository.tombstone.TargetRevision != repository.secret.Revision ||
		repository.tombstone.Phase != etcd.DeletionPhaseFinalizing {
		t.Fatalf(
			"deletion response/task/marker/tombstone = %#v/%#v/%#v/%#v",
			response,
			repository.task,
			repository.marker,
			repository.tombstone,
		)
	}
}

// Rationale: exact retries must resolve through the stable Secret replay index
// before reading a target that successful finalization has already removed.
func TestSecretDeletionReplaysBeforeTargetLookup(t *testing.T) {
	t.Parallel()
	secretID := "sec_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	want := etcd.IdempotencyResponse{
		Status: http.StatusAccepted, ContentKind: "application/json",
		Body: []byte(`{"task_id":"task_01ARZ3NDEKTSV4RRFFQ69G5FAV"}`),
	}
	repository := &fakeSecretDeletionRepository{}
	idempotency := &fakeSecretDeletionIdempotency{
		indexed: true,
		locator: etcd.IdempotencyLocator{
			ScopeKind: etcd.IdempotencyScopePlatform, ScopeID: "-",
			Method: http.MethodDelete, Route: secretDeletionRoute, Key: "secret-delete-key-0002",
		},
		existing: true,
		existingResolution: idempotentintent.Resolution{
			Kind: idempotentintent.ResolutionReplay, Response: want,
		},
	}
	service, err := newSecretDeletionService(repository, idempotency)
	if err != nil {
		t.Fatalf("newSecretDeletionService() error = %v", err)
	}
	got, err := service.DeleteSecret(context.Background(), secretID, "secret-delete-key-0002")
	if err != nil || !reflect.DeepEqual(got, want) || repository.readCalls != 0 || repository.beginCalls != 0 {
		t.Fatalf(
			"DeleteSecret(replay) = %#v/%v, repository calls = %d/%d",
			got,
			err,
			repository.readCalls,
			repository.beginCalls,
		)
	}
}

type fakeSecretDeletionRepository struct {
	project    etcd.Versioned[etcd.ProjectRecord]
	secret     etcd.Versioned[etcd.SecretRecord]
	tombstone  etcd.DeletionTombstoneRecord
	task       etcd.TaskRecord
	marker     etcd.IdempotencyMarker
	readCalls  int
	beginCalls int
}

func (repository *fakeSecretDeletionRepository) GetProject(
	_ context.Context,
	_ string,
) (etcd.Versioned[etcd.ProjectRecord], error) {
	repository.readCalls++
	return repository.project, nil
}

func (repository *fakeSecretDeletionRepository) GetSecret(
	_ context.Context,
	_ string,
) (etcd.Versioned[etcd.SecretRecord], error) {
	repository.readCalls++
	return repository.secret, nil
}

func (repository *fakeSecretDeletionRepository) BeginSecretDeletionWithTask(
	_ context.Context,
	_ etcd.SecretOwner,
	_ etcd.Versioned[etcd.SecretRecord],
	tombstone etcd.DeletionTombstoneRecord,
	task etcd.TaskRecord,
	marker etcd.IdempotencyMarker,
) (etcd.IdempotencyTransactionResult, error) {
	repository.beginCalls++
	repository.tombstone = tombstone
	repository.task = task
	repository.marker = marker
	return etcd.IdempotencyTransactionResult{}, nil
}

type fakeSecretDeletionIdempotency struct {
	locator            etcd.IdempotencyLocator
	indexed            bool
	existing           bool
	existingResolution idempotentintent.Resolution
	known              idempotentintent.Resolution
}

func (idempotency *fakeSecretDeletionIdempotency) ResolveReplayLocator(
	_ context.Context,
	_ etcd.IdempotencyReplayTarget,
	_ string,
	_ string,
	_ string,
) (etcd.IdempotencyLocator, bool, error) {
	return idempotency.locator, idempotency.indexed, nil
}

func (idempotency *fakeSecretDeletionIdempotency) Prepare(
	_ context.Context,
	_ etcd.IdempotencyLocator,
	_ string,
) (secretDeletionEvidence, error) {
	return secretDeletionEvidence{}, nil
}

func (idempotency *fakeSecretDeletionIdempotency) ResolveExisting(
	_ context.Context,
	_ etcd.IdempotencyLocator,
	_ secretDeletionEvidence,
) (idempotentintent.Resolution, bool, error) {
	return idempotency.existingResolution, idempotency.existing, nil
}

func (idempotency *fakeSecretDeletionIdempotency) ResolveKnown(
	_ context.Context,
	_ secretDeletionEvidence,
	_ etcd.IdempotencyTransactionResult,
) (idempotentintent.Resolution, error) {
	return idempotency.known, nil
}

func (idempotency *fakeSecretDeletionIdempotency) ResolveUnknown(
	_ context.Context,
	_ etcd.IdempotencyLocator,
	_ secretDeletionEvidence,
	_ error,
) (idempotentintent.Resolution, error) {
	return idempotency.known, nil
}
