package backupkey

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/controller/idempotentintent"
	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

const (
	testEnvironmentID = "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	testProjectID     = "prj_01ARZ3NDEKTSV4RRFFQ69G5FAW"
)

type recordingRotationRepository struct {
	input    etcd.BackupKeyRotationInput
	task     etcd.TaskRecord
	marker   etcd.IdempotencyMarker
	material etcd.BackupPolicyInitialKeyMaterial
}

func (repository *recordingRotationRepository) PrepareBackupKeyRotation(
	_ context.Context,
	input etcd.BackupKeyRotationInput,
	material etcd.BackupPolicyInitialKeyMaterial,
) (etcd.PreparedBackupKeyRotation, error) {
	repository.input = input
	repository.material = etcd.BackupPolicyInitialKeyMaterial{
		Recipient:  material.Recipient,
		Ciphertext: append([]byte(nil), material.Ciphertext...),
	}
	return etcd.PreparedBackupKeyRotation{Owner: etcd.TaskOwner{
		WorkspaceType: etcd.TaskWorkspacePlatform,
		ProjectID:     testProjectID,
		EnvironmentID: input.EnvironmentID,
	}}, nil
}

func (repository *recordingRotationRepository) PublishBackupKeyRotation(
	_ context.Context,
	_ etcd.PreparedBackupKeyRotation,
	task etcd.TaskRecord,
	marker etcd.IdempotencyMarker,
) (etcd.IdempotencyTransactionResult, error) {
	repository.task = task
	repository.marker = marker
	repository.marker.Intent.Ciphertext = append([]byte(nil), marker.Intent.Ciphertext...)
	repository.marker.Response.Body = append([]byte(nil), marker.Response.Body...)
	return etcd.IdempotencyTransactionResult{}, nil
}

func (*recordingRotationRepository) GetBackupKey(
	context.Context,
	string,
) (etcd.VersionedBackupKey, bool, error) {
	return etcd.VersionedBackupKey{}, false, nil
}

func (*recordingRotationRepository) ApplyBackupKeyRotation(context.Context, string) error {
	return nil
}

type fixedRotationKeyFactory struct {
	ciphertext []byte
}

func (factory *fixedRotationKeyFactory) Create(context.Context) (etcd.BackupPolicyInitialKeyMaterial, error) {
	factory.ciphertext = []byte("wrapped-next-private-identity")
	return etcd.BackupPolicyInitialKeyMaterial{
		Recipient:  "age1test",
		Ciphertext: factory.ciphertext,
	}, nil
}

type appliedRotationIdempotency struct{}

func (*appliedRotationIdempotency) Prepare(context.Context, string) (idempotentintent.ProtectedEvidence, error) {
	return idempotentintent.ProtectedEvidence{}, nil
}

func (*appliedRotationIdempotency) ResolveExisting(
	context.Context,
	etcd.IdempotencyLocator,
	idempotentintent.ProtectedEvidence,
) (idempotentintent.Resolution, bool, error) {
	return idempotentintent.Resolution{}, false, nil
}

func (*appliedRotationIdempotency) NewMarker(
	_ idempotentintent.ProtectedEvidence,
	locator etcd.IdempotencyLocator,
	response etcd.IdempotencyResponse,
	taskID string,
	now time.Time,
) (etcd.IdempotencyMarker, error) {
	return newPendingRotationMarker(etcd.ProtectedIntentRecord{}, locator, response, taskID, now), nil
}

func (*appliedRotationIdempotency) ResolveKnown(
	context.Context,
	idempotentintent.ProtectedEvidence,
	etcd.IdempotencyTransactionResult,
) (idempotentintent.Resolution, error) {
	return idempotentintent.Resolution{Kind: idempotentintent.ResolutionApplied}, nil
}

func (*appliedRotationIdempotency) ResolveUnknown(
	context.Context,
	etcd.IdempotencyLocator,
	idempotentintent.ProtectedEvidence,
	error,
) (idempotentintent.Resolution, error) {
	return idempotentintent.Resolution{}, nil
}

type copyCrypt struct{}

func (*copyCrypt) Seal(_ context.Context, value []byte) ([]byte, error) {
	return append([]byte(nil), value...), nil
}

func (*copyCrypt) Open(_ context.Context, value []byte) ([]byte, error) {
	return append([]byte(nil), value...), nil
}

// Rationale: the production key use case must publish the pending marker tied
// to its exact Task and return immutable TaskAccepted bytes after all transient
// response and key-material buffers have been cleared.
func TestServiceRotationPublishesPendingTaskMarkerAndReturnsOwnedBody(t *testing.T) {
	repository := &recordingRotationRepository{}
	keys := &fixedRotationKeyFactory{}
	crypt := &copyCrypt{}
	protector, err := secretvalue.NewProtector(crypt, crypt)
	if err != nil {
		t.Fatal(err)
	}
	service, err := newService(repository, keys, &appliedRotationIdempotency{}, protector)
	if err != nil {
		t.Fatal(err)
	}
	service.now = func() time.Time {
		return time.Date(2026, time.August, 27, 12, 0, 0, 0, time.UTC)
	}

	response, err := service.RotateBackupKey(
		context.Background(),
		testEnvironmentID,
		"rotate-key-test-0001",
	)
	if err != nil {
		t.Fatalf("RotateBackupKey() error = %v", err)
	}
	if response.Status != 202 || response.ContentKind != "application/json" ||
		bytes.Contains(response.Body, []byte{0}) {
		t.Fatalf("response = %#v, body %q", response, response.Body)
	}
	var accepted apiTypes.TaskAccepted
	if err := json.Unmarshal(response.Body, &accepted); err != nil {
		t.Fatalf("TaskAccepted body = %q: %v", response.Body, err)
	}
	if accepted.TaskID == "" || accepted.TaskID != repository.task.ID {
		t.Fatalf("TaskAccepted task id = %q, published %q", accepted.TaskID, repository.task.ID)
	}
	if repository.marker.Kind != etcd.IdempotencyMarkerTask ||
		repository.marker.State != etcd.IdempotencyMarkerPending ||
		repository.marker.TaskID != repository.task.ID ||
		!bytes.Equal(repository.marker.Response.Body, response.Body) {
		t.Fatalf("published marker = %#v", repository.marker)
	}
	if repository.input.TaskID != repository.task.ID ||
		repository.input.OperationID != repository.task.OperationID ||
		repository.input.PlanID != repository.task.PlanID {
		t.Fatalf("preparation/task identity = %#v / %#v", repository.input, repository.task)
	}
	if !bytes.Equal(keys.ciphertext, make([]byte, len(keys.ciphertext))) {
		t.Fatalf("key factory ciphertext retained %q", keys.ciphertext)
	}
	clear(response.Body)
	clear(repository.material.Ciphertext)
	clear(repository.marker.Intent.Ciphertext)
	clear(repository.marker.Response.Body)
}
