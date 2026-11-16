package backupkey

import (
	"bytes"
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	idempotentintent "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	testbackuppolicy "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	testbackuppolicymutations "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicymutations"
	testidempotency "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	testEnvironmentID = "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	testProjectID     = "prj_01ARZ3NDEKTSV4RRFFQ69G5FAW"
)

type recordingRotationRepository struct {
	input    etcd.BackupKeyRotationInput
	marker   testidempotency.IdempotencyMarker
	material testbackuppolicymutations.BackupPolicyInitialKeyMaterial
}

func (repository *recordingRotationRepository) PrepareBackupKeyRotation(
	_ context.Context,
	input etcd.BackupKeyRotationInput,
	material testbackuppolicymutations.BackupPolicyInitialKeyMaterial,
) (etcd.PreparedBackupKeyRotation, error) {
	repository.input = input
	repository.material = testbackuppolicymutations.BackupPolicyInitialKeyMaterial{
		Recipient:  material.Recipient,
		Ciphertext: append([]byte(nil), material.Ciphertext...),
	}
	return etcd.PreparedBackupKeyRotation{}, nil
}

func (repository *recordingRotationRepository) PrepareBackupKeyRotationAuthority(
	_ context.Context,
	_ etcd.PreparedBackupKeyRotation,
	task workexecution.Task,
	at time.Time,
) (etcd.BackupKeyRotationCapture, error) {
	ref := workexecution.Input{ID: repository.input.PlanID, SHA256: strings.Repeat("a", 64)}
	task.Intent, task.ExpectedItems, task.UpdatedAt = ref, 1, at
	item := workexecution.Item{ID: repository.input.WorkItemID, TaskID: task.ID, Input: &ref}
	return etcd.BackupKeyRotationCapture{Task: task, Item: item}, nil
}

func (*recordingRotationRepository) ReadBackupTaskAcceptance(
	context.Context,
	string,
) (etcd.BackupTaskAcceptance, error) {
	panic("late compiler must not read acceptance metadata")
}

func (*recordingRotationRepository) GetBackupKey(
	context.Context, string,

) (testbackuppolicy.VersionedBackupKey, bool, error) {
	return testbackuppolicy.VersionedBackupKey{}, false, nil
}

func (*recordingRotationRepository) PrepareBackupKeyRotationSettlement(
	context.Context,
	workqueue.View,
	workexecution.Outcome,
	time.Time,
) (workqueue.Change, error) {
	return workqueue.Change{}, errs.New(errs.KindInternal, "rotation acceptance unexpectedly settled work")
}

func (*recordingRotationRepository) PrepareBackupKeyRotationFinish(
	context.Context,
	workqueue.FinishedWork,
) (workqueue.Change, error) {
	return workqueue.Change{}, errs.New(errs.KindInternal, "rotation acceptance unexpectedly finished work")
}

func (*recordingRotationRepository) PrepareBackupKeyRotationRetention(
	context.Context,
	workqueue.View,
	time.Time,
	bool,
) (bool, workqueue.Change, error) {
	return false, workqueue.Change{}, errs.New(errs.KindInternal, "rotation acceptance unexpectedly collected work")
}

type fixedRotationKeyFactory struct {
	ciphertext []byte
}

func (factory *fixedRotationKeyFactory) Create(
	context.Context,
) (testbackuppolicymutations.BackupPolicyInitialKeyMaterial, error) {
	factory.ciphertext = []byte("wrapped-next-private-identity")
	return testbackuppolicymutations.BackupPolicyInitialKeyMaterial{
		Recipient:  "age1test",
		Ciphertext: factory.ciphertext,
	}, nil
}

type appliedRotationIdempotency struct{}

func (*appliedRotationIdempotency) Prepare(
	context.Context,
	string,
	[]string,
) (idempotentintent.ProtectedEvidence, error) {
	return idempotentintent.ProtectedEvidence{}, nil
}

func (*appliedRotationIdempotency) ResolveExisting(
	context.Context, testidempotency.IdempotencyLocator,

	idempotentintent.ProtectedEvidence,
) (idempotentintent.Resolution, bool, error) {
	return idempotentintent.Resolution{}, false, nil
}

type copyCrypt struct{}

func (*copyCrypt) Seal(_ context.Context, value []byte) ([]byte, error) {
	return append([]byte(nil), value...), nil
}

func (*copyCrypt) Open(_ context.Context, value []byte) ([]byte, error) {
	return append([]byte(nil), value...), nil
}

// Key material is generated only during deferred compilation, and cleared
// after the exact accepted Task's native rotation authority has been prepared.
func TestServiceRotationDefersKeyMaterialUntilCompilation(t *testing.T) {
	repository := &recordingRotationRepository{}
	keys := &fixedRotationKeyFactory{}
	crypt := &copyCrypt{}
	protector, err := secretvalue.NewProtector(crypt, crypt)
	if err != nil {
		t.Fatal(err)
	}
	service, err := newService(repository, repository, keys, &appliedRotationIdempotency{}, protector)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 8, 27, 12, 0, 0, 0, time.UTC)
	task := workexecution.Task{
		ID:        ids.New(ids.KindTask),
		Scope:     workexecution.Scope{EnvironmentID: testEnvironmentID},
		CreatedAt: at,
		UpdatedAt: at,
		Deadline:  at.Add(time.Hour),
	}
	if keys.ciphertext != nil {
		t.Fatal("key generated before compilation")
	}
	captured, err := service.prepareAcceptedRotation(
		t.Context(),
		workqueue.TaskSnapshot{Task: task, Revision: 1},
		1,
		at,
		apiTypes.TaskRunAfterRequest{},
	)
	if err != nil || captured.Task.ID != task.ID || repository.input.TaskID != task.ID || len(captured.Graph) != 1 {
		t.Fatalf("rotation capture: %#v / %v", captured, err)
	}
	if !bytes.Equal(keys.ciphertext, make([]byte, len(keys.ciphertext))) {
		t.Fatalf("key factory ciphertext retained %q", keys.ciphertext)
	}
	clear(repository.material.Ciphertext)
}
