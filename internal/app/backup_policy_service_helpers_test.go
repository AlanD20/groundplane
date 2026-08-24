package app

import (
	"context"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/controller/idempotentintent"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

const (
	testEnvironmentID  = "env_01AAAAAAAAAAAAAAAAAAAAAAAA"
	testIdempotencyKey = "0123456789abcdef"
)

func backupPolicyTestRequest() apiTypes.BackupPolicyReplacementRequest {
	return apiTypes.BackupPolicyReplacementRequest{Enabled: false, Sources: []apiTypes.BackupSourceInput{}}
}

func backupPolicyTestService(
	t *testing.T,
	repository backupPolicyRepository,
	keys backupPolicyKeyFactory,
	idempotency backupPolicyIdempotency,
) *backupPolicyService {
	t.Helper()
	service, err := newBackupPolicyService(repository, keys, idempotency)
	if err != nil {
		t.Fatal(err)
	}
	return service
}

type backupPolicyTestRepository struct {
	needsKey     bool
	prepareCalls int
	supplyCalls  int
	replaceCalls int
}

func (*backupPolicyTestRepository) GetBackupPolicyProjection(
	context.Context,
	string,
) (etcd.BackupPolicyProjection, error) {
	return etcd.BackupPolicyProjection{Sources: []etcd.BackupPolicySourceProjection{}}, nil
}

func (repository *backupPolicyTestRepository) PrepareBackupPolicyReplacement(
	context.Context,
	etcd.BackupPolicyReplacementInput,
) (etcd.PreparedBackupPolicyReplacement, bool, error) {
	repository.prepareCalls++
	return etcd.PreparedBackupPolicyReplacement{}, repository.needsKey, nil
}

func (repository *backupPolicyTestRepository) SupplyBackupPolicyInitialKey(
	_ context.Context,
	prepared etcd.PreparedBackupPolicyReplacement,
	_ etcd.BackupPolicyInitialKeyMaterial,
) (etcd.PreparedBackupPolicyReplacement, error) {
	repository.supplyCalls++
	return prepared, nil
}

func (repository *backupPolicyTestRepository) ReplaceBackupPolicyProtected(
	context.Context,
	etcd.PreparedBackupPolicyReplacement,
	etcd.IdempotencyMarker,
) (etcd.IdempotencyTransactionResult, error) {
	repository.replaceCalls++
	return etcd.IdempotencyTransactionResult{}, nil
}

type backupPolicyTestKeys struct {
	calls int
	err   error
}

func (keys *backupPolicyTestKeys) Create(context.Context) (etcd.BackupPolicyInitialKeyMaterial, error) {
	keys.calls++
	if keys.err != nil {
		return etcd.BackupPolicyInitialKeyMaterial{}, keys.err
	}
	return etcd.BackupPolicyInitialKeyMaterial{
		Recipient: "age1test", Ciphertext: []byte("ciphertext"),
	}, nil
}

type backupPolicyTestIdempotency struct {
	prepareCalls int
	existing     bool
	response     etcd.IdempotencyResponse
}

func (service *backupPolicyTestIdempotency) Prepare(
	context.Context,
	string,
	apiTypes.BackupPolicyReplacementRequest,
) (backupPolicyEvidence, error) {
	service.prepareCalls++
	return backupPolicyEvidence{}, nil
}

func (service *backupPolicyTestIdempotency) ResolveExisting(
	context.Context,
	etcd.IdempotencyLocator,
	backupPolicyEvidence,
) (idempotentintent.Resolution, bool, error) {
	return idempotentintent.Resolution{Response: service.response}, service.existing, nil
}

func (*backupPolicyTestIdempotency) NewMarker(
	backupPolicyEvidence,
	etcd.IdempotencyLocator,
	etcd.IdempotencyResponse,
	time.Time,
) (etcd.IdempotencyMarker, error) {
	return etcd.IdempotencyMarker{}, nil
}

func (*backupPolicyTestIdempotency) ResolveKnown(
	context.Context,
	backupPolicyEvidence,
	etcd.IdempotencyTransactionResult,
) (idempotentintent.Resolution, error) {
	return idempotentintent.Resolution{Kind: idempotentintent.ResolutionApplied}, nil
}

func (*backupPolicyTestIdempotency) ResolveUnknown(
	context.Context,
	etcd.IdempotencyLocator,
	backupPolicyEvidence,
	error,
) (idempotentintent.Resolution, error) {
	return idempotentintent.Resolution{}, nil
}
