package backup

import (
	"context"
	"testing"
	"time"

	idempotentintent "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	testbackuppolicy "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	testbackuppolicymutations "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicymutations"
	testbackupqueries "github.com/AlanD20/groundplane/internal/infra/etcd/backupqueries"
	testidempotency "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
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
) *PolicyService {
	t.Helper()
	service, err := NewBackupPolicyService(repository, keys, idempotency)
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
	context.Context, string,

) (testbackupqueries.BackupPolicyProjection, error) {
	return testbackupqueries.BackupPolicyProjection{Sources: []testbackupqueries.BackupPolicySourceProjection{}}, nil
}

func (repository *backupPolicyTestRepository) PrepareBackupPolicyReplacement(
	context.Context, testbackuppolicy.BackupPolicyReplacementInput,

) (testbackuppolicymutations.PreparedBackupPolicyReplacement, bool, error) {
	repository.prepareCalls++
	return testbackuppolicymutations.PreparedBackupPolicyReplacement{}, repository.needsKey, nil
}

func (repository *backupPolicyTestRepository) SupplyBackupPolicyInitialKey(
	_ context.Context,
	prepared testbackuppolicymutations.PreparedBackupPolicyReplacement,
	_ testbackuppolicymutations.BackupPolicyInitialKeyMaterial,
) (testbackuppolicymutations.PreparedBackupPolicyReplacement, error) {
	repository.supplyCalls++
	return prepared, nil
}

func (*backupPolicyTestRepository) FinalizeBackupPolicySchedule(
	prepared testbackuppolicymutations.PreparedBackupPolicyReplacement,
	_ time.Time,
) (testbackuppolicymutations.PreparedBackupPolicyReplacement, testbackupqueries.BackupPolicyProjection, error) {
	return prepared, testbackupqueries.BackupPolicyProjection{
		Sources: []testbackupqueries.BackupPolicySourceProjection{},
	}, nil
}

func (repository *backupPolicyTestRepository) ReplaceBackupPolicyProtected(
	context.Context, testbackuppolicymutations.PreparedBackupPolicyReplacement, testidempotency.IdempotencyMarker,

) (etcd.IdempotencyTransactionResult, error) {
	repository.replaceCalls++
	return etcd.IdempotencyTransactionResult{}, nil
}

type backupPolicyTestKeys struct {
	calls int
	err   error
}

func (keys *backupPolicyTestKeys) Create(
	context.Context,
) (testbackuppolicymutations.BackupPolicyInitialKeyMaterial, error) {
	keys.calls++
	if keys.err != nil {
		return testbackuppolicymutations.BackupPolicyInitialKeyMaterial{}, keys.err
	}
	return testbackuppolicymutations.BackupPolicyInitialKeyMaterial{
		Recipient: "age1test", Ciphertext: []byte("ciphertext"),
	}, nil
}

type backupPolicyTestIdempotency struct {
	prepareCalls int
	existing     bool
	response     testidempotency.IdempotencyResponse
}

func (service *backupPolicyTestIdempotency) Prepare(
	context.Context, string,

	apiTypes.BackupPolicyReplacementRequest,
) (backupPolicyEvidence, error) {
	service.prepareCalls++
	return backupPolicyEvidence{}, nil
}

func (service *backupPolicyTestIdempotency) ResolveExisting(
	context.Context, testidempotency.IdempotencyLocator,

	backupPolicyEvidence,
) (idempotentintent.Resolution, bool, error) {
	return idempotentintent.Resolution{Response: service.response}, service.existing, nil
}

func (*backupPolicyTestIdempotency) NewMarker(
	backupPolicyEvidence, testidempotency.IdempotencyLocator, testidempotency.IdempotencyResponse,

	time.Time,
) (testidempotency.IdempotencyMarker, error) {
	return testidempotency.IdempotencyMarker{}, nil
}

func (*backupPolicyTestIdempotency) ResolveKnown(
	context.Context,
	backupPolicyEvidence,
	etcd.IdempotencyTransactionResult,
) (idempotentintent.Resolution, error) {
	return idempotentintent.Resolution{Kind: idempotentintent.ResolutionApplied}, nil
}

func (*backupPolicyTestIdempotency) ResolveUnknown(
	context.Context, testidempotency.IdempotencyLocator,

	backupPolicyEvidence,
	error,
) (idempotentintent.Resolution, error) {
	return idempotentintent.Resolution{}, nil
}
