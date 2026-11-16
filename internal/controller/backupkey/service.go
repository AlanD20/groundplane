package backupkey

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	backuppolicy "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	backuppolicymutations "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicymutations"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"net/http"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/acceptedaction"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const backupKeyRotationRoute = "/environments/{id}/rotate-key"

type Export struct {
	Identity []byte
	Era      int
}

type Repository interface {
	PrepareBackupKeyRotation(
		context.Context,
		etcd.BackupKeyRotationInput,
		backuppolicymutations.BackupPolicyInitialKeyMaterial,
	) (etcd.PreparedBackupKeyRotation, error)
	ReadBackupTaskAcceptance(context.Context, string) (etcd.BackupTaskAcceptance, error)
	PrepareBackupKeyRotationAuthority(
		context.Context,
		etcd.PreparedBackupKeyRotation,
		workexecution.Task,
		time.Time,
	) (etcd.BackupKeyRotationCapture, error)
	PrepareBackupKeyRotationSettlement(
		context.Context,
		workqueue.View,
		workexecution.Outcome,
		time.Time,
	) (workqueue.Change, error)
	PrepareBackupKeyRotationFinish(context.Context, workqueue.FinishedWork) (workqueue.Change, error)
	PrepareBackupKeyRotationRetention(context.Context, workqueue.View, time.Time, bool) (bool, workqueue.Change, error)
}

type KeyReader interface {
	GetBackupKey(context.Context, string) (backuppolicy.VersionedBackupKey, bool, error)
}

type backupKeyRotationIdempotency interface {
	Prepare(context.Context, string, []string) (requestidempotency.ProtectedEvidence, error)
	ResolveExisting(
		context.Context,
		idempotencyrecord.IdempotencyLocator,
		requestidempotency.ProtectedEvidence,
	) (requestidempotency.Resolution, bool, error)
}

type durableBackupKeyRotationIdempotency struct {
	coordinator *requestidempotency.Coordinator
	repository  *etcd.IdempotencyRepository
}

func newDurableBackupKeyRotationIdempotency(
	coordinator *requestidempotency.Coordinator,
	repository *etcd.IdempotencyRepository,
) (*durableBackupKeyRotationIdempotency, error) {
	if coordinator == nil || repository == nil {
		return nil, errs.New(errs.KindInternal, "backup key rotation idempotency dependencies are required")
	}
	return &durableBackupKeyRotationIdempotency{coordinator: coordinator, repository: repository}, nil
}

func (service *durableBackupKeyRotationIdempotency) Prepare(
	ctx context.Context,
	environmentID string, after []string,
) (requestidempotency.ProtectedEvidence, error) {
	version, digest, err := requestidempotency.Canonicalize(ctx, requestidempotency.CanonicalIntentV1{
		Method: http.MethodPost, Route: backupKeyRotationRoute,
		Scope: requestidempotency.Scope{Kind: requestidempotency.ScopeEnvironment, ID: environmentID},
		Path: []requestidempotency.PathBinding{
			{Name: "id", Value: environmentID},
		}, Query: requestidempotency.Object(), Body: requestidempotency.JSONBody(requestidempotency.Object(requestidempotency.Field{Name: "after", Value: acceptedaction.AfterValue(after)})),
	})
	if err != nil {
		return requestidempotency.ProtectedEvidence{}, err
	}
	defer digest.Destroy()
	return service.coordinator.ProtectIntent(ctx, version, digest)
}

func (service *durableBackupKeyRotationIdempotency) ResolveExisting(
	ctx context.Context,
	locator idempotencyrecord.IdempotencyLocator,
	evidence requestidempotency.ProtectedEvidence,
) (requestidempotency.Resolution, bool, error) {
	return service.coordinator.ResolveExisting(ctx, service.repository, locator, evidence)
}

type KeyFactory interface {
	Create(context.Context) (backuppolicymutations.BackupPolicyInitialKeyMaterial, error)
}

type Service struct {
	accepted    *acceptedaction.Submitter
	repository  Repository
	keyReader   KeyReader
	keys        KeyFactory
	idempotency backupKeyRotationIdempotency
	protector   *secretvalue.Protector
	now         func() time.Time
}

func NewService(
	repository Repository,
	keyReader KeyReader,
	keys KeyFactory,
	coordinator *requestidempotency.Coordinator,
	markers *etcd.IdempotencyRepository,
	protector *secretvalue.Protector,
	accepted *acceptedaction.Submitter,
) (*Service, error) {
	idempotency, err := newDurableBackupKeyRotationIdempotency(coordinator, markers)
	if err != nil {
		return nil, err
	}
	service, err := newService(repository, keyReader, keys, idempotency, protector)
	if err != nil {
		return nil, err
	}
	service.accepted = accepted
	return service, nil
}

func newService(
	repository Repository,
	keyReader KeyReader,
	keys KeyFactory,
	idempotency backupKeyRotationIdempotency,
	protector *secretvalue.Protector,
) (*Service, error) {
	if repository == nil || keyReader == nil || keys == nil || idempotency == nil || protector == nil {
		return nil, errs.New(errs.KindInternal, "backup key service dependencies are required")
	}
	return &Service{
		repository: repository, keyReader: keyReader, keys: keys, idempotency: idempotency,
		protector: protector, now: func() time.Time { return time.Now().UTC() },
	}, nil
}

func (service *Service) ExportBackupKey(ctx context.Context, environmentID string) (Export, error) {
	key, found, err := service.keyReader.GetBackupKey(ctx, environmentID)
	if err != nil {
		return Export{}, err
	}
	if !found {
		return Export{}, errs.New(errs.KindStateConflict, "backup encryption key is not configured")
	}
	defer clear(key.Encrypted.Ciphertext)
	digest := sha256.Sum256(key.Encrypted.Ciphertext)
	envelope, err := secretvalue.Restore(secretvalue.Metadata{
		Version: secretvalue.EnvelopeVersion1, Cipher: secretvalue.CipherSuiteAgeX25519,
		Digest: secretvalue.Digest{Algorithm: secretvalue.DigestAlgorithmSHA256, Value: hex.EncodeToString(digest[:])},
	}, key.Encrypted.Ciphertext)
	if err != nil {
		return Export{}, err
	}
	var identity []byte
	err = service.protector.OpenOwned(ctx, &envelope, func(plaintext []byte) error {
		identity = append(identity, plaintext...)
		return nil
	})
	if err != nil {
		clear(identity)
		return Export{}, err
	}
	if len(identity) == 0 {
		return Export{}, errs.New(errs.KindInternal, "backup key identity is empty")
	}
	identity = append(identity, '\n')
	return Export{Identity: identity, Era: key.Record.KeyEra}, nil
}

var _ backupKeyRotationIdempotency = (*durableBackupKeyRotationIdempotency)(nil)
