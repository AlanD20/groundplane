package backupkey

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	backuppolicy "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"net/http"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	corebackup "github.com/AlanD20/groundplane/internal/core/backup"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
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
		etcd.BackupPolicyInitialKeyMaterial,
	) (etcd.PreparedBackupKeyRotation, error)
	PublishBackupKeyRotation(
		context.Context,
		etcd.PreparedBackupKeyRotation,
		etcd.TaskRecord,
		idempotencyrecord.IdempotencyMarker,
	) (etcd.IdempotencyTransactionResult, error)
	ApplyBackupKeyRotation(context.Context, string) error
}

type KeyReader interface {
	GetBackupKey(context.Context, string) (backuppolicy.VersionedBackupKey, bool, error)
}

type backupKeyRotationIdempotency interface {
	Prepare(context.Context, string) (requestidempotency.ProtectedEvidence, error)
	ResolveExisting(
		context.Context,
		idempotencyrecord.IdempotencyLocator,
		requestidempotency.ProtectedEvidence,
	) (requestidempotency.Resolution, bool, error)
	NewMarker(
		requestidempotency.ProtectedEvidence,
		idempotencyrecord.IdempotencyLocator,
		idempotencyrecord.IdempotencyResponse,
		string,
		time.Time,
	) (idempotencyrecord.IdempotencyMarker, error)
	ResolveKnown(
		context.Context,
		requestidempotency.ProtectedEvidence,
		etcd.IdempotencyTransactionResult,
	) (requestidempotency.Resolution, error)
	ResolveUnknown(
		context.Context,
		idempotencyrecord.IdempotencyLocator,
		requestidempotency.ProtectedEvidence,
		error,
	) (requestidempotency.Resolution, error)
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
	environmentID string,
) (requestidempotency.ProtectedEvidence, error) {
	version, digest, err := requestidempotency.Canonicalize(ctx, requestidempotency.CanonicalIntentV1{
		Method: http.MethodPost, Route: backupKeyRotationRoute,
		Scope: requestidempotency.Scope{Kind: requestidempotency.ScopeEnvironment, ID: environmentID},
		Path: []requestidempotency.PathBinding{
			{Name: "id", Value: environmentID},
		}, Query: requestidempotency.Object(), Body: requestidempotency.NoBody(),
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

func (*durableBackupKeyRotationIdempotency) NewMarker(
	evidence requestidempotency.ProtectedEvidence,
	locator idempotencyrecord.IdempotencyLocator,
	response idempotencyrecord.IdempotencyResponse,
	taskID string,
	now time.Time,
) (idempotencyrecord.IdempotencyMarker, error) {
	intent, err := evidence.DurableRecord()
	if err != nil {
		return idempotencyrecord.IdempotencyMarker{}, err
	}
	defer clear(intent.Ciphertext)
	return newPendingRotationMarker(intent, locator, response, taskID, now), nil
}

func (service *durableBackupKeyRotationIdempotency) ResolveKnown(
	ctx context.Context,
	evidence requestidempotency.ProtectedEvidence,
	result etcd.IdempotencyTransactionResult,
) (requestidempotency.Resolution, error) {
	return service.coordinator.ResolveKnown(ctx, evidence, result)
}

func (service *durableBackupKeyRotationIdempotency) ResolveUnknown(
	ctx context.Context,
	locator idempotencyrecord.IdempotencyLocator,
	evidence requestidempotency.ProtectedEvidence,
	original error,
) (requestidempotency.Resolution, error) {
	return service.coordinator.ResolveUnknown(ctx, service.repository, locator, evidence, original)
}

type KeyFactory interface {
	Create(context.Context) (etcd.BackupPolicyInitialKeyMaterial, error)
}

type Service struct {
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
) (*Service, error) {
	idempotency, err := newDurableBackupKeyRotationIdempotency(coordinator, markers)
	if err != nil {
		return nil, err
	}
	return newService(repository, keyReader, keys, idempotency, protector)
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

func (service *Service) RotateBackupKey(
	ctx context.Context,
	environmentID, idempotencyKey string,
) (idempotencyrecord.IdempotencyResponse, error) {
	evidence, err := service.idempotency.Prepare(ctx, environmentID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	locator := idempotencyrecord.IdempotencyLocator{
		ScopeKind: idempotencyrecord.IdempotencyScopeEnvironment,
		ScopeID:   environmentID,
		Method:    http.MethodPost,
		Route:     backupKeyRotationRoute,
		Key:       idempotencyKey,
	}
	resolution, existing, err := service.idempotency.ResolveExisting(ctx, locator, evidence)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if existing {
		defer clear(resolution.Response.Body)
		return cloneIdempotencyResponse(resolution.Response), nil
	}
	now := service.now().UTC()
	material, err := service.keys.Create(ctx)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	defer clear(material.Ciphertext)
	taskID, operationID, planID := ids.New(ids.KindTask), ids.New(ids.KindOperation), ids.New(ids.KindPlan)
	prepared, err := service.repository.PrepareBackupKeyRotation(ctx, etcd.BackupKeyRotationInput{
		EnvironmentID: environmentID, TaskID: taskID, OperationID: operationID, PlanID: planID, CreatedAt: now,
	}, material)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	defer prepared.Clear()
	task := etcd.TaskRecord{
		ID: taskID, OperationID: operationID, IdempotencyKey: idempotencyKey,
		Owner: prepared.Owner, Actor: taskjournal.TaskActorOperator,
		Executor: taskjournal.TaskExecutorController, PlanID: planID, PlanHash: corebackup.KeyRotationPlanHash(), RenderGeneration: 1,
		Type: taskjournal.TaskRotate, Target: environmentID, TimeoutSeconds: corebackup.KeyRotationTimeoutSeconds, Status: taskjournal.TaskStatusPending,
		NextEventSequence: 1, CreatedAt: now, UpdatedAt: now,
	}
	body, err := json.Marshal(apiTypes.TaskAccepted{TaskID: taskID})
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.Wrap(errs.KindInternal, err)
	}
	response := idempotencyrecord.IdempotencyResponse{
		Status:      http.StatusAccepted,
		ContentKind: "application/json",
		Body:        append([]byte(nil), body...),
	}
	defer clear(response.Body)
	marker, err := service.idempotency.NewMarker(evidence, locator, response, taskID, now)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	defer clear(marker.Intent.Ciphertext)
	defer clear(marker.Response.Body)
	transaction, publishErr := service.repository.PublishBackupKeyRotation(ctx, prepared, task, marker)
	if publishErr != nil {
		if !isUnknownBackupKeyPublicationOutcome(publishErr) {
			return idempotencyrecord.IdempotencyResponse{}, publishErr
		}
		resolution, err = service.idempotency.ResolveUnknown(ctx, locator, evidence, publishErr)
	} else {
		resolution, err = service.idempotency.ResolveKnown(ctx, evidence, transaction)
	}
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	defer clear(resolution.Response.Body)
	if resolution.Kind == requestidempotency.ResolutionApplied {
		return cloneIdempotencyResponse(response), nil
	}
	if resolution.Kind == requestidempotency.ResolutionReplay {
		return cloneIdempotencyResponse(resolution.Response), nil
	}
	return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindInternal, "backup key rotation resolution is invalid")
}

func (service *Service) Execute(ctx context.Context, task etcd.TaskRecord) error {
	if task.Type != taskjournal.TaskRotate || task.Executor != taskjournal.TaskExecutorController {
		return errs.New(errs.KindValidationFailed, "backup key rotation Task is invalid")
	}
	return service.repository.ApplyBackupKeyRotation(ctx, task.ID)
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

func newPendingRotationMarker(
	intent idempotencyrecord.ProtectedIntentRecord,
	locator idempotencyrecord.IdempotencyLocator,
	response idempotencyrecord.IdempotencyResponse,
	taskID string,
	now time.Time,
) idempotencyrecord.IdempotencyMarker {
	marker := idempotencyrecord.IdempotencyMarker{
		Kind: idempotencyrecord.IdempotencyMarkerTask, State: idempotencyrecord.IdempotencyMarkerPending,
		Locator: locator, Intent: intent, Response: cloneIdempotencyResponse(response),
		TaskID: taskID, CreatedAt: now, UpdatedAt: now,
	}
	marker.Intent.Ciphertext = append([]byte(nil), intent.Ciphertext...)
	return marker
}

func isUnknownBackupKeyPublicationOutcome(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	kind, ok := errs.KindOf(err)
	return ok && kind == errs.KindStorageUnavailable
}

var _ backupKeyRotationIdempotency = (*durableBackupKeyRotationIdempotency)(nil)

func cloneIdempotencyResponse(response idempotencyrecord.IdempotencyResponse) idempotencyrecord.IdempotencyResponse {
	response.Body = append([]byte(nil), response.Body...)
	return response
}
