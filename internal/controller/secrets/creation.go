package secrets

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"time"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/internal/common/ids"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	secretCreationRoute           = "/secrets"
	maximumSecretCreationAttempts = 3
)

type secretCreationRepository interface {
	GetProject(context.Context, string) (etcd.Versioned[etcd.ProjectRecord], error)
	CreateSecretIdempotent(
		context.Context,
		etcd.SecretOwner,
		etcd.SecretRecord,
		etcd.SecretEncryptedValue,
		etcd.IdempotencyMarker,
	) (etcd.IdempotencyTransactionResult, error)
}

type secretCreationEvidence struct {
	candidate requestidempotency.ProtectedEvidence
	durable   etcd.ProtectedIntentRecord
}

type secretCreationIdempotency interface {
	Prepare(context.Context, apiTypes.SecretCreateRequest) (secretCreationEvidence, error)
	ResolveExisting(
		context.Context,
		etcd.IdempotencyLocator,
		secretCreationEvidence,
	) (requestidempotency.Resolution, bool, error)
	ResolveKnown(
		context.Context,
		secretCreationEvidence,
		etcd.IdempotencyTransactionResult,
	) (requestidempotency.Resolution, error)
	ResolveUnknown(
		context.Context,
		etcd.IdempotencyLocator,
		secretCreationEvidence,
		error,
	) (requestidempotency.Resolution, error)
}

type durableSecretCreationIdempotency struct {
	coordinator *requestidempotency.Coordinator
	repository  *etcd.IdempotencyRepository
}

func NewCreationIdempotency(
	coordinator *requestidempotency.Coordinator,
	repository *etcd.IdempotencyRepository,
) (*durableSecretCreationIdempotency, error) {
	if coordinator == nil || repository == nil {
		return nil, errs.New(errs.KindInternal, "Secret creation idempotency is not configured")
	}
	return &durableSecretCreationIdempotency{coordinator: coordinator, repository: repository}, nil
}

func (service *durableSecretCreationIdempotency) Prepare(
	ctx context.Context,
	input apiTypes.SecretCreateRequest,
) (secretCreationEvidence, error) {
	valueBytes := []byte(input.Value)
	defer clear(valueBytes)
	digest := sha256.Sum256(valueBytes)
	scope := requestidempotency.Scope{Kind: requestidempotency.ScopePlatform}
	if input.ProjectID != "" {
		scope = requestidempotency.Scope{Kind: requestidempotency.ScopeProject, ID: input.ProjectID}
	}
	version, canonicalDigest, err := requestidempotency.Canonicalize(ctx, requestidempotency.CanonicalIntentV1{
		Method: http.MethodPost,
		Route:  secretCreationRoute,
		Scope:  scope,
		Query:  requestidempotency.Object(),
		Body: requestidempotency.JSONBody(requestidempotency.Object(
			requestidempotency.Field{Name: "key", Value: requestidempotency.String(input.Key)},
			requestidempotency.Field{Name: "kind", Value: requestidempotency.String(input.Kind)},
			requestidempotency.Field{Name: "path", Value: requestidempotency.String(input.Path)},
			requestidempotency.Field{Name: "platform", Value: requestidempotency.Bool(input.Platform)},
			requestidempotency.Field{Name: "project_id", Value: requestidempotency.String(input.ProjectID)},
			requestidempotency.Field{Name: "value_sha256", Value: requestidempotency.String(hex.EncodeToString(digest[:]))},
		)),
	})
	if err != nil {
		return secretCreationEvidence{}, err
	}
	defer canonicalDigest.Destroy()
	candidate, err := service.coordinator.ProtectIntent(ctx, version, canonicalDigest)
	if err != nil {
		return secretCreationEvidence{}, err
	}
	durable, err := candidate.DurableRecord()
	if err != nil {
		return secretCreationEvidence{}, err
	}
	return secretCreationEvidence{candidate: candidate, durable: durable}, nil
}

func (service *durableSecretCreationIdempotency) ResolveExisting(
	ctx context.Context,
	locator etcd.IdempotencyLocator,
	evidence secretCreationEvidence,
) (requestidempotency.Resolution, bool, error) {
	return service.coordinator.ResolveExisting(ctx, service.repository, locator, evidence.candidate)
}

func (service *durableSecretCreationIdempotency) ResolveKnown(
	ctx context.Context,
	evidence secretCreationEvidence,
	result etcd.IdempotencyTransactionResult,
) (requestidempotency.Resolution, error) {
	return service.coordinator.ResolveKnown(ctx, evidence.candidate, result)
}

func (service *durableSecretCreationIdempotency) ResolveUnknown(
	ctx context.Context,
	locator etcd.IdempotencyLocator,
	evidence secretCreationEvidence,
	original error,
) (requestidempotency.Resolution, error) {
	return service.coordinator.ResolveUnknown(ctx, service.repository, locator, evidence.candidate, original)
}

type secretCreationService struct {
	repository  secretCreationRepository
	protector   *secretvalue.Protector
	idempotency secretCreationIdempotency
	now         func() time.Time
}

func NewCreationService(
	repository secretCreationRepository,
	protector *secretvalue.Protector,
	idempotency secretCreationIdempotency,
) (*secretCreationService, error) {
	if repository == nil || protector == nil || idempotency == nil {
		return nil, errs.New(errs.KindInternal, "Secret creation service is not configured")
	}
	return &secretCreationService{
		repository: repository, protector: protector, idempotency: idempotency, now: time.Now,
	}, nil
}

func (service *secretCreationService) CreateSecret(
	ctx context.Context,
	input apiTypes.SecretCreateRequest,
	idempotencyKey string,
) (etcd.IdempotencyResponse, error) {
	if ctx == nil {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Secret creation context is required")
	}
	for attempt := 0; attempt < maximumSecretCreationAttempts; attempt++ {
		response, err := service.createSecretOnce(ctx, input, idempotencyKey)
		if err == nil {
			return response, nil
		}
		kind, ok := errs.KindOf(err)
		if !ok || kind != errs.KindStateConflict || attempt == maximumSecretCreationAttempts-1 {
			return etcd.IdempotencyResponse{}, err
		}
	}
	return etcd.IdempotencyResponse{}, errs.New(
		errs.KindInternal,
		"Secret creation retry bound was not enforced",
	)
}

func (service *secretCreationService) createSecretOnce(
	ctx context.Context,
	input apiTypes.SecretCreateRequest,
	idempotencyKey string,
) (etcd.IdempotencyResponse, error) {
	record, err := prepareSecretCreation(input, service.now().UTC())
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	evidence, err := service.idempotency.Prepare(ctx, input)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	defer clear(evidence.durable.Ciphertext)
	locator := secretCreationLocator(input, idempotencyKey)
	resolution, existing, err := service.idempotency.ResolveExisting(ctx, locator, evidence)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if existing {
		if resolution.Kind != requestidempotency.ResolutionReplay {
			return etcd.IdempotencyResponse{}, errs.New(
				errs.KindInternal,
				"Secret creation replay resolution is invalid",
			)
		}
		return requestidempotency.CloneResponse(resolution.Response), nil
	}

	owner := etcd.PlatformSecretOwner()
	if input.ProjectID != "" {
		project, projectErr := service.repository.GetProject(ctx, input.ProjectID)
		if projectErr != nil {
			return etcd.IdempotencyResponse{}, projectErr
		}
		owner = etcd.ProjectSecretOwner(project)
	}
	plaintext := []byte(input.Value)
	defer clear(plaintext)
	envelope, err := service.protector.Seal(ctx, plaintext)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	metadata := envelope.Metadata()
	ciphertext := envelope.Ciphertext()
	defer clear(ciphertext)
	value := etcd.SecretEncryptedValue{
		SecretID: record.Secret.ID, EnvelopeVersion: uint8(metadata.Version),
		Cipher: string(metadata.Cipher), DigestAlgorithm: string(metadata.Digest.Algorithm),
		CiphertextSHA256: metadata.Digest.Value, Ciphertext: ciphertext,
	}
	responseBody, err := json.Marshal(secretCreationResponse(record))
	if err != nil {
		return etcd.IdempotencyResponse{}, errs.Wrap(errs.KindInternal, err)
	}
	defer clear(responseBody)
	response := etcd.IdempotencyResponse{
		Status: http.StatusCreated, ContentKind: "application/json",
		Body: append([]byte(nil), responseBody...),
	}
	marker, err := etcd.NewCompletedDirectIdempotencyMarker(
		locator,
		evidence.durable,
		response,
		service.now().UTC(),
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	defer clear(marker.Intent.Ciphertext)
	defer clear(marker.Response.Body)
	result, createErr := service.repository.CreateSecretIdempotent(ctx, owner, record, value, marker)
	if createErr != nil {
		if !isUnknownSecretCreationOutcome(createErr) {
			return etcd.IdempotencyResponse{}, createErr
		}
		resolution, err = service.idempotency.ResolveUnknown(ctx, locator, evidence, createErr)
	} else {
		resolution, err = service.idempotency.ResolveKnown(ctx, evidence, result)
	}
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	switch resolution.Kind {
	case requestidempotency.ResolutionApplied:
		return requestidempotency.CloneResponse(response), nil
	case requestidempotency.ResolutionReplay:
		return requestidempotency.CloneResponse(resolution.Response), nil
	default:
		return etcd.IdempotencyResponse{}, errs.New(
			errs.KindInternal,
			"Secret creation resolution is invalid",
		)
	}
}

func prepareSecretCreation(
	input apiTypes.SecretCreateRequest,
	updatedAt time.Time,
) (etcd.SecretRecord, error) {
	hasProject := input.ProjectID != ""
	if hasProject == input.Platform {
		return etcd.SecretRecord{}, errs.New(
			errs.KindValidationFailed,
			"Secret creation requires exactly one owner selector",
		)
	}
	if !utf8.ValidString(input.Value) {
		return etcd.SecretRecord{}, errs.New(
			errs.KindValidationFailed,
			"Secret value must be valid UTF-8",
		)
	}
	if len(input.Value) > apiTypes.MaximumSecretValueBytes {
		return etcd.SecretRecord{}, errs.New(
			errs.KindValidationFailed,
			"Secret value exceeds the 255 KiB limit",
		)
	}
	kind := core.SecretKind(input.Kind)
	if kind == core.SecretKindEnvVar && input.Path != "" {
		return etcd.SecretRecord{}, errs.New(
			errs.KindValidationFailed,
			"Environment-variable Secret must not set path",
		)
	}
	id := ids.New(ids.KindSecret)
	if input.Platform {
		return etcd.NewPlatformSecretRecord(id, input.Key, kind, input.Path, updatedAt)
	}
	return etcd.NewProjectSecretRecord(id, input.ProjectID, input.Key, kind, input.Path, updatedAt)
}

func secretCreationLocator(input apiTypes.SecretCreateRequest, key string) etcd.IdempotencyLocator {
	scopeKind := etcd.IdempotencyScopePlatform
	scopeID := "-"
	if input.ProjectID != "" {
		scopeKind = etcd.IdempotencyScopeProject
		scopeID = input.ProjectID
	}
	return etcd.IdempotencyLocator{
		ScopeKind: scopeKind, ScopeID: scopeID,
		Method: http.MethodPost, Route: secretCreationRoute, Key: key,
	}
}

func secretCreationResponse(record etcd.SecretRecord) apiTypes.Secret {
	secret := record.Secret
	return apiTypes.Secret{
		ID: secret.ID, Scope: string(secret.Scope), ProjectID: secret.ProjectID,
		Key: secret.Key, Kind: string(secret.Kind), Ref: secret.Ref,
		UpdatedAt: secret.UpdatedAt.UTC().Format(time.RFC3339Nano),
	}
}

func isUnknownSecretCreationOutcome(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	kind, ok := errs.KindOf(err)
	return ok && kind == errs.KindStorageUnavailable
}
