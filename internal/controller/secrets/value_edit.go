package secrets

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"time"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/internal/common/ids"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	secretrecord "github.com/AlanD20/groundplane/internal/infra/etcd/secrets"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type ValueEditor struct {
	repository  *Repository
	protector   *secretvalue.Protector
	evidence    *etcd.IdempotencyRepository
	coordinator *requestidempotency.Coordinator
}

func NewValueEditor(repository *Repository, protector *secretvalue.Protector, evidence *etcd.IdempotencyRepository, coordinator *requestidempotency.Coordinator) (*ValueEditor, error) {
	if repository == nil || protector == nil || evidence == nil || coordinator == nil {
		return nil, errs.New(errs.KindInternal, "Secret value editor dependencies are required")
	}
	return &ValueEditor{repository: repository, protector: protector, evidence: evidence, coordinator: coordinator}, nil
}
func (service *ValueEditor) EditSecret(ctx context.Context, id string, input apiTypes.SecretEditRequest, key string) (idempotency.IdempotencyResponse, error) {
	if ctx == nil || ids.Validate(ids.KindSecret, id) != nil || input.Revision <= 0 || !utf8.ValidString(input.Value) || len(input.Value) > apiTypes.MaximumSecretValueBytes {
		return idempotency.IdempotencyResponse{}, errs.New(errs.KindValidationFailed, "Secret edit requires its current revision and a UTF-8 value of at most 255 KiB")
	}
	target := idempotency.IdempotencyReplayTarget{Kind: idempotency.IdempotencyReplayTargetSecret, ID: id}
	locator, indexed, err := service.evidence.ResolveReplayLocator(ctx, target, http.MethodPatch, "/secrets/{id}", key)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	if !indexed {
		current, err := service.repository.GetSecret(ctx, id)
		if err != nil {
			return idempotency.IdempotencyResponse{}, err
		}
		locator = idempotency.IdempotencyLocator{ScopeKind: idempotency.IdempotencyScopePlatform, ScopeID: "-", Method: http.MethodPatch, Route: "/secrets/{id}", Key: key}
		if current.Record.Secret.ProjectID != "" {
			locator.ScopeKind = idempotency.IdempotencyScopeProject
			locator.ScopeID = current.Record.Secret.ProjectID
		}
	}
	valueBytes := []byte(input.Value)
	defer clear(valueBytes)
	hash := sha256.Sum256(valueBytes)
	scope := requestidempotency.Scope{Kind: requestidempotency.ScopePlatform}
	if locator.ScopeKind == idempotency.IdempotencyScopeProject {
		scope = requestidempotency.Scope{Kind: requestidempotency.ScopeProject, ID: locator.ScopeID}
	}
	version, digest, err := requestidempotency.Canonicalize(ctx, requestidempotency.CanonicalIntentV1{Method: http.MethodPatch, Route: "/secrets/{id}", Scope: scope, Path: []requestidempotency.PathBinding{{Name: "id", Value: id}}, Query: requestidempotency.Object(), Body: requestidempotency.JSONBody(requestidempotency.Object(requestidempotency.Field{Name: "revision", Value: requestidempotency.Integer(input.Revision)}, requestidempotency.Field{Name: "value_sha256", Value: requestidempotency.String(hex.EncodeToString(hash[:]))}))})
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	defer digest.Destroy()
	evidence, err := service.coordinator.ProtectIntent(ctx, version, digest)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	defer evidence.Destroy()
	resolution, found, err := service.coordinator.ResolveExisting(ctx, service.evidence, locator, evidence)
	if err != nil || found {
		return resolution.Response, err
	}
	current, err := service.repository.GetSecret(ctx, id)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	if current.Revision != input.Revision {
		return idempotency.IdempotencyResponse{}, errs.New(errs.KindStateConflict, "Secret changed; refresh before replacing its value")
	}
	owner := secretrecord.PlatformOwner()
	if current.Record.Secret.ProjectID != "" {
		project, err := service.repository.GetProject(ctx, current.Record.Secret.ProjectID)
		if err != nil {
			return idempotency.IdempotencyResponse{}, err
		}
		owner = secretrecord.ProjectOwner(project)
	}
	envelope, err := service.protector.Seal(ctx, valueBytes)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	metadata := envelope.Metadata()
	ciphertext := envelope.Ciphertext()
	defer clear(ciphertext)
	value := secretrecord.EncryptedValue{SecretID: id, EnvelopeVersion: uint8(metadata.Version), Cipher: string(metadata.Cipher), DigestAlgorithm: string(metadata.Digest.Algorithm), CiphertextSHA256: metadata.Digest.Value, Ciphertext: ciphertext}
	replacement := current.Record
	replacement.Secret.UpdatedAt = time.Now().UTC()
	body, err := json.Marshal(secretCreationResponse(replacement))
	if err != nil {
		return idempotency.IdempotencyResponse{}, errs.Wrap(errs.KindInternal, err)
	}
	response := idempotency.IdempotencyResponse{Status: http.StatusOK, ContentKind: "application/json", Body: body}
	durable, err := evidence.DurableRecord()
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	defer clear(durable.Ciphertext)
	marker, err := idempotency.NewCompletedDirectIdempotencyMarker(locator, durable, response, replacement.Secret.UpdatedAt)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	marker.ReplayTarget = &target
	defer clear(marker.Intent.Ciphertext)
	result, mutationErr := service.repository.secrets.ReplaceSecretValueIdempotent(ctx, owner, current, replacement, value, marker)
	if mutationErr != nil {
		if !isUnknownSecretCreationOutcome(mutationErr) {
			return idempotency.IdempotencyResponse{}, mutationErr
		}
		resolution, err = service.coordinator.ResolveUnknown(ctx, service.evidence, locator, evidence, mutationErr)
	} else {
		resolution, err = service.coordinator.ResolveKnown(ctx, evidence, result)
	}
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	if resolution.Kind == requestidempotency.ResolutionApplied {
		return response, nil
	}
	return resolution.Response, nil
}
