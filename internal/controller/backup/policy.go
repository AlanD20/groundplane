package backup

import (
	"context"
	"encoding/json"
	"errors"
	backuppolicy "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"net/http"
	"time"

	"filippo.io/age"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const backupPolicyReplacementRoute = "/environments/{id}/backup-policy"

type backupPolicyRepository interface {
	GetBackupPolicyProjection(context.Context, string) (etcd.BackupPolicyProjection, error)
	PrepareBackupPolicyReplacement(
		context.Context,
		backuppolicy.BackupPolicyReplacementInput,
	) (etcd.PreparedBackupPolicyReplacement, bool, error)
	SupplyBackupPolicyInitialKey(
		context.Context,
		etcd.PreparedBackupPolicyReplacement,
		etcd.BackupPolicyInitialKeyMaterial,
	) (etcd.PreparedBackupPolicyReplacement, error)
	FinalizeBackupPolicySchedule(
		etcd.PreparedBackupPolicyReplacement,
		time.Time,
	) (etcd.PreparedBackupPolicyReplacement, etcd.BackupPolicyProjection, error)
	ReplaceBackupPolicyProtected(
		context.Context,
		etcd.PreparedBackupPolicyReplacement,
		idempotencyrecord.IdempotencyMarker,
	) (etcd.IdempotencyTransactionResult, error)
}

type backupPolicyEvidence struct {
	candidate requestidempotency.ProtectedEvidence
}

type backupPolicyIdempotency interface {
	Prepare(context.Context, string, apiTypes.BackupPolicyReplacementRequest) (backupPolicyEvidence, error)
	ResolveExisting(
		context.Context, idempotencyrecord.IdempotencyLocator, backupPolicyEvidence,
	) (requestidempotency.Resolution, bool, error)
	NewMarker(
		backupPolicyEvidence, idempotencyrecord.IdempotencyLocator, idempotencyrecord.IdempotencyResponse, time.Time,
	) (idempotencyrecord.IdempotencyMarker, error)
	ResolveKnown(
		context.Context, backupPolicyEvidence, etcd.IdempotencyTransactionResult,
	) (requestidempotency.Resolution, error)
	ResolveUnknown(
		context.Context, idempotencyrecord.IdempotencyLocator, backupPolicyEvidence, error,
	) (requestidempotency.Resolution, error)
}

type durableBackupPolicyIdempotency struct {
	coordinator *requestidempotency.Coordinator
	repository  *etcd.IdempotencyRepository
}

func NewDurableBackupPolicyIdempotency(
	coordinator *requestidempotency.Coordinator,
	repository *etcd.IdempotencyRepository,
) (*durableBackupPolicyIdempotency, error) {
	if coordinator == nil || repository == nil {
		return nil, errs.New(errs.KindInternal, "backup policy idempotency dependencies are required")
	}
	return &durableBackupPolicyIdempotency{coordinator: coordinator, repository: repository}, nil
}

func (service *durableBackupPolicyIdempotency) Prepare(
	ctx context.Context,
	environmentID string,
	input apiTypes.BackupPolicyReplacementRequest,
) (backupPolicyEvidence, error) {
	version, digest, err := requestidempotency.Canonicalize(ctx, backupPolicyIntent(environmentID, input))
	if err != nil {
		return backupPolicyEvidence{}, err
	}
	defer digest.Destroy()
	candidate, err := service.coordinator.ProtectIntent(ctx, version, digest)
	if err != nil {
		return backupPolicyEvidence{}, err
	}
	return backupPolicyEvidence{candidate: candidate}, nil
}

func (service *durableBackupPolicyIdempotency) ResolveExisting(
	ctx context.Context,
	locator idempotencyrecord.IdempotencyLocator,
	evidence backupPolicyEvidence,
) (requestidempotency.Resolution, bool, error) {
	return service.coordinator.ResolveExisting(ctx, service.repository, locator, evidence.candidate)
}

func (*durableBackupPolicyIdempotency) NewMarker(
	evidence backupPolicyEvidence,
	locator idempotencyrecord.IdempotencyLocator,
	response idempotencyrecord.IdempotencyResponse,
	now time.Time,
) (idempotencyrecord.IdempotencyMarker, error) {
	intent, err := evidence.candidate.DurableRecord()
	if err != nil {
		return idempotencyrecord.IdempotencyMarker{}, err
	}
	defer clear(intent.Ciphertext)
	return idempotencyrecord.NewCompletedDirectIdempotencyMarker(locator, intent, response, now)
}

func (service *durableBackupPolicyIdempotency) ResolveKnown(
	ctx context.Context,
	evidence backupPolicyEvidence,
	result etcd.IdempotencyTransactionResult,
) (requestidempotency.Resolution, error) {
	return service.coordinator.ResolveKnown(ctx, evidence.candidate, result)
}

func (service *durableBackupPolicyIdempotency) ResolveUnknown(
	ctx context.Context,
	locator idempotencyrecord.IdempotencyLocator,
	evidence backupPolicyEvidence,
	original error,
) (requestidempotency.Resolution, error) {
	return service.coordinator.ResolveUnknown(ctx, service.repository, locator, evidence.candidate, original)
}

type backupPolicyKeyFactory interface {
	Create(context.Context) (etcd.BackupPolicyInitialKeyMaterial, error)
}

type ageBackupPolicyKeyFactory struct {
	protector *secretvalue.Protector
}

func NewAgeBackupPolicyKeyFactory(protector *secretvalue.Protector) (*ageBackupPolicyKeyFactory, error) {
	if protector == nil {
		return nil, errs.New(errs.KindInternal, "backup policy key protector is required")
	}
	return &ageBackupPolicyKeyFactory{protector: protector}, nil
}

func (factory *ageBackupPolicyKeyFactory) Create(
	ctx context.Context,
) (etcd.BackupPolicyInitialKeyMaterial, error) {
	identity, err := age.GenerateX25519Identity()
	if err != nil {
		return etcd.BackupPolicyInitialKeyMaterial{}, errs.Wrap(errs.KindInternal, err)
	}
	plaintext := []byte(identity.String())
	defer clear(plaintext)
	envelope, err := factory.protector.Seal(ctx, plaintext)
	if err != nil {
		return etcd.BackupPolicyInitialKeyMaterial{}, err
	}
	return etcd.BackupPolicyInitialKeyMaterial{
		Recipient: identity.Recipient().String(), Ciphertext: envelope.Ciphertext(),
	}, nil
}

type backupPolicyService struct {
	repository  backupPolicyRepository
	keys        backupPolicyKeyFactory
	idempotency backupPolicyIdempotency
	now         func() time.Time
}

func NewBackupPolicyService(
	repository backupPolicyRepository,
	keys backupPolicyKeyFactory,
	idempotency backupPolicyIdempotency,
) (*backupPolicyService, error) {
	if repository == nil || keys == nil || idempotency == nil {
		return nil, errs.New(errs.KindInternal, "backup policy service dependencies are required")
	}
	return &backupPolicyService{
		repository: repository, keys: keys, idempotency: idempotency,
		now: func() time.Time { return time.Now().UTC() },
	}, nil
}

func (service *backupPolicyService) GetBackupPolicy(
	ctx context.Context,
	environmentID string,
) (apiTypes.BackupPolicy, error) {
	projection, err := service.repository.GetBackupPolicyProjection(ctx, environmentID)
	if err != nil {
		return apiTypes.BackupPolicy{}, err
	}
	return backupPolicyAPI(projection), nil
}

func (service *backupPolicyService) SetBackupPolicy(
	ctx context.Context,
	environmentID string,
	input apiTypes.BackupPolicyReplacementRequest,
	idempotencyKey string,
) (apiTypes.BackupPolicyMutationResult, error) {
	if ctx == nil {
		return apiTypes.BackupPolicyMutationResult{}, errs.New(
			errs.KindInternal, "backup policy context is required",
		)
	}
	if input.Keep < 1 || input.Keep > apiTypes.MaximumBackupPolicyKeep {
		return apiTypes.BackupPolicyMutationResult{}, errs.New(
			errs.KindValidationFailed,
			"backup policy keep must be between 1 and 9007199254740991",
		)
	}
	if len(input.Sources) > apiTypes.MaximumBackupPolicySources {
		return apiTypes.BackupPolicyMutationResult{}, errs.New(
			errs.KindValidationFailed,
			"backup policy may select at most 12 sources",
		)
	}
	evidence, err := service.idempotency.Prepare(ctx, environmentID, input)
	if err != nil {
		return apiTypes.BackupPolicyMutationResult{}, err
	}
	locator := backupPolicyLocator(environmentID, idempotencyKey)
	resolution, existing, err := service.idempotency.ResolveExisting(ctx, locator, evidence)
	if err != nil {
		return apiTypes.BackupPolicyMutationResult{}, err
	}
	if existing {
		return decodeBackupPolicyResponse(resolution.Response)
	}
	prepared, needsInitialKey, err := service.repository.PrepareBackupPolicyReplacement(
		ctx,
		backupPolicyReplacementInput(environmentID, input),
	)
	if err != nil {
		return apiTypes.BackupPolicyMutationResult{}, err
	}
	defer prepared.Destroy()
	if needsInitialKey {
		material, createErr := service.keys.Create(ctx)
		if createErr != nil {
			return apiTypes.BackupPolicyMutationResult{}, createErr
		}
		defer clear(material.Ciphertext)
		prepared, err = service.repository.SupplyBackupPolicyInitialKey(ctx, prepared, material)
		if err != nil {
			return apiTypes.BackupPolicyMutationResult{}, err
		}
	}
	transitionAt := service.now().UTC().Truncate(time.Second)
	var projection etcd.BackupPolicyProjection
	prepared, projection, err = service.repository.FinalizeBackupPolicySchedule(prepared, transitionAt)
	if err != nil {
		return apiTypes.BackupPolicyMutationResult{}, err
	}
	policy := backupPolicyAPI(projection)
	responseBody, err := json.Marshal(policy)
	if err != nil {
		return apiTypes.BackupPolicyMutationResult{}, errs.Wrap(errs.KindInternal, err)
	}
	defer clear(responseBody)
	response := idempotencyrecord.IdempotencyResponse{
		Status: http.StatusOK, ContentKind: "application/json", Body: append([]byte(nil), responseBody...),
	}
	defer clear(response.Body)
	marker, err := service.idempotency.NewMarker(evidence, locator, response, transitionAt)
	if err != nil {
		return apiTypes.BackupPolicyMutationResult{}, err
	}
	defer clear(marker.Intent.Ciphertext)
	defer clear(marker.Response.Body)
	result, replaceErr := service.repository.ReplaceBackupPolicyProtected(ctx, prepared, marker)
	if replaceErr != nil {
		if isUnknownBackupPolicyOutcome(replaceErr) {
			resolution, err = service.idempotency.ResolveUnknown(ctx, locator, evidence, replaceErr)
			if err != nil {
				return apiTypes.BackupPolicyMutationResult{}, err
			}
			return decodeBackupPolicyResponse(resolution.Response)
		}
		return apiTypes.BackupPolicyMutationResult{}, replaceErr
	}
	resolution, err = service.idempotency.ResolveKnown(ctx, evidence, result)
	if err != nil {
		return apiTypes.BackupPolicyMutationResult{}, err
	}
	if resolution.Kind == requestidempotency.ResolutionApplied {
		resolution.Response = idempotencyrecord.IdempotencyResponse{
			Status: response.Status, ContentKind: response.ContentKind,
			Body: append([]byte(nil), response.Body...),
		}
	}
	return decodeBackupPolicyResponse(resolution.Response)
}

func backupPolicyReplacementInput(
	environmentID string,
	input apiTypes.BackupPolicyReplacementRequest,
) backuppolicy.BackupPolicyReplacementInput {
	sources := make([]backuppolicy.BackupPolicySourceSelection, len(input.Sources))
	for index, source := range input.Sources {
		sources[index] = backuppolicy.BackupPolicySourceSelection{
			Kind: core.BackupSourceKind(source.Kind), TargetID: source.TargetID,
		}
	}
	return backuppolicy.BackupPolicyReplacementInput{
		EnvironmentID: environmentID, Enabled: input.Enabled, Frequency: input.Frequency,
		Keep: input.Keep, Encryption: string(input.Encryption), ConnectorID: input.ConnectorID,
		Sources: sources,
	}
}

func backupPolicyAPI(projection etcd.BackupPolicyProjection) apiTypes.BackupPolicy {
	sources := make([]apiTypes.BackupSource, len(projection.Sources))
	for index, source := range projection.Sources {
		sources[index] = apiTypes.BackupSource{
			ID: source.ID, Kind: apiTypes.BackupSourceKind(source.Kind), TargetID: source.TargetID,
		}
	}
	return apiTypes.BackupPolicy{
		Enabled:   projection.Enabled,
		Frequency: projection.Frequency,
		Keep:      projection.Keep,
		Encryption: apiTypes.BackupEncryption(
			projection.Encryption,
		),
		ConnectorID:  projection.ConnectorID,
		Sources:      sources,
		AgeRecipient: projection.AgeRecipient,
		KeyEra:       projection.KeyEra,
		KeyCreatedAt: backupPolicyTimestamp(projection.KeyCreatedAt),
		KeyRotatedAt: backupPolicyTimestamp(projection.KeyRotatedAt),
		NextRunAt:    backupPolicyTimestampPointer(projection.NextRunAt),
	}
}

func backupPolicyTimestampPointer(value *time.Time) *string {
	if value == nil {
		return nil
	}
	formatted := value.UTC().Format(time.RFC3339)
	return &formatted
}

func backupPolicyTimestamp(value time.Time) string {
	if value.IsZero() {
		return ""
	}
	return value.UTC().Format(time.RFC3339)
}

func backupPolicyLocator(environmentID string, key string) idempotencyrecord.IdempotencyLocator {
	return idempotencyrecord.IdempotencyLocator{
		ScopeKind: idempotencyrecord.IdempotencyScopeEnvironment, ScopeID: environmentID,
		Method: http.MethodPut, Route: backupPolicyReplacementRoute, Key: key,
	}
}

func backupPolicyIntent(
	environmentID string,
	input apiTypes.BackupPolicyReplacementRequest,
) requestidempotency.CanonicalIntentV1 {
	sources := make([]requestidempotency.Value, len(input.Sources))
	for index, source := range input.Sources {
		sources[index] = requestidempotency.Object(
			requestidempotency.Field{Name: "kind", Value: requestidempotency.String(string(source.Kind))},
			requestidempotency.Field{Name: "target_id", Value: requestidempotency.String(source.TargetID)},
		)
	}
	return requestidempotency.CanonicalIntentV1{
		Method: http.MethodPut, Route: backupPolicyReplacementRoute,
		Scope: requestidempotency.Scope{Kind: requestidempotency.ScopeEnvironment, ID: environmentID},
		Path:  []requestidempotency.PathBinding{{Name: "id", Value: environmentID}},
		Query: requestidempotency.Object(),
		Body: requestidempotency.JSONBody(requestidempotency.Object(
			requestidempotency.Field{Name: "enabled", Value: requestidempotency.Bool(input.Enabled)},
			requestidempotency.Field{Name: "frequency", Value: requestidempotency.String(input.Frequency)},
			requestidempotency.Field{Name: "keep", Value: requestidempotency.Integer(input.Keep)},
			requestidempotency.Field{Name: "encryption", Value: requestidempotency.String(string(input.Encryption))},
			requestidempotency.Field{Name: "connector_id", Value: requestidempotency.String(input.ConnectorID)},
			requestidempotency.Field{Name: "sources", Value: requestidempotency.List(sources...)},
		)),
	}
}

func decodeBackupPolicyResponse(
	response idempotencyrecord.IdempotencyResponse,
) (apiTypes.BackupPolicyMutationResult, error) {
	if response.Status != http.StatusOK || response.ContentKind != "application/json" {
		return apiTypes.BackupPolicyMutationResult{}, errs.New(
			errs.KindInternal, "backup policy replay response is invalid",
		)
	}
	var policy apiTypes.BackupPolicy
	if err := json.Unmarshal(response.Body, &policy); err != nil || policy.Sources == nil {
		return apiTypes.BackupPolicyMutationResult{}, errs.New(
			errs.KindInternal, "backup policy replay body is invalid",
		)
	}
	return apiTypes.BackupPolicyMutationResult{
		Policy: policy, Representation: append([]byte(nil), response.Body...),
	}, nil
}

func isUnknownBackupPolicyOutcome(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	kind, ok := errs.KindOf(err)
	return ok && kind == errs.KindStorageUnavailable
}
