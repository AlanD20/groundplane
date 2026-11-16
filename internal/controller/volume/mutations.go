package volume

import (
	"context"
	"crypto/sha256"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"net/http"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/slug"
	"github.com/AlanD20/groundplane/internal/common/volumeidentity"
	"github.com/AlanD20/groundplane/internal/controller/acceptedaction"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	volumeCreationRoute        = "/volumes"
	volumeIdentityRoute        = "/volumes/{id}"
	volumeMutationActionAdd    = "add"
	volumeMutationActionEdit   = "edit"
	volumeMutationActionRemove = "remove"
)

type MutationService struct {
	accepted    *acceptedaction.Submitter
	repository  MutationRepository
	idempotency *mutationIdempotency
	reads       *ReadService
	policies    *etcd.BackupPolicyRepository
	now         func() time.Time
}

func NewMutationService(
	repository MutationRepository,
	coordinator *requestidempotency.Coordinator,
	idempotencyRepository *etcd.IdempotencyRepository,
	reads *ReadService,
	policies *etcd.BackupPolicyRepository,
	accepted *acceptedaction.Submitter,
) (*MutationService, error) {
	if repository == nil || reads == nil || policies == nil || accepted == nil {
		return nil, errs.New(errs.KindInternal, "Volume mutation service is not configured")
	}
	idempotency, err := newMutationIdempotency(coordinator, idempotencyRepository)
	if err != nil {
		return nil, err
	}
	return &MutationService{
		repository: repository, idempotency: idempotency, reads: reads, now: time.Now,
		policies: policies, accepted: accepted,
	}, nil
}

func (service *MutationService) CreateVolume(
	ctx context.Context,
	input apiTypes.VolumeCreate,
	idempotencyKey string,
) (idempotencyrecord.IdempotencyResponse, error) {
	if ctx == nil || ids.Validate(ids.KindEnvironment, input.EnvironmentID) != nil ||
		slug.Validate("volume slug", input.Slug) != nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(
			errs.KindValidationFailed,
			"Volume creation input is invalid",
		)
	}
	keySupplied := input.Key != ""
	if input.Key == "" {
		input.Key = input.Slug
	}
	if err := volumeidentity.ValidateKey(input.Key); err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	intent := mutationIntent{
		method: http.MethodPost, route: volumeCreationRoute, environmentID: input.EnvironmentID,
		query: requestidempotency.Object(),
		body: requestidempotency.JSONBody(requestidempotency.Object(
			requestidempotency.Field{Name: "environment_id", Value: requestidempotency.String(input.EnvironmentID)},
			requestidempotency.Field{Name: "key", Value: requestidempotency.String(input.Key)},
			requestidempotency.Field{Name: "slug", Value: requestidempotency.String(input.Slug)},
			requestidempotency.Field{Name: "after", Value: acceptedaction.AfterValue(input.After)},
		)),
	}
	return service.acceptMutation(ctx, volumeMutationRequest{
		action: volumeMutationActionAdd, environmentID: input.EnvironmentID,
		slug: input.Slug, key: input.Key, keySupplied: keySupplied,
		idempotencyKey: idempotencyKey, intent: intent, after: append([]string(nil), input.After...),
	})
}

func (service *MutationService) EditVolume(
	ctx context.Context,
	volumeID string,
	input apiTypes.VolumeEdit,
	idempotencyKey string,
) (idempotencyrecord.IdempotencyResponse, error) {
	if ctx == nil || ids.Validate(ids.KindVolume, volumeID) != nil || slug.Validate("volume slug", input.Slug) != nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(
			errs.KindValidationFailed,
			"Volume edit input is invalid",
		)
	}
	projection, identity, err := service.repository.FindEnvironmentVolumeIdentity(ctx, volumeID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	intent := mutationIntent{
		method: http.MethodPatch, route: volumeIdentityRoute,
		environmentID: projection.Record.EnvironmentID, volumeID: volumeID,
		query: requestidempotency.Object(),
		body: requestidempotency.JSONBody(requestidempotency.Object(
			requestidempotency.Field{Name: "slug", Value: requestidempotency.String(input.Slug)},
			requestidempotency.Field{Name: "after", Value: acceptedaction.AfterValue(input.After)},
		)),
	}
	return service.acceptMutation(ctx, volumeMutationRequest{
		action: volumeMutationActionEdit, environmentID: projection.Record.EnvironmentID,
		volumeID: volumeID, slug: input.Slug, key: identity.Key,
		idempotencyKey: idempotencyKey, intent: intent, after: append([]string(nil), input.After...),
	})
}

func (service *MutationService) RemoveVolume(
	ctx context.Context,
	volumeID string,
	impactToken string,
	confirmKey string,
	input apiTypes.TaskRunAfterRequest,
	idempotencyKey string,
) (idempotencyrecord.IdempotencyResponse, error) {
	if ctx == nil || ids.Validate(ids.KindVolume, volumeID) != nil || len(impactToken) != sha256.Size*2 {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(
			errs.KindValidationFailed,
			"Volume removal input is invalid",
		)
	}
	target := idempotencyrecord.IdempotencyReplayTarget{
		Kind: idempotencyrecord.IdempotencyReplayTargetVolume,
		ID:   volumeID,
	}
	locator, indexed, err := service.idempotency.ResolveReplayLocator(
		ctx, target, http.MethodDelete, volumeIdentityRoute, idempotencyKey,
	)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if indexed {
		return service.replayVolumeRemoval(ctx, locator, volumeID, impactToken, confirmKey, input.After)
	}
	projection, identity, err := service.repository.FindEnvironmentVolumeIdentity(ctx, volumeID)
	if err != nil {
		if kind, ok := errs.KindOf(err); ok && kind == errs.KindVolumeNotFound {
			locator, indexed, replayErr := service.idempotency.ResolveReplayLocator(
				ctx, target, http.MethodDelete, volumeIdentityRoute, idempotencyKey,
			)
			if replayErr != nil {
				return idempotencyrecord.IdempotencyResponse{}, replayErr
			}
			if indexed {
				return service.replayVolumeRemoval(ctx, locator, volumeID, impactToken, confirmKey, input.After)
			}
		}
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	intent := volumeRemovalIntent(projection.Record.EnvironmentID, volumeID, impactToken, confirmKey, input.After)
	return service.acceptMutation(ctx, volumeMutationRequest{
		action: volumeMutationActionRemove, environmentID: projection.Record.EnvironmentID,
		volumeID: volumeID, slug: identity.Slug, key: identity.Key, keySupplied: true,
		impactToken: impactToken, confirmKey: confirmKey, idempotencyKey: idempotencyKey, intent: intent, after: append([]string(nil), input.After...),
	})
}

func (service *MutationService) replayVolumeRemoval(
	ctx context.Context,
	locator idempotencyrecord.IdempotencyLocator,
	volumeID string,
	impactToken string,
	confirmKey string,
	after []string,
) (idempotencyrecord.IdempotencyResponse, error) {
	intent := volumeRemovalIntent(locator.ScopeID, volumeID, impactToken, confirmKey, after)
	evidence, err := service.idempotency.Prepare(ctx, intent)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	defer clear(evidence.durable.Ciphertext)
	resolution, existing, err := service.idempotency.ResolveOperationRootExisting(ctx, locator, evidence)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if !existing {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(
			errs.KindInternal,
			"Volume removal replay index is inconsistent",
		)
	}
	return replayResponse(resolution)
}

func volumeRemovalIntent(
	environmentID string,
	volumeID string,
	impactToken string,
	confirmKey string,
	after []string,
) mutationIntent {
	return mutationIntent{
		method: http.MethodDelete, route: volumeIdentityRoute,
		environmentID: environmentID, volumeID: volumeID,
		query: requestidempotency.Object(
			requestidempotency.Field{Name: "confirm_key", Value: requestidempotency.String(confirmKey)},
			requestidempotency.Field{Name: "impact_token", Value: requestidempotency.String(impactToken)},
		),
		body: requestidempotency.JSONBody(
			requestidempotency.Object(requestidempotency.Field{Name: "after", Value: acceptedaction.AfterValue(after)}),
		),
	}
}

type volumeMutationRequest struct {
	after          []string
	confirmKey     string
	action         string
	environmentID  string
	volumeID       string
	slug           string
	key            string
	keySupplied    bool
	impactToken    string
	idempotencyKey string
	intent         mutationIntent
}
