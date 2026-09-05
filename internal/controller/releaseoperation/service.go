package releaseoperation

import (
	"context"
	"net/http"
	"strconv"
	"strings"
	"time"

	controllerpkg "github.com/AlanD20/groundplane/internal/controller"
	"github.com/AlanD20/groundplane/internal/controller/idempotentintent"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	etcdrg "github.com/AlanD20/groundplane/internal/infra/etcd/releasegroup"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	serviceDeployRoute             = "/services/{id}/deploy"
	serviceRollbackRoute           = "/services/{id}/rollback"
	releaseGroupDeployRoute        = "/release-groups/{id}/deploy"
	releaseGroupRollbackRoute      = "/release-groups/{id}/rollback"
	defaultReleaseExecutionTimeout = 15 * time.Hour
)

type Service struct {
	ledger      *etcd.ReleaseLedger
	services    *etcd.ServiceRepository
	groups      *etcdrg.Store
	idempotency *etcd.IdempotencyRepository
	coordinator *idempotentintent.Coordinator
	plans       *controllerpkg.TaskPlanResolver
	scripts     *etcd.ScriptRepository
	artifacts   *controllerpkg.ScriptArtifactService
	timeout     time.Duration
	now         func() time.Time
}

type releaseCandidateInput struct {
	planning       etcd.ReleasePlanningService
	image          string
	tag            string
	digest         string
	priorImage     string
	priorReleaseID string
	strategy       domain.Strategy
	priorStrategy  domain.Strategy
	slot           domain.Slot
	onFailure      domain.OnFailure
	rollbackSource string
}

func NewService(
	ledger *etcd.ReleaseLedger,
	services *etcd.ServiceRepository,
	groups *etcdrg.Store,
	idempotency *etcd.IdempotencyRepository,
	coordinator *idempotentintent.Coordinator,
	plans *controllerpkg.TaskPlanResolver,
	scripts *etcd.ScriptRepository,
	artifacts *controllerpkg.ScriptArtifactService,
	timeout time.Duration,
) (*Service, error) {
	if ledger == nil || services == nil || groups == nil || idempotency == nil || coordinator == nil ||
		plans == nil || scripts == nil || artifacts == nil {
		return nil, errs.New(errs.KindInternal, "release operation dependencies are not configured")
	}
	if timeout == 0 {
		timeout = defaultReleaseExecutionTimeout
	}
	if timeout < 40*time.Minute || timeout > 24*time.Hour || timeout%time.Second != 0 {
		return nil, errs.New(errs.KindValidationFailed, "release execution timeout must be an integral duration from 40m through 24h")
	}
	return &Service{
		ledger: ledger, services: services, groups: groups, idempotency: idempotency,
		coordinator: coordinator, plans: plans, scripts: scripts, artifacts: artifacts,
		timeout: timeout, now: time.Now,
	}, nil
}

func (service *Service) DeployService(
	ctx context.Context,
	serviceID string,
	request domain.ServiceDeployInput,
	idempotencyKey string,
) (etcd.IdempotencyResponse, error) {
	current, err := service.services.GetService(ctx, serviceID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	body := idempotentintent.JSONBody(idempotentintent.Object(
		idempotentintent.Field{Name: "tag", Value: idempotentintent.String(request.Tag)},
		idempotentintent.Field{Name: "strategy", Value: idempotentintent.String(string(request.Strategy))},
		idempotentintent.Field{Name: "on_failure", Value: idempotentintent.String(string(request.OnFailure))},
	))
	locator, protected, durable, replay, err := service.begin(
		ctx, current.Record.EnvironmentID, http.MethodPost, serviceDeployRoute, serviceID,
		idempotencyKey, body,
	)
	if err != nil || replay != nil {
		if replay != nil {
			return *replay, nil
		}
		return etcd.IdempotencyResponse{}, err
	}
	defer protected.Destroy()
	defer clear(durable.Ciphertext)
	scope, err := service.ledger.LoadPlanningScope(ctx, current.Record.EnvironmentID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	planning, err := service.ledger.LoadPlanningServices(ctx, scope, []string{serviceID})
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	selected, err := service.deployCandidate(ctx, scope, planning[0], request.Tag, string(request.Strategy), request.OnFailure)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	return service.publish(ctx, scope, etcd.ReleaseDesiredService, serviceID, planning[0].Service.Revision,
		"", []releaseCandidateInput{selected}, locator, durable, protected)
}

func (service *Service) RollbackService(
	ctx context.Context,
	serviceID string,
	request domain.ServiceRollbackInput,
	idempotencyKey string,
) (etcd.IdempotencyResponse, error) {
	current, err := service.services.GetService(ctx, serviceID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	body := releaseRollbackRequestBody(request)
	locator, protected, durable, replay, err := service.begin(
		ctx, current.Record.EnvironmentID, http.MethodPost, serviceRollbackRoute, serviceID,
		idempotencyKey, body,
	)
	if err != nil || replay != nil {
		if replay != nil {
			return *replay, nil
		}
		return etcd.IdempotencyResponse{}, err
	}
	defer protected.Destroy()
	defer clear(durable.Ciphertext)
	scope, err := service.ledger.LoadPlanningScope(ctx, current.Record.EnvironmentID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	planning, err := service.ledger.LoadPlanningServices(ctx, scope, []string{serviceID})
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	selected, err := service.rollbackCandidate(ctx, scope, planning[0], request.Tag, "")
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	return service.publish(ctx, scope, etcd.ReleaseDesiredService, serviceID, planning[0].Service.Revision,
		"", []releaseCandidateInput{selected}, locator, durable, protected)
}

func (service *Service) DeployReleaseGroup(
	ctx context.Context,
	groupID string,
	request domain.GroupDeployInput,
	idempotencyKey string,
) (etcd.IdempotencyResponse, error) {
	current, err := service.groups.Get(ctx, groupID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	body := idempotentintent.JSONBody(idempotentintent.Object(
		idempotentintent.Field{Name: "tag", Value: idempotentintent.String(request.Tag)},
	))
	locator, protected, durable, replay, err := service.begin(
		ctx, current.Group.EnvironmentID, http.MethodPost, releaseGroupDeployRoute, groupID,
		idempotencyKey, body,
	)
	if err != nil || replay != nil {
		if replay != nil {
			return *replay, nil
		}
		return etcd.IdempotencyResponse{}, err
	}
	defer protected.Destroy()
	defer clear(durable.Ciphertext)
	scope, err := service.ledger.LoadPlanningScope(ctx, current.Group.EnvironmentID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	group, err := service.groups.GetAtRevision(ctx, groupID, scope.ReadRevision)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	tag := request.Tag
	if tag == "" {
		tag = group.Group.DefaultTag
	}
	if tag == "" {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindReleaseGroupTagRequired, "release group deploy requires a tag or group default")
	}
	planning, err := service.ledger.LoadPlanningServices(ctx, scope, group.Group.Order)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	candidates := make([]releaseCandidateInput, len(planning))
	for index := range planning {
		candidates[index], err = service.deployCandidate(ctx, scope, planning[index], tag, "", domain.OnFailure(group.Group.OnFailure))
		if err != nil {
			return etcd.IdempotencyResponse{}, err
		}
	}
	return service.publish(ctx, scope, etcd.ReleaseDesiredGroup, groupID, group.Revision,
		groupID, candidates, locator, durable, protected)
}

func (service *Service) RollbackReleaseGroup(ctx context.Context, groupID string,
	request domain.GroupRollbackInput, idempotencyKey string) (etcd.IdempotencyResponse, error) {
	current, err := service.groups.Get(ctx, groupID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	locator, protected, durable, replay, err := service.begin(
		ctx, current.Group.EnvironmentID, http.MethodPost, releaseGroupRollbackRoute, groupID,
		idempotencyKey, groupRollbackRequestBody(request),
	)
	if err != nil || replay != nil {
		if replay != nil {
			return *replay, nil
		}
		return etcd.IdempotencyResponse{}, err
	}
	defer protected.Destroy()
	defer clear(durable.Ciphertext)
	selection, err := service.selectGroupRollback(ctx, current.Group.EnvironmentID, groupID, request.Tag, request.PreviewRevision)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	return service.publish(ctx, selection.scope, etcd.ReleaseDesiredGroup, groupID, selection.group.Revision,
		groupID, selection.candidates, locator, durable, protected)
}

func (service *Service) PreviewReleaseGroupRollback(ctx context.Context, groupID string, input domain.GroupRollbackPreviewInput) (domain.GroupRollbackPreview, error) {
	current, err := service.groups.Get(ctx, groupID)
	if err != nil {
		return domain.GroupRollbackPreview{}, err
	}
	selection, err := service.selectGroupRollback(ctx, current.Group.EnvironmentID, groupID, input.Tag, nil)
	if err != nil {
		return domain.GroupRollbackPreview{}, err
	}
	sources := make([]domain.RollbackSource, len(selection.candidates))
	for index, candidate := range selection.candidates {
		sources[index] = domain.RollbackSource{ServiceID: candidate.planning.Service.Record.Desired.ID, ReleaseID: candidate.rollbackSource, Tag: candidate.tag}
	}
	return domain.GroupRollbackPreview{GroupID: groupID, Revision: selection.scope.ReadRevision, Sources: sources}, nil
}

type groupRollbackSelection struct {
	scope      etcd.ReleasePlanningScope
	group      etcdrg.Versioned
	candidates []releaseCandidateInput
}

func (service *Service) selectGroupRollback(ctx context.Context, environmentID, groupID string, tag *string, revision *int64) (groupRollbackSelection, error) {
	var scope etcd.ReleasePlanningScope
	var err error
	if revision == nil {
		scope, err = service.ledger.LoadPlanningScope(ctx, environmentID)
	} else {
		scope, err = service.ledger.LoadPlanningScopeAtRevision(ctx, environmentID, *revision)
	}
	if err != nil {
		return groupRollbackSelection{}, err
	}
	group, err := service.groups.GetAtRevision(ctx, groupID, scope.ReadRevision)
	if err != nil {
		return groupRollbackSelection{}, err
	}
	planning, err := service.ledger.LoadPlanningServices(ctx, scope, group.Group.Order)
	if err != nil {
		return groupRollbackSelection{}, err
	}
	candidates := make([]releaseCandidateInput, len(planning))
	explicitTag := ""
	if tag != nil {
		explicitTag = *tag
	}
	for index := range planning {
		candidates[index], err = service.rollbackCandidate(ctx, scope, planning[index], explicitTag, domain.OnFailure(group.Group.OnFailure))
		if err != nil {
			return groupRollbackSelection{}, err
		}
	}
	return groupRollbackSelection{scope: scope, group: group, candidates: candidates}, nil
}

func releaseRollbackRequestBody(request domain.ServiceRollbackInput) idempotentintent.Body {
	return idempotentintent.JSONBody(idempotentintent.Object(idempotentintent.Field{Name: "tag", Value: idempotentintent.String(request.Tag)}))
}

func groupRollbackRequestBody(request domain.GroupRollbackInput) idempotentintent.Body {
	fields := make([]idempotentintent.Field, 0, 2)
	if request.Tag != nil {
		fields = append(fields, idempotentintent.Field{Name: "tag", Value: idempotentintent.String(*request.Tag)})
	}
	if request.PreviewRevision != nil {
		fields = append(fields, idempotentintent.Field{Name: "preview_revision", Value: idempotentintent.String(strconv.FormatInt(*request.PreviewRevision, 10))})
	}
	return idempotentintent.JSONBody(idempotentintent.Object(fields...))
}

func (service *Service) begin(
	ctx context.Context,
	environmentID string,
	method string,
	route string,
	targetID string,
	key string,
	body idempotentintent.Body,
) (etcd.IdempotencyLocator, idempotentintent.ProtectedEvidence, etcd.ProtectedIntentRecord, *etcd.IdempotencyResponse, error) {
	locator := etcd.IdempotencyLocator{
		ScopeKind: etcd.IdempotencyScopeEnvironment, ScopeID: environmentID,
		Method: method, Route: route, Key: key,
	}
	version, digest, err := idempotentintent.Canonicalize(ctx, idempotentintent.CanonicalIntentV1{
		Method: method, Route: route,
		Scope: idempotentintent.Scope{Kind: idempotentintent.ScopeEnvironment, ID: environmentID},
		Path:  []idempotentintent.PathBinding{{Name: "id", Value: targetID}},
		Query: idempotentintent.Object(), Body: body,
	})
	if err != nil {
		return locator, idempotentintent.ProtectedEvidence{}, etcd.ProtectedIntentRecord{}, nil, err
	}
	defer digest.Destroy()
	protected, err := service.coordinator.ProtectIntent(ctx, version, digest)
	if err != nil {
		return locator, idempotentintent.ProtectedEvidence{}, etcd.ProtectedIntentRecord{}, nil, err
	}
	durable, err := protected.DurableRecord()
	if err != nil {
		protected.Destroy()
		return locator, idempotentintent.ProtectedEvidence{}, etcd.ProtectedIntentRecord{}, nil, err
	}
	resolution, exists, err := service.coordinator.ResolveExisting(ctx, service.idempotency, locator, protected)
	if err != nil {
		clear(durable.Ciphertext)
		protected.Destroy()
		return locator, idempotentintent.ProtectedEvidence{}, etcd.ProtectedIntentRecord{}, nil, err
	}
	if exists {
		clear(durable.Ciphertext)
		protected.Destroy()
		if resolution.Kind != idempotentintent.ResolutionReplay {
			return locator, idempotentintent.ProtectedEvidence{}, etcd.ProtectedIntentRecord{}, nil,
				errs.New(errs.KindInternal, "release idempotency resolution is invalid")
		}
		response := cloneIdempotencyResponse(resolution.Response)
		return locator, idempotentintent.ProtectedEvidence{}, etcd.ProtectedIntentRecord{}, &response, nil
	}
	return locator, protected, durable, nil, nil
}

func (service *Service) deployCandidate(
	ctx context.Context,
	scope etcd.ReleasePlanningScope,
	planning etcd.ReleasePlanningService,
	requestedTag string,
	requestedStrategy string,
	requestedFailure domain.OnFailure,
) (releaseCandidateInput, error) {
	serving, hasServing, err := service.ledger.GetPlanningServingIntent(ctx, scope, planning)
	if err != nil {
		return releaseCandidateInput{}, err
	}
	tag := strings.TrimSpace(requestedTag)
	if tag != requestedTag {
		return releaseCandidateInput{}, errs.New(errs.KindValidationFailed, "release tag must not contain surrounding whitespace")
	}
	currentTag := ""
	if hasServing {
		currentTag = serving.Tag
	}
	image, tag, digest, err := releaseImageWithTag(
		planning.Service.Record.Desired.Image,
		tag,
		currentTag,
	)
	if err != nil {
		return releaseCandidateInput{}, err
	}
	strategy, err := releaseStrategy(requestedStrategy, planning.Service.Record.Desired.Strategy)
	if err != nil {
		return releaseCandidateInput{}, err
	}
	onFailure, err := releaseFailurePolicy(requestedFailure, planning.Service.Record.Desired.OnFailure)
	if err != nil {
		return releaseCandidateInput{}, err
	}
	return releaseCandidateInput{
		planning: planning, image: image, tag: tag, digest: digest,
		priorImage: releasePriorImage(planning, serving, hasServing), priorReleaseID: releasePriorID(serving, hasServing),
		strategy: strategy, priorStrategy: releasePriorStrategy(serving, hasServing),
		slot: inactiveReleaseSlot(strategy, planning.Projection.ServingSlot), onFailure: onFailure,
	}, nil
}

func (service *Service) rollbackCandidate(
	ctx context.Context,
	scope etcd.ReleasePlanningScope,
	planning etcd.ReleasePlanningService,
	explicitTag string,
	overrideFailure domain.OnFailure,
) (releaseCandidateInput, error) {
	selection, err := service.ledger.SelectRollback(
		ctx, scope.Environment.Record.ID, planning.Service.Record.Desired.ID, explicitTag, scope.ReadRevision,
	)
	if err != nil {
		return releaseCandidateInput{}, err
	}
	serving, hasServing, err := service.ledger.GetPlanningServingIntent(ctx, scope, planning)
	if err != nil {
		return releaseCandidateInput{}, err
	}
	onFailure := selection.Source.OnFailure
	if overrideFailure != "" {
		onFailure = overrideFailure
	}
	return releaseCandidateInput{
		planning: planning, image: selection.Source.Image, tag: selection.Source.Tag,
		digest: selection.Source.Digest, strategy: selection.Source.Strategy,
		priorImage: releasePriorImage(planning, serving, hasServing), priorReleaseID: releasePriorID(serving, hasServing),
		priorStrategy: releasePriorStrategy(serving, hasServing),
		slot:          inactiveReleaseSlot(selection.Source.Strategy, planning.Projection.ServingSlot),
		onFailure:     onFailure, rollbackSource: selection.Source.ID,
	}, nil
}
