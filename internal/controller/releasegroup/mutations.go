package releasegroup

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"slices"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	domain "github.com/AlanD20/groundplane/internal/core/releasegroup"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	etcdreleasegroup "github.com/AlanD20/groundplane/internal/infra/etcd/releasegroup"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	releaseGroupAddRoute      = "/release-groups"
	releaseGroupEditRoute     = "/release-groups/{id}"
	releaseGroupRemoveRoute   = "/release-groups/{id}"
	releaseGroupTaskTimeout   = int64(30)
	releaseGroupMutationTries = 3
)

type MutationService struct {
	groups      *etcdreleasegroup.Store
	hierarchy   *etcd.HierarchyRepository
	tasks       *etcd.TaskRepository
	idempotency *etcd.IdempotencyRepository
	coordinator *requestidempotency.Coordinator
	now         func() time.Time
}

func NewMutationService(
	groups *etcdreleasegroup.Store,
	hierarchy *etcd.HierarchyRepository,
	tasks *etcd.TaskRepository,
	idempotency *etcd.IdempotencyRepository,
	coordinator *requestidempotency.Coordinator,
) (*MutationService, error) {
	if groups == nil || hierarchy == nil || tasks == nil || idempotency == nil || coordinator == nil {
		return nil, errs.New(errs.KindInternal, "release group mutation dependencies are not configured")
	}
	return &MutationService{
		groups: groups, hierarchy: hierarchy, tasks: tasks, idempotency: idempotency,
		coordinator: coordinator, now: time.Now,
	}, nil
}

func (service *MutationService) AddReleaseGroup(
	ctx context.Context, request apiTypes.ReleaseGroupAddRequest, key string,
) (etcd.IdempotencyResponse, error) {
	group, err := domain.New(domain.Input{
		ID: ids.New(ids.KindReleaseGroup), EnvironmentID: request.EnvironmentID, Name: request.Name,
		ServiceIDs: request.ServiceIDs, Order: request.Order, DefaultTag: request.Tag,
		OnFailure: domain.OnFailure(request.OnFailure),
	})
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	body := requestidempotency.JSONBody(releaseGroupAddIntent(request))
	return service.apply(ctx, key, http.MethodPost, releaseGroupAddRoute, group.EnvironmentID, nil, group, body)
}

func (service *MutationService) EditReleaseGroup(
	ctx context.Context, groupID string, request apiTypes.ReleaseGroupEditRequest, key string,
) (etcd.IdempotencyResponse, error) {
	if ids.Validate(ids.KindReleaseGroup, groupID) != nil {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindValidationFailed, "release group id is invalid")
	}
	present := request.Name != nil || request.ServiceIDs != nil || request.Order != nil || request.Tag.Present ||
		request.OnFailure != nil
	if !present {
		return etcd.IdempotencyResponse{}, errs.New(
			errs.KindValidationFailed,
			"release group edit requires at least one field",
		)
	}
	body := requestidempotency.JSONBody(releaseGroupEditIntent(request))
	target := etcd.IdempotencyReplayTarget{Kind: etcd.IdempotencyReplayTargetReleaseGroup, ID: groupID}
	locator, indexed, err := service.idempotency.ResolveReplayLocator(
		ctx,
		target,
		http.MethodPatch,
		releaseGroupEditRoute,
		key,
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if indexed {
		return service.replay(ctx, locator, groupID, body)
	}
	current, err := service.groups.Get(ctx, groupID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	desired := domain.Desired{
		Name: current.Group.Name, ServiceIDs: slices.Clone(current.Group.ServiceIDs), Order: slices.Clone(current.Group.Order),
		DefaultTag: current.Group.DefaultTag, OnFailure: current.Group.OnFailure,
	}
	if request.Name != nil {
		desired.Name = *request.Name
	}
	if request.ServiceIDs != nil {
		desired.ServiceIDs = slices.Clone(*request.ServiceIDs)
		if request.Order == nil {
			desired.Order = slices.Clone(*request.ServiceIDs)
		}
	}
	if request.Order != nil {
		desired.Order = slices.Clone(*request.Order)
	}
	if request.Tag.Present {
		if request.Tag.Value == nil {
			desired.DefaultTag = ""
		} else {
			desired.DefaultTag = *request.Tag.Value
		}
	}
	if request.OnFailure != nil {
		desired.OnFailure = domain.OnFailure(*request.OnFailure)
	}
	replacement, err := domain.Replace(current.Group, desired)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	return service.apply(
		ctx,
		key,
		http.MethodPatch,
		releaseGroupEditRoute,
		current.Group.EnvironmentID,
		&current,
		replacement,
		body,
	)
}

func (service *MutationService) RemoveReleaseGroup(
	ctx context.Context, groupID string, key string,
) (etcd.IdempotencyResponse, error) {
	if ids.Validate(ids.KindReleaseGroup, groupID) != nil {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindValidationFailed, "release group id is invalid")
	}
	target := etcd.IdempotencyReplayTarget{Kind: etcd.IdempotencyReplayTargetReleaseGroup, ID: groupID}
	locator, indexed, err := service.idempotency.ResolveReplayLocator(
		ctx,
		target,
		http.MethodDelete,
		releaseGroupRemoveRoute,
		key,
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if indexed {
		return service.replay(ctx, locator, groupID, requestidempotency.NoBody())
	}
	current, err := service.groups.Get(ctx, groupID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	return service.apply(
		ctx,
		key,
		http.MethodDelete,
		releaseGroupRemoveRoute,
		current.Group.EnvironmentID,
		&current,
		current.Group,
		requestidempotency.NoBody(),
	)
}

func (service *MutationService) apply(
	ctx context.Context, key, method, route, environmentID string,
	current *etcdreleasegroup.Versioned, desired domain.Group, body requestidempotency.Body,
) (etcd.IdempotencyResponse, error) {
	if ctx == nil {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "release group mutation context is required")
	}
	path := []requestidempotency.PathBinding(nil)
	if method != http.MethodPost {
		path = []requestidempotency.PathBinding{{Name: "id", Value: desired.ID}}
	}
	version, digest, err := requestidempotency.Canonicalize(ctx, requestidempotency.CanonicalIntentV1{
		Method: method, Route: route, Scope: requestidempotency.Scope{Kind: requestidempotency.ScopeEnvironment, ID: environmentID},
		Path: path, Query: requestidempotency.Object(), Body: body,
	})
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	defer digest.Destroy()
	candidate, err := service.coordinator.ProtectIntent(ctx, version, digest)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	durable, err := candidate.DurableRecord()
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	defer clear(durable.Ciphertext)
	locator := etcd.IdempotencyLocator{
		ScopeKind: etcd.IdempotencyScopeEnvironment,
		ScopeID:   environmentID,
		Method:    method,
		Route:     route,
		Key:       key,
	}
	resolution, existing, err := service.coordinator.ResolveExisting(ctx, service.idempotency, locator, candidate)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if existing {
		return releaseGroupReplayResponse(resolution)
	}
	environment, err := service.hierarchy.GetEnvironment(ctx, environmentID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	project, err := service.hierarchy.GetProject(ctx, environment.Record.ProjectID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	var prepared etcd.ReleaseGroupPreparedMutation
	if current == nil {
		prepared, err = service.tasks.PrepareReleaseGroupCreate(ctx, desired)
	} else if method == http.MethodPatch {
		prepared, err = service.tasks.PrepareReleaseGroupUpdate(ctx, current.Group, current.Revision, desired)
	} else {
		prepared, err = service.tasks.PrepareReleaseGroupRemove(ctx, current.Group, current.Revision)
	}
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if method != http.MethodDelete {
		status := http.StatusCreated
		if method == http.MethodPatch {
			status = http.StatusOK
		}
		responseBody, err := json.Marshal(releaseGroupAPIResponse(desired))
		if err != nil {
			return etcd.IdempotencyResponse{}, errs.Wrap(errs.KindInternal, err)
		}
		response := etcd.IdempotencyResponse{Status: status, ContentKind: "application/json", Body: responseBody}
		marker, err := etcd.NewCompletedDirectIdempotencyMarker(locator, durable, response, service.now().UTC())
		if err != nil {
			return etcd.IdempotencyResponse{}, err
		}
		replayTarget := etcd.IdempotencyReplayTarget{Kind: etcd.IdempotencyReplayTargetReleaseGroup, ID: desired.ID}
		marker.ReplayTarget = &replayTarget
		result, mutationErr := service.tasks.PublishReleaseGroupDirectMutation(ctx, prepared, marker)
		if mutationErr != nil {
			if !releaseGroupUnknownOutcome(mutationErr) {
				return etcd.IdempotencyResponse{}, mutationErr
			}
			resolution, err = service.coordinator.ResolveUnknown(
				ctx,
				service.idempotency,
				locator,
				candidate,
				mutationErr,
			)
		} else {
			resolution, err = service.coordinator.ResolveKnown(ctx, candidate, result)
		}
		if err != nil {
			return etcd.IdempotencyResponse{}, err
		}
		if resolution.Kind == requestidempotency.ResolutionApplied {
			return requestidempotency.CloneResponse(response), nil
		}
		return releaseGroupReplayResponse(resolution)
	}
	owner, err := etcd.EnvironmentTaskOwner(project.Record, environment.Record)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	taskType := etcd.TaskCreate
	if method == http.MethodPatch {
		taskType = etcd.TaskUpdate
	}
	if method == http.MethodDelete {
		taskType = etcd.TaskRemove
	}
	now := service.now().UTC()
	task := etcd.TaskRecord{
		ID: ids.New(ids.KindTask), OperationID: ids.New(ids.KindOperation), IdempotencyKey: key,
		Owner: owner, Actor: etcd.TaskActorOperator, Executor: etcd.TaskExecutorController,
		PlanID: ids.New(ids.KindPlan), RenderGeneration: 1, Type: taskType, Target: desired.ID,
		Params: map[string]string{etcd.TaskResourceKindParam: etcd.TaskResourceReleaseGroup},
		Steps: []etcd.TaskStepRecord{
			{Kind: etcd.TaskStepOperation, ID: ids.New(ids.KindStep)},
		}, TimeoutSeconds: releaseGroupTaskTimeout,
		Status: etcd.TaskStatusPending, NextEventSequence: 1, CreatedAt: now, UpdatedAt: now,
	}
	task.PlanHash, err = releaseGroupMutationPlanHash(taskType, desired)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	responseBody, err := json.Marshal(
		apiTypes.ReleaseGroupMutationAccepted{TaskID: task.ID, ReleaseGroupID: desired.ID},
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, errs.Wrap(errs.KindInternal, err)
	}
	response := etcd.IdempotencyResponse{
		Status:      http.StatusAccepted,
		ContentKind: "application/json",
		Body:        responseBody,
	}
	target := etcd.IdempotencyReplayTarget{Kind: etcd.IdempotencyReplayTargetReleaseGroup, ID: desired.ID}
	marker := etcd.IdempotencyMarker{
		Kind: etcd.IdempotencyMarkerTask, State: etcd.IdempotencyMarkerPending, Locator: locator,
		ReplayTarget: &target, Intent: durable, Response: response, TaskID: task.ID, CreatedAt: now, UpdatedAt: now,
	}
	result, mutationErr := service.tasks.PublishReleaseGroupMutation(ctx, environment, project, prepared, task, marker)
	if mutationErr != nil {
		if !releaseGroupUnknownOutcome(mutationErr) {
			return etcd.IdempotencyResponse{}, mutationErr
		}
		resolution, err = service.coordinator.ResolveUnknown(ctx, service.idempotency, locator, candidate, mutationErr)
	} else {
		resolution, err = service.coordinator.ResolveKnown(ctx, candidate, result)
	}
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if resolution.Kind == requestidempotency.ResolutionApplied {
		return requestidempotency.CloneResponse(response), nil
	}
	return releaseGroupReplayResponse(resolution)
}

func releaseGroupAPIResponse(group domain.Group) apiTypes.ReleaseGroup {
	return apiTypes.ReleaseGroup{
		ID: group.ID, EnvironmentID: group.EnvironmentID, Name: group.Name,
		ServiceIDs: slices.Clone(group.ServiceIDs), Order: slices.Clone(group.Order),
		Tag: group.DefaultTag, OnFailure: apiTypes.OnFailure(group.OnFailure),
	}
}

func (service *MutationService) replay(
	ctx context.Context, locator etcd.IdempotencyLocator, groupID string, body requestidempotency.Body,
) (etcd.IdempotencyResponse, error) {
	version, digest, err := requestidempotency.Canonicalize(ctx, requestidempotency.CanonicalIntentV1{
		Method: locator.Method, Route: locator.Route, Scope: requestidempotency.Scope{Kind: requestidempotency.ScopeEnvironment, ID: locator.ScopeID},
		Path: []requestidempotency.PathBinding{
			{Name: "id", Value: groupID},
		}, Query: requestidempotency.Object(), Body: body,
	})
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	defer digest.Destroy()
	candidate, err := service.coordinator.ProtectIntent(ctx, version, digest)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	resolution, existing, err := service.coordinator.ResolveExisting(ctx, service.idempotency, locator, candidate)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if !existing {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "release group replay locator is inconsistent")
	}
	return releaseGroupReplayResponse(resolution)
}

func releaseGroupReplayResponse(resolution requestidempotency.Resolution) (etcd.IdempotencyResponse, error) {
	if resolution.Kind != requestidempotency.ResolutionReplay {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "release group replay resolution is invalid")
	}
	return requestidempotency.CloneResponse(resolution.Response), nil
}

func releaseGroupMutationPlanHash(kind etcd.TaskType, group domain.Group) (string, error) {
	value, err := json.Marshal(struct {
		Version int           `json:"version"`
		Type    etcd.TaskType `json:"type"`
		Group   domain.Group  `json:"group"`
	}{1, kind, group})
	if err != nil {
		return "", errs.Wrap(errs.KindInternal, err)
	}
	digest := sha256.Sum256(value)
	return hex.EncodeToString(digest[:]), nil
}

func releaseGroupUnknownOutcome(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	kind, ok := errs.KindOf(err)
	return ok && kind == errs.KindStorageUnavailable
}

func releaseGroupStrings(values []string) requestidempotency.Value {
	items := make([]requestidempotency.Value, len(values))
	for index, value := range values {
		items[index] = requestidempotency.String(value)
	}
	return requestidempotency.List(items...)
}

func releaseGroupAddIntent(request apiTypes.ReleaseGroupAddRequest) requestidempotency.Value {
	return requestidempotency.Object(
		requestidempotency.Field{Name: "environment_id", Value: requestidempotency.String(request.EnvironmentID)},
		requestidempotency.Field{Name: "name", Value: requestidempotency.String(request.Name)},
		requestidempotency.Field{Name: "service_ids", Value: releaseGroupStrings(request.ServiceIDs)},
		requestidempotency.Field{Name: "order", Value: releaseGroupStrings(request.Order)},
		requestidempotency.Field{Name: "tag", Value: requestidempotency.String(request.Tag)},
		requestidempotency.Field{Name: "on_failure", Value: requestidempotency.String(string(request.OnFailure))},
	)
}

func releaseGroupEditIntent(request apiTypes.ReleaseGroupEditRequest) requestidempotency.Value {
	fields := []requestidempotency.Field{}
	if request.Name != nil {
		fields = append(fields, requestidempotency.Field{Name: "name", Value: requestidempotency.String(*request.Name)})
	}
	if request.ServiceIDs != nil {
		fields = append(
			fields,
			requestidempotency.Field{Name: "service_ids", Value: releaseGroupStrings(*request.ServiceIDs)},
		)
	}
	if request.Order != nil {
		fields = append(fields, requestidempotency.Field{Name: "order", Value: releaseGroupStrings(*request.Order)})
	}
	if request.Tag.Present {
		value := requestidempotency.Null()
		if request.Tag.Value != nil {
			value = requestidempotency.String(*request.Tag.Value)
		}
		fields = append(fields, requestidempotency.Field{Name: "tag", Value: value})
	}
	if request.OnFailure != nil {
		fields = append(
			fields,
			requestidempotency.Field{Name: "on_failure", Value: requestidempotency.String(string(*request.OnFailure))},
		)
	}
	return requestidempotency.Object(fields...)
}
