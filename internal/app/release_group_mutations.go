package app

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
	"github.com/AlanD20/groundplane/internal/controller/idempotentintent"
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

type releaseGroupMutationService struct {
	groups      *etcdreleasegroup.Store
	hierarchy   *etcd.HierarchyRepository
	tasks       *etcd.TaskRepository
	idempotency *etcd.IdempotencyRepository
	coordinator *idempotentintent.Coordinator
	now         func() time.Time
}

func newReleaseGroupMutationService(
	groups *etcdreleasegroup.Store,
	hierarchy *etcd.HierarchyRepository,
	tasks *etcd.TaskRepository,
	idempotency *etcd.IdempotencyRepository,
	coordinator *idempotentintent.Coordinator,
) (*releaseGroupMutationService, error) {
	if groups == nil || hierarchy == nil || tasks == nil || idempotency == nil || coordinator == nil {
		return nil, errs.New(errs.KindInternal, "release group mutation dependencies are not configured")
	}
	return &releaseGroupMutationService{
		groups: groups, hierarchy: hierarchy, tasks: tasks, idempotency: idempotency,
		coordinator: coordinator, now: time.Now,
	}, nil
}

func (service *releaseGroupMutationService) AddReleaseGroup(
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
	body := idempotentintent.JSONBody(releaseGroupAddIntent(request))
	return service.apply(ctx, key, http.MethodPost, releaseGroupAddRoute, group.EnvironmentID, nil, group, body)
}

func (service *releaseGroupMutationService) EditReleaseGroup(
	ctx context.Context, groupID string, request apiTypes.ReleaseGroupEditRequest, key string,
) (etcd.IdempotencyResponse, error) {
	if ids.Validate(ids.KindReleaseGroup, groupID) != nil {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindValidationFailed, "release group id is invalid")
	}
	present := request.Name != nil || request.ServiceIDs != nil || request.Order != nil || request.Tag.Present || request.OnFailure != nil
	if !present {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindValidationFailed, "release group edit requires at least one field")
	}
	body := idempotentintent.JSONBody(releaseGroupEditIntent(request))
	target := etcd.IdempotencyReplayTarget{Kind: etcd.IdempotencyReplayTargetReleaseGroup, ID: groupID}
	locator, indexed, err := service.idempotency.ResolveReplayLocator(ctx, target, http.MethodPatch, releaseGroupEditRoute, key)
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
	return service.apply(ctx, key, http.MethodPatch, releaseGroupEditRoute, current.Group.EnvironmentID, &current, replacement, body)
}

func (service *releaseGroupMutationService) RemoveReleaseGroup(
	ctx context.Context, groupID string, key string,
) (etcd.IdempotencyResponse, error) {
	if ids.Validate(ids.KindReleaseGroup, groupID) != nil {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindValidationFailed, "release group id is invalid")
	}
	target := etcd.IdempotencyReplayTarget{Kind: etcd.IdempotencyReplayTargetReleaseGroup, ID: groupID}
	locator, indexed, err := service.idempotency.ResolveReplayLocator(ctx, target, http.MethodDelete, releaseGroupRemoveRoute, key)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if indexed {
		return service.replay(ctx, locator, groupID, idempotentintent.NoBody())
	}
	current, err := service.groups.Get(ctx, groupID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	return service.apply(ctx, key, http.MethodDelete, releaseGroupRemoveRoute, current.Group.EnvironmentID, &current, current.Group, idempotentintent.NoBody())
}

func (service *releaseGroupMutationService) apply(
	ctx context.Context, key, method, route, environmentID string,
	current *etcdreleasegroup.Versioned, desired domain.Group, body idempotentintent.Body,
) (etcd.IdempotencyResponse, error) {
	if ctx == nil {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "release group mutation context is required")
	}
	path := []idempotentintent.PathBinding(nil)
	if method != http.MethodPost {
		path = []idempotentintent.PathBinding{{Name: "id", Value: desired.ID}}
	}
	version, digest, err := idempotentintent.Canonicalize(ctx, idempotentintent.CanonicalIntentV1{
		Method: method, Route: route, Scope: idempotentintent.Scope{Kind: idempotentintent.ScopeEnvironment, ID: environmentID},
		Path: path, Query: idempotentintent.Object(), Body: body,
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
	locator := etcd.IdempotencyLocator{ScopeKind: etcd.IdempotencyScopeEnvironment, ScopeID: environmentID, Method: method, Route: route, Key: key}
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
		prepared, err = service.groups.PrepareCreate(ctx, desired)
	} else if method == http.MethodPatch {
		prepared, err = service.groups.PrepareUpdate(ctx, *current, desired)
	} else {
		prepared, err = service.groups.PrepareRemove(ctx, *current)
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
			resolution, err = service.coordinator.ResolveUnknown(ctx, service.idempotency, locator, candidate, mutationErr)
		} else {
			resolution, err = service.coordinator.ResolveKnown(ctx, candidate, result)
		}
		if err != nil {
			return etcd.IdempotencyResponse{}, err
		}
		if resolution.Kind == idempotentintent.ResolutionApplied {
			return cloneIdempotencyResponse(response), nil
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
		Steps:  []etcd.TaskStepRecord{{ID: ids.New(ids.KindStep)}}, TimeoutSeconds: releaseGroupTaskTimeout,
		Status: etcd.TaskStatusPending, NextEventSequence: 1, CreatedAt: now, UpdatedAt: now,
	}
	task.PlanHash, err = releaseGroupMutationPlanHash(taskType, desired)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	responseBody, err := json.Marshal(apiTypes.ReleaseGroupMutationAccepted{TaskID: task.ID, ReleaseGroupID: desired.ID})
	if err != nil {
		return etcd.IdempotencyResponse{}, errs.Wrap(errs.KindInternal, err)
	}
	response := etcd.IdempotencyResponse{Status: http.StatusAccepted, ContentKind: "application/json", Body: responseBody}
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
	if resolution.Kind == idempotentintent.ResolutionApplied {
		return cloneIdempotencyResponse(response), nil
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

func (service *releaseGroupMutationService) replay(
	ctx context.Context, locator etcd.IdempotencyLocator, groupID string, body idempotentintent.Body,
) (etcd.IdempotencyResponse, error) {
	version, digest, err := idempotentintent.Canonicalize(ctx, idempotentintent.CanonicalIntentV1{
		Method: locator.Method, Route: locator.Route, Scope: idempotentintent.Scope{Kind: idempotentintent.ScopeEnvironment, ID: locator.ScopeID},
		Path: []idempotentintent.PathBinding{{Name: "id", Value: groupID}}, Query: idempotentintent.Object(), Body: body,
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

func releaseGroupReplayResponse(resolution idempotentintent.Resolution) (etcd.IdempotencyResponse, error) {
	if resolution.Kind != idempotentintent.ResolutionReplay {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "release group replay resolution is invalid")
	}
	return cloneIdempotencyResponse(resolution.Response), nil
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

func releaseGroupStrings(values []string) idempotentintent.Value {
	items := make([]idempotentintent.Value, len(values))
	for index, value := range values {
		items[index] = idempotentintent.String(value)
	}
	return idempotentintent.List(items...)
}

func releaseGroupAddIntent(request apiTypes.ReleaseGroupAddRequest) idempotentintent.Value {
	return idempotentintent.Object(
		idempotentintent.Field{Name: "environment_id", Value: idempotentintent.String(request.EnvironmentID)},
		idempotentintent.Field{Name: "name", Value: idempotentintent.String(request.Name)},
		idempotentintent.Field{Name: "service_ids", Value: releaseGroupStrings(request.ServiceIDs)},
		idempotentintent.Field{Name: "order", Value: releaseGroupStrings(request.Order)},
		idempotentintent.Field{Name: "tag", Value: idempotentintent.String(request.Tag)},
		idempotentintent.Field{Name: "on_failure", Value: idempotentintent.String(string(request.OnFailure))},
	)
}

func releaseGroupEditIntent(request apiTypes.ReleaseGroupEditRequest) idempotentintent.Value {
	fields := []idempotentintent.Field{}
	if request.Name != nil {
		fields = append(fields, idempotentintent.Field{Name: "name", Value: idempotentintent.String(*request.Name)})
	}
	if request.ServiceIDs != nil {
		fields = append(fields, idempotentintent.Field{Name: "service_ids", Value: releaseGroupStrings(*request.ServiceIDs)})
	}
	if request.Order != nil {
		fields = append(fields, idempotentintent.Field{Name: "order", Value: releaseGroupStrings(*request.Order)})
	}
	if request.Tag.Present {
		value := idempotentintent.Null()
		if request.Tag.Value != nil {
			value = idempotentintent.String(*request.Tag.Value)
		}
		fields = append(fields, idempotentintent.Field{Name: "tag", Value: value})
	}
	if request.OnFailure != nil {
		fields = append(fields, idempotentintent.Field{Name: "on_failure", Value: idempotentintent.String(string(*request.OnFailure))})
	}
	return idempotentintent.Object(fields...)
}
