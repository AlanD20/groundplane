// Package gitreleasebridge connects verified Git delivery to native image and release Tasks.
package gitreleasebridge

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/imagefetch"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/gitdelivery"
	"github.com/AlanD20/groundplane/internal/controller/imagedelivery"
	"github.com/AlanD20/groundplane/internal/controller/releaseoperation"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentqueries"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hostbindings"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type Service struct {
	images   *imagedelivery.Service
	releases *releaseoperation.Service
	queue    *workqueue.Repository
	ledger   *etcd.ReleaseLedger
	store    hostbindings.Reader
}

var _ gitdelivery.Releases = (*Service)(nil)

func New(images *imagedelivery.Service, releases *releaseoperation.Service, queue *workqueue.Repository,
	ledger *etcd.ReleaseLedger, store hostbindings.Reader,
) *Service {
	return &Service{images: images, releases: releases, queue: queue, ledger: ledger, store: store}
}

func (service *Service) Fetch(ctx context.Context, input gitdelivery.FetchInput) (string, error) {
	image, err := verifiedImage(input.Target, input.RequestID, input.ImageRepository, input.Digest)
	if err != nil {
		return "", err
	}
	_, guards, err := service.scopeConditions(ctx, input.Target)
	if err != nil {
		return "", err
	}
	response, err := service.images.FetchServiceImageGuarded(
		ctx,
		input.Target.ServiceID,
		image,
		"git.fetch."+input.RequestID,
		nil,
		append(guards, input.Authority...),
		targetScope(input.Target),
	)
	if err != nil {
		return "", err
	}
	return acceptedTaskID(response)
}

func (service *Service) Deploy(ctx context.Context, input gitdelivery.DeployInput) (string, error) {
	request, err := deploymentRequest(input)
	if err != nil {
		return "", err
	}
	if input.Fence.Key == "" || input.Fence.Prefix || input.Fence.ModRevision <= 0 {
		return "", errs.New(errs.KindValidationFailed, "Git deployment requires a captured activation fence")
	}
	fetch, revision, err := service.queue.ReadTask(ctx, input.FetchTaskID, 0)
	if err != nil {
		return "", err
	}
	if err := service.validateFetch(ctx, fetch.Task, revision, input.Target, request.Image); err != nil {
		return "", err
	}
	if taskState(fetch.Task) != gitdelivery.TaskSucceeded || fetch.Task.CollectionStartedAt != nil {
		return "", errs.New(errs.KindStateConflict, "Git deployment requires its successful image Fetch Task")
	}
	_, guards, err := service.scopeConditions(ctx, input.Target)
	if err != nil {
		return "", err
	}
	guards = append(guards, input.Fence, keyvalue.Condition{
		Key: workqueue.TaskKey(fetch.Task.ID), ModRevision: fetch.Revision,
	})
	guards = append(guards, input.Authority...)
	response, err := service.releases.DeployServiceGuarded(
		ctx,
		input.Target.ServiceID,
		request,
		"git.deploy."+input.RequestID,
		guards,
	)
	if err != nil {
		return "", err
	}
	return acceptedTaskID(response)
}

// DeployReceipt reads the accepted native Deploy without admitting work. An
// absent receipt lets Git delivery drop its activation fence in one transaction.
func (service *Service) DeployReceipt(
	ctx context.Context,
	input gitdelivery.DeployInput,
) (string, keyvalue.Condition, error) {
	request, err := deploymentRequest(input)
	if err != nil {
		return "", keyvalue.Condition{}, err
	}
	response, absent, err := service.releases.ReadDeployServiceReceipt(ctx, input.Target.EnvironmentID,
		input.Target.ServiceID, request, "git.deploy."+input.RequestID)
	if err != nil || response == nil {
		return "", absent, err
	}
	taskID, err := acceptedTaskID(*response)
	return taskID, keyvalue.Condition{}, err
}

func deploymentRequest(input gitdelivery.DeployInput) (release.ServiceDeployInput, error) {
	image, err := verifiedImage(input.Target, input.RequestID, input.ImageRepository, input.Digest)
	if err != nil {
		return release.ServiceDeployInput{}, err
	}
	if ids.Validate(ids.KindTask, input.FetchTaskID) != nil {
		return release.ServiceDeployInput{}, errs.New(
			errs.KindValidationFailed,
			"Git deployment requires its captured Fetch Task",
		)
	}
	strategy, err := release.DeploymentStrategy(string(input.Policy.Strategy), "")
	if err != nil || strategy != input.Policy.Strategy ||
		(input.Policy.OnFailure != release.OnFailureSwitchBack && input.Policy.OnFailure != release.OnFailureLeaveActive) {
		return release.ServiceDeployInput{}, errs.New(
			errs.KindValidationFailed,
			"Git deployment requires its captured rollout policy",
		)
	}
	return release.ServiceDeployInput{
		Image:     image,
		After:     []string{input.FetchTaskID},
		Strategy:  input.Policy.Strategy,
		OnFailure: input.Policy.OnFailure,
	}, nil
}

func (service *Service) Policy(
	ctx context.Context,
	target gitdelivery.Target,
) (gitdelivery.RolloutPolicy, []keyvalue.Condition, error) {
	desired, guards, err := service.scopeConditions(ctx, target)
	if err != nil {
		return gitdelivery.RolloutPolicy{}, nil, err
	}
	strategy, err := release.DeploymentStrategy("", string(desired.Strategy))
	if err != nil {
		return gitdelivery.RolloutPolicy{}, nil, err
	}
	return gitdelivery.RolloutPolicy{
		Strategy:  strategy,
		OnFailure: release.OnFailure(desired.OnFailure.WithDefault()),
	}, guards, nil
}

func (service *Service) scopeConditions(
	ctx context.Context,
	target gitdelivery.Target,
) (core.Service, []keyvalue.Condition, error) {
	scope, err := service.ledger.LoadPlanningScope(ctx, target.EnvironmentID)
	if err != nil {
		return core.Service{}, nil, err
	}
	if scope.Tenant.Record.ID != target.TenantID || scope.Project.Record.ID != target.ProjectID ||
		scope.Environment.Record.ID != target.EnvironmentID {
		return core.Service{}, nil, errs.New(
			errs.KindStateConflict,
			"Git deployment target no longer has its captured ancestry",
		)
	}
	for _, desired := range environmentqueries.OrdinaryServices(scope.Compose.Record) {
		if desired.Desired.ID == target.ServiceID {
			return desired.Desired, []keyvalue.Condition{
				{Key: hierarchy.TenantKey(target.TenantID), ModRevision: scope.Tenant.Revision},
				{Key: hierarchy.ProjectKey(target.ProjectID), ModRevision: scope.Project.Revision},
				{Key: hierarchy.EnvironmentKey(target.EnvironmentID), ModRevision: scope.Environment.Revision},
				{
					Key:         blueprints.EnvironmentBlueprintHeadKey(target.EnvironmentID),
					ModRevision: scope.Compose.Revision,
				},
				{
					Key:         hierarchy.EnvironmentMutationEpochKey(target.EnvironmentID),
					ModRevision: scope.EnvironmentEpochRevision,
				},
				{Key: deletions.TombstoneKey("tenant", target.TenantID)},
				{Key: deletions.TombstoneKey("project", target.ProjectID)},
				{Key: deletions.TombstoneKey("environment", target.EnvironmentID)},
				{Key: deletions.TombstoneKey("service", target.ServiceID)},
			}, nil
		}
	}
	return core.Service{}, nil, errs.New(
		errs.KindServiceNotFound,
		"Git deployment Service was not found in the captured Environment",
	)
}

func verifiedImage(target gitdelivery.Target, requestID, repository, digest string) (string, error) {
	for _, field := range []struct {
		kind  ids.Kind
		value string
	}{
		{ids.KindTenant, target.TenantID},
		{ids.KindProject, target.ProjectID},
		{ids.KindEnvironment, target.EnvironmentID},
		{ids.KindService, target.ServiceID},
		{ids.KindOperation, requestID},
	} {
		if ids.Validate(field.kind, field.value) != nil {
			return "", errs.New(errs.KindValidationFailed, "Git deployment target or request identity is invalid")
		}
	}
	if repository != gitdelivery.ImageRepository(target, requestID) {
		return "", errs.New(
			errs.KindValidationFailed,
			"Git image repository does not belong to the captured deployment",
		)
	}
	if !strings.HasPrefix(digest, "sha256:") {
		return "", errs.New(errs.KindValidationFailed, "Git deployment requires an immutable image digest")
	}
	image := repository + "@" + digest
	if _, err := imagefetch.ParseRequested(image); err != nil {
		return "", err
	}
	return image, nil
}

func acceptedTaskID(response idempotency.IdempotencyResponse) (string, error) {
	var accepted api.TaskAccepted
	if response.Status != http.StatusAccepted || json.Unmarshal(response.Body, &accepted) != nil ||
		ids.Validate(
			ids.KindTask,
			accepted.TaskID,
		) != nil || !idempotency.ValidTaskResponse(response, accepted.TaskID) {
		return "", errs.New(errs.KindInternal, "Native Git delivery Task acceptance is invalid")
	}
	return accepted.TaskID, nil
}

func targetScope(target gitdelivery.Target) workexecution.Scope {
	return workexecution.Scope{
		TenantID:      target.TenantID,
		ProjectID:     target.ProjectID,
		EnvironmentID: target.EnvironmentID,
	}
}
