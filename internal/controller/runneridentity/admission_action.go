package runneridentity

import (
	"context"
	"encoding/json"
	"net/http"
	"slices"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/imagefetch"
	"github.com/AlanD20/groundplane/internal/common/imageref"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/core/runneraccess"
	"github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runnergrants"
	"github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (admission *ScopedAdmission) resolveAction(
	ctx context.Context, principal Principal, request Request, grant CapturedGrant, revision int64,
) (runneraccess.Request, []keyvalue.Condition, scopedAction, error) {
	target, conditions, err := admission.resolveWorkspace(ctx, grant.Policy.Target, revision)
	if err != nil {
		return runneraccess.Request{}, nil, nil, err
	}
	if request.Operation == runneraccess.ReadTask {
		resolved, task, err := admission.resolveTask(ctx, target, request.ResourceID, revision)
		if err != nil {
			return runneraccess.Request{}, nil, nil, err
		}
		conditions = append(
			conditions,
			keyvalue.Condition{Key: workqueue.TaskKey(task.Task.ID), ModRevision: task.Revision},
		)
		policy := runneraccess.Request{Operation: request.Operation, Target: resolved}
		if grant.Task.Revision > 0 {
			if !grant.Task.Record.MatchesTask(task.Task) {
				return runneraccess.Request{}, nil, nil, forbidden()
			}
			service, err := admission.resolveService(
				ctx,
				target.EnvironmentID,
				grant.Task.Record.Authority.ServiceID,
				revision,
			)
			if err != nil {
				return runneraccess.Request{}, nil, nil, err
			}
			target.Kind, target.ID = ids.KindService, service.Record.Desired.ID
			policy.Related = []runneraccess.Resource{target}
			conditions = append(conditions, serviceGuards(service)...)
			conditions = append(
				conditions,
				keyvalue.Condition{Key: runnergrants.TaskProvenanceKey(task.Task.ID), ModRevision: grant.Task.Revision},
			)
		}
		return policy, conditions,
			readAction(taskDetails(task.Task, revision)), nil
	}
	service, err := admission.resolveService(ctx, target.EnvironmentID, request.ResourceID, revision)
	if err != nil {
		return runneraccess.Request{}, nil, nil, err
	}
	target.Kind, target.ID = ids.KindService, service.Record.Desired.ID
	conditions = append(conditions, serviceGuards(service)...)
	policy := runneraccess.Request{Operation: request.Operation, Target: target}
	if request.Operation == runneraccess.ReadService {
		detail := api.ServiceDetail{Service: api.Service{
			ID: target.ID, EnvironmentID: target.EnvironmentID, Name: service.Record.Desired.Name,
			Image: service.Record.Desired.Image, Strategy: string(service.Record.Desired.Strategy),
			RuntimeIntent: api.ServiceRuntimeIntent(service.Record.Runtime.RuntimeIntent),
		}}
		return policy, conditions, readAction(detail), nil
	}

	after, apply := admission.mutation(principal, target, request, grant)
	if apply == nil || grant.DesiredRevision <= 0 || service.Revision != grant.DesiredRevision || !validTaskIDs(after) {
		return runneraccess.Request{}, nil, nil, forbidden()
	}
	for _, taskID := range after {
		related, task, err := admission.resolveTask(ctx, target, taskID, revision)
		if err != nil {
			return runneraccess.Request{}, nil, nil, err
		}
		policy.Related = append(policy.Related, related)
		conditions = append(conditions, keyvalue.Condition{Key: workqueue.TaskKey(taskID), ModRevision: task.Revision})
	}
	return policy, conditions, apply, nil
}

func (admission *ScopedAdmission) mutation(
	principal Principal, target runneraccess.Resource, request Request, grant CapturedGrant,
) ([]string, scopedAction) {
	if !approvedImage(grant.Image) {
		return nil, nil
	}
	scope := workexecution.Scope{
		TenantID:      target.TenantID,
		ProjectID:     target.ProjectID,
		EnvironmentID: target.EnvironmentID,
	}
	provenance := runnergrants.TaskProvenance{
		RunnerID: principal.Credential.RunnerID, RunnerEpoch: principal.Credential.RunnerEpoch,
		GrantRevision: grant.Condition.ModRevision, CredentialRevision: principal.Credential.Revision,
		ExpiresAt: grant.ExpiresAt, TokenDigest: runnergrants.TokenDigest(principal.Digest),
		Scope: scope, ServiceID: target.ID, Image: grant.Image,
	}
	switch request.Operation {
	case runneraccess.DeployService:
		input := request.Deploy
		if input == nil || input.Image != grant.Image || input.Tag != "" || input.Strategy != "" ||
			input.OnFailure != "" {
			return nil, nil
		}
		provenance.Action = workexecution.ActionDeploy
		desired := release.ServiceDeployInput{Image: grant.Image, After: slices.Clone(input.After)}
		return desired.After, func(ctx context.Context, guards []keyvalue.Condition) (Result, error) {
			result, err := admission.releases.DeployServiceForRunnerGuarded(
				ctx,
				target.ID,
				desired,
				request.IdempotencyKey,
				guards,
				provenance,
			)
			return Result{Status: result.Status, Body: result.Body}, err
		}
	case runneraccess.FetchServiceImage:
		input := request.Fetch
		if input == nil || input.Image != grant.Image {
			return nil, nil
		}
		provenance.Action = workexecution.ActionFetch
		after := slices.Clone(input.After)
		return after, func(ctx context.Context, guards []keyvalue.Condition) (Result, error) {
			result, err := admission.images.FetchServiceImageForRunnerGuarded(
				ctx,
				target.ID,
				grant.Image,
				request.IdempotencyKey,
				after,
				guards,
				scope,
				provenance,
			)
			return Result{Status: result.Status, Body: result.Body}, err
		}
	default:
		return nil, nil
	}
}

func approvedImage(image string) bool {
	if !strings.HasPrefix(image, imagefetch.RegistryAuthority+"/") || !imageref.IsDigestPinned(image) {
		return false
	}
	_, err := imagefetch.ParseRequested(image)
	return err == nil
}

func serviceGuards(service keyvalue.Versioned[services.ServiceRecord]) []keyvalue.Condition {
	return []keyvalue.Condition{
		services.ServiceDesiredCondition(service),
		{Key: deletions.TombstoneKey(string(deletions.DeletionTargetService), service.Record.Desired.ID)},
	}
}

func readAction[T api.ServiceDetail | api.TaskDetails](projection T) scopedAction {
	return func(_ context.Context, _ []keyvalue.Condition) (Result, error) {
		body, err := json.Marshal(projection)
		if err != nil {
			return Result{}, errs.Wrap(errs.KindInternal, err)
		}
		return Result{Status: http.StatusOK, Body: body}, nil
	}
}

// Only the status projection is visible through the Runner listener. Work
// plans, child targets and diagnostics can hold details outside its grant.
func taskDetails(task workexecution.Task, revision int64) api.TaskDetails {
	status := "queued"
	switch {
	case task.FinishedAt != nil:
		status = string(task.Outcome)
	case task.Progress.Attention > 0:
		status = "attention_required"
	case task.Stop != nil:
		status = "stopping"
	case task.Progress.Running > 0 || task.Progress.Assigned > 0:
		status = "running"
	case task.Progress.Waiting > 0:
		status = "waiting"
	case !task.PublicationComplete:
		status = "preparing"
	case task.Progress.Unsettled() == 0:
		status = "finishing"
	}
	return api.TaskDetails{Task: api.TaskSummary{
		ID: task.ID, Action: task.Action, Scope: task.Scope, Actor: task.Actor, Status: status,
		Counts: api.TaskCounts(task.Progress), CreatedAt: task.CreatedAt, UpdatedAt: task.UpdatedAt,
		Deadline: task.Deadline, FinishedAt: task.FinishedAt, StopRequested: task.Stop != nil,
		PendingPreparation: task.PendingPreparation(),
	}, Work: api.ExecutionPage[api.TaskWork]{Items: []api.TaskWork{}, Revision: revision}}
}
