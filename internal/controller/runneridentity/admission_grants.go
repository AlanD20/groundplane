package runneridentity

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/core/runner"
	"github.com/AlanD20/groundplane/internal/core/runneraccess"
	"github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runnercredential"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runnergrants"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runners"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runnerscoped"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type StoredGrantReader struct{ grants *runnergrants.Repository }

func NewStoredGrantReader(grants *runnergrants.Repository) *StoredGrantReader {
	return &StoredGrantReader{grants: grants}
}

func (reader *StoredGrantReader) ReadRunnerGrant(
	ctx context.Context, principal Principal, operation runneraccess.Operation, targetID string,
) (CapturedGrant, error) {
	current, err := reader.grants.Get(ctx, principal.Credential.RunnerID)
	if err != nil {
		return CapturedGrant{}, err
	}
	record := current.Record
	if current.Revision <= 0 || record.Revoked || !principal.Credential.EpochBound ||
		record.RunnerEpoch != principal.Credential.RunnerEpoch || !record.ExpiresAt.After(time.Now()) ||
		record.TokenDigest != runnergrants.TokenDigest(principal.Digest) {
		return CapturedGrant{}, forbidden()
	}
	for _, stored := range record.Actions {
		if stored.Operation != string(operation) || stored.TargetID != targetID {
			continue
		}
		if !validGrantAction(
			GrantAction{Operation: operation, TargetID: targetID, TaskIDs: stored.TaskIDs, Image: stored.Image},
		) {
			return CapturedGrant{}, errs.New(errs.KindInternal, "Runner CLI grant action is corrupt")
		}
		kind := ids.KindService
		if operation == runneraccess.ReadTask {
			kind = ids.KindTask
		}
		policy := runneraccess.Grant{Operation: operation, Target: grantResource(record.Scope, kind, targetID)}
		for _, taskID := range stored.TaskIDs {
			policy.Related = append(policy.Related, grantResource(record.Scope, ids.KindTask, taskID))
		}
		return CapturedGrant{
			Policy: policy, Image: stored.Image, DesiredRevision: stored.DesiredRevision,
			Condition: keyvalue.Condition{
				Key:         runnergrants.Key(record.RunnerID),
				ModRevision: current.Revision,
			}, ExpiresAt: record.ExpiresAt,
		}, nil
	}
	if operation == runneraccess.ReadTask {
		return reader.readAcceptedTaskGrant(ctx, principal, current, targetID)
	}
	return CapturedGrant{}, forbidden()
}

func (reader *StoredGrantReader) readAcceptedTaskGrant(
	ctx context.Context, principal Principal, current keyvalue.Versioned[runnergrants.Record], taskID string,
) (CapturedGrant, error) {
	provenance, err := reader.grants.ReadTaskProvenance(ctx, taskID, current.ReadRevision)
	if err != nil {
		return CapturedGrant{}, err
	}
	grant, authority := current.Record, provenance.Record.Authority
	if provenance.Revision <= 0 || authority.RunnerID != grant.RunnerID ||
		authority.RunnerEpoch != grant.RunnerEpoch || authority.GrantRevision != current.Revision ||
		authority.CredentialRevision != principal.Credential.Revision || authority.TokenDigest != grant.TokenDigest ||
		authority.Scope != grant.Scope || !authority.ExpiresAt.Equal(grant.ExpiresAt) {
		return CapturedGrant{}, forbidden()
	}
	operation := runneraccess.FetchServiceImage
	if authority.Action == workexecution.ActionDeploy {
		operation = runneraccess.DeployService
	}
	for _, action := range grant.Actions {
		if action.Operation != string(operation) || action.TargetID != authority.ServiceID ||
			action.Image != authority.Image {
			continue
		}
		return CapturedGrant{
			Policy: runneraccess.Grant{
				Operation: runneraccess.ReadTask, Target: grantResource(grant.Scope, ids.KindTask, taskID),
				Related: []runneraccess.Resource{grantResource(grant.Scope, ids.KindService, authority.ServiceID)},
			},
			Condition: keyvalue.Condition{Key: runnergrants.Key(grant.RunnerID), ModRevision: current.Revision},
			Task:      provenance,
			ExpiresAt: grant.ExpiresAt,
		}, nil
	}
	return CapturedGrant{}, forbidden()
}

func grantResource(scope workexecution.Scope, kind ids.Kind, id string) runneraccess.Resource {
	return runneraccess.Resource{
		Kind: kind, ID: id, TenantID: scope.TenantID, ProjectID: scope.ProjectID, EnvironmentID: scope.EnvironmentID,
	}
}

type GrantAction struct {
	Operation runneraccess.Operation
	TargetID  string
	TaskIDs   []string
	Image     string
}

type GrantIssue struct {
	RunnerID string
	Scope    workexecution.Scope
	Actions  []GrantAction
	Lifetime time.Duration
}

type GrantManager struct {
	admission   *ScopedAdmission
	grants      *runnergrants.Repository
	credentials *runnercredential.Repository
	migrations  *runnerscoped.Repository
}

func NewGrantManager(
	admission *ScopedAdmission, grants *runnergrants.Repository, credentials *runnercredential.Repository,
	migrations *runnerscoped.Repository,
) *GrantManager {
	return &GrantManager{admission: admission, grants: grants, credentials: credentials, migrations: migrations}
}

func (manager *GrantManager) prepare(
	ctx context.Context, issue GrantIssue, now time.Time,
) (runnergrants.Record, []keyvalue.Condition, error) {
	if len(issue.Actions) == 0 || len(issue.Actions) > runnergrants.MaxActions ||
		issue.Lifetime <= 0 || issue.Lifetime > runnercredential.MaximumLifetime {
		return runnergrants.Record{}, nil, errs.New(
			errs.KindValidationFailed,
			"Runner CLI grant lifetime or action count is invalid",
		)
	}
	admission := manager.admission
	current, err := admission.runners.GetRunner(ctx, issue.RunnerID)
	if err != nil {
		return runnergrants.Record{}, nil, err
	}
	if current.Record.ProvisioningState != runners.RunnerProvisioningReady || current.Record.RuntimeEpoch == 0 {
		return runnergrants.Record{}, nil, errs.New(errs.KindStateConflict, "Runner registration is not ready")
	}
	workspace, guards, err := admission.resolveWorkspace(ctx, grantResource(issue.Scope, "", ""), current.ReadRevision)
	if err != nil {
		return runnergrants.Record{}, nil, err
	}
	registration := runneraccess.Registration{
		RunnerID: current.Record.Desired.ID, TenantID: current.Record.Desired.TenantID,
		OwnerKind: runner.OwnerKind(current.Record.Desired.OwnerKind), OwnerID: current.Record.Desired.OwnerID,
	}
	guards = append(
		guards,
		keyvalue.Condition{Key: runners.RunnerKey(issue.RunnerID), ModRevision: current.Revision},
		keyvalue.Condition{
			Key:         runners.RunnerLifecycleKey(issue.RunnerID),
			ModRevision: current.Record.LifecycleRevision,
		},
		keyvalue.Condition{Key: deletions.TombstoneKey(string(deletions.DeletionTargetRunner), issue.RunnerID)},
	)
	actions, guards, err := admission.captureGrantActions(
		ctx,
		issue.Actions,
		workspace,
		registration,
		guards,
		current.ReadRevision,
	)
	if err != nil {
		return runnergrants.Record{}, nil, err
	}
	active, err := manager.migrations.Authority(ctx, issue.RunnerID)
	if err != nil {
		return runnergrants.Record{}, nil, err
	}
	digest, binding, credentialConditions, err := manager.credentials.ReadActiveBaseCredential(
		ctx,
		issue.RunnerID,
		current.Record.RuntimeEpoch,
	)
	if err != nil {
		return runnergrants.Record{}, nil, err
	}
	if binding.Revision <= 0 || !binding.EpochBound {
		return runnergrants.Record{}, nil, errs.New(errs.KindStateConflict, "Runner has no active scoped credential")
	}
	for _, captured := range [][]keyvalue.Condition{active.Conditions, credentialConditions} {
		guards, err = mergeGuards(guards, captured)
		if err != nil {
			return runnergrants.Record{}, nil, err
		}
	}
	return runnergrants.Record{
		RunnerID: issue.RunnerID, RunnerEpoch: binding.RunnerEpoch, TokenDigest: runnergrants.TokenDigest(digest),
		ExpiresAt: now.Add(issue.Lifetime), Scope: issue.Scope, Actions: actions,
	}, guards, nil
}

func (admission *ScopedAdmission) captureGrantActions(
	ctx context.Context, requested []GrantAction, workspace runneraccess.Resource,
	registration runneraccess.Registration, guards []keyvalue.Condition, revision int64,
) ([]runnergrants.Action, []keyvalue.Condition, error) {
	actions := make([]runnergrants.Action, 0, len(requested))
	type identity struct {
		op runneraccess.Operation
		id string
	}
	seen := make(map[identity]bool, len(requested))
	for _, input := range requested {
		key := identity{op: input.Operation, id: input.TargetID}
		if seen[key] || !validGrantAction(input) {
			return nil, nil, errs.New(errs.KindValidationFailed, "Runner CLI grant action is invalid or duplicated")
		}
		seen[key] = true
		action, conditions, policy, err := admission.captureGrantAction(ctx, input, workspace, revision)
		if err != nil {
			return nil, nil, err
		}
		ownGrant := runneraccess.Grant(policy)
		if err := runneraccess.Authorize(registration, []runneraccess.Grant{ownGrant}, policy); err != nil {
			return nil, nil, err
		}
		guards, err = mergeGuards(guards, conditions)
		if err != nil {
			return nil, nil, err
		}
		actions = append(actions, action)
	}
	return actions, guards, nil
}

func (admission *ScopedAdmission) captureGrantAction(
	ctx context.Context, input GrantAction, target runneraccess.Resource, revision int64,
) (runnergrants.Action, []keyvalue.Condition, runneraccess.Request, error) {
	action := runnergrants.Action{
		Operation: string(input.Operation),
		TargetID:  input.TargetID,
		Image:     input.Image,
		TaskIDs:   input.TaskIDs,
	}
	var conditions []keyvalue.Condition
	policy := runneraccess.Request{Operation: input.Operation}
	if input.Operation == runneraccess.ReadTask {
		resource, task, err := admission.resolveTask(ctx, target, input.TargetID, revision)
		if err != nil {
			return action, nil, policy, err
		}
		policy.Target = resource
		conditions = append(
			conditions,
			keyvalue.Condition{Key: workqueue.TaskKey(task.Task.ID), ModRevision: task.Revision},
		)
		return action, conditions, policy, nil
	}
	service, err := admission.resolveService(ctx, target.EnvironmentID, input.TargetID, revision)
	if err != nil {
		return action, nil, policy, err
	}
	target.Kind, target.ID = ids.KindService, service.Record.Desired.ID
	policy.Target = target
	action.DesiredRevision = service.Revision
	conditions = append(conditions, serviceGuards(service)...)
	for _, id := range input.TaskIDs {
		related, task, err := admission.resolveTask(ctx, target, id, revision)
		if err != nil {
			return action, nil, policy, err
		}
		policy.Related = append(policy.Related, related)
		conditions = append(conditions, keyvalue.Condition{Key: workqueue.TaskKey(id), ModRevision: task.Revision})
	}
	return action, conditions, policy, nil
}

func validGrantAction(input GrantAction) bool {
	switch input.Operation {
	case runneraccess.ReadTask:
		return ids.Validate(ids.KindTask, input.TargetID) == nil && input.Image == "" && len(input.TaskIDs) == 0
	case runneraccess.ReadService:
		return ids.Validate(ids.KindService, input.TargetID) == nil && input.Image == "" && len(input.TaskIDs) == 0
	case runneraccess.FetchServiceImage, runneraccess.DeployService:
		return ids.Validate(ids.KindService, input.TargetID) == nil && approvedImage(input.Image) &&
			validTaskIDs(input.TaskIDs)
	default:
		return false
	}
}
