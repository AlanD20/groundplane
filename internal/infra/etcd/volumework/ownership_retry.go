package volumework

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	sourceref "github.com/AlanD20/groundplane/internal/infra/scriptsourcereference"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// A completed action released resource ownership. Reopening it must not race
// parent deletion or a newly captured Script, even if the desired head matches.
func (repository *InputRepository) prepareRemovalRetryGuards(
	ctx context.Context,
	view workqueue.View,
) ([]keyvalue.Condition, []keyvalue.Mutation, error) {
	scope := view.Task.Task.Scope
	keys := []string{hierarchy.EnvironmentKey(scope.EnvironmentID), hierarchy.ProjectKey(scope.ProjectID),
		hierarchy.TenantKey(scope.TenantID)}
	read, err := repository.read(ctx, keys, view.ReadRevision)
	if err != nil {
		return nil, nil, err
	}
	defer keyvalue.ClearValues(read.Values)
	for _, value := range read.Values {
		if value == nil {
			return nil, nil, errs.New(errs.KindTaskNotRetryable, "Volume Retry parent was removed")
		}
	}
	environment, environmentErr := hierarchy.DecodeEnvironment(read.Values[0].Value)
	project, projectErr := hierarchy.DecodeProject(read.Values[1].Value)
	tenant, tenantErr := hierarchy.DecodeTenant(read.Values[2].Value)
	if environmentErr != nil || projectErr != nil || tenantErr != nil ||
		environment.ID != scope.EnvironmentID || environment.ProjectID != scope.ProjectID ||
		project.ID != scope.ProjectID || project.TenantID != scope.TenantID || tenant.ID != scope.TenantID {
		return nil, nil, conflict("Volume Retry parent ownership is inconsistent")
	}
	if environment.DeletionTaskID != "" || project.DeletionTaskID != "" || tenant.DeletionTaskID != "" {
		return nil, nil, errs.New(errs.KindTaskNotRetryable, "Volume Retry parent removal has started")
	}
	guards := make([]keyvalue.Condition, 0, len(keys)+2)
	for index, key := range keys {
		guards = append(guards, keyvalue.Condition{Key: key, ModRevision: read.Values[index].ModRevision})
	}
	source := sourceref.SourceIdentity{Kind: sourceref.SourceVolume, VolumeID: view.Item.Item.TargetID}
	prefix := sourceref.ForwardReferencePrefix + sourceref.SourceSuffix(source) + "/"
	count, err := repository.read(ctx, []string{sourceref.CountKey(source)}, view.ReadRevision)
	if err != nil {
		return nil, nil, err
	}
	defer keyvalue.ClearValues(count.Values)
	members, err := repository.store.Range(
		ctx,
		keyvalue.RangeRequest{Prefix: prefix, Limit: 1, Revision: view.ReadRevision},
	)
	if err != nil {
		return nil, nil, err
	}
	if members == nil || members.ReadRevision != view.ReadRevision || len(members.Values) > 1 ||
		members.More && len(members.Values) == 0 {
		return nil, nil, conflict("Volume Retry Script reference read is incomplete")
	}
	defer keyvalue.ClearRangeValues(members.Values)
	if count.Values[0] != nil || len(members.Values) != 0 {
		return nil, nil, errs.New(errs.KindTaskNotRetryable, "Volume is now referenced by a Script")
	}
	guards = append(
		guards,
		keyvalue.Condition{Key: sourceref.CountKey(source)},
		keyvalue.Condition{Key: prefix, Prefix: true},
	)
	ancestry, err := hierarchydeletion.BindMutationEpochs(ctx, repository.store, view.ReadRevision,
		hierarchydeletion.MutationScope{TenantID: scope.TenantID, ProjectID: scope.ProjectID}, guards, nil)
	if err != nil {
		return nil, nil, err
	}
	// The returned mutation bytes transfer to the enclosing Retry preparation.
	return ancestry.Conditions(), ancestry.Mutations(), nil
}
