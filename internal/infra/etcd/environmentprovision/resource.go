package environmentprovision

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Provisioning and Retry consume the same Environment epoch and lock as other
// resource mutations. They cannot race hierarchy deletion or Backup operations.
func (repository *Repository) resourceSnapshot(
	ctx context.Context,
	input Input,
	revision int64,
) (hierarchy.EnvironmentRecord, workqueue.Change, error) {
	keys := []string{
		hierarchy.EnvironmentKey(input.Environment.ID),
		hierarchy.EnvironmentMutationEpochKey(input.Environment.ID),
		hierarchy.EnvironmentOperationLockKey(
			input.Environment.ID,
		),
		deletions.TombstoneKey("environment", input.Scope.EnvironmentID),
		deletions.TombstoneKey(
			"project",
			input.Scope.ProjectID,
		),
		deletions.TombstoneKey("tenant", input.Scope.TenantID),
		hierarchy.ProjectKey(input.Scope.ProjectID),
		hierarchy.TenantKey(input.Scope.TenantID),
	}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return hierarchy.EnvironmentRecord{}, workqueue.Change{}, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != len(keys) ||
		read.Values[0] == nil || read.Values[1] == nil || read.Values[6] == nil || read.Values[7] == nil {
		return hierarchy.EnvironmentRecord{}, workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"environment provisioning resource or hierarchy is unavailable",
		)
	}
	defer keyvalue.ClearValues(read.Values)
	for _, index := range []int{2, 3, 4, 5} {
		if read.Values[index] != nil {
			return hierarchy.EnvironmentRecord{}, workqueue.Change{}, errs.New(
				errs.KindResourceInUse,
				"environment provisioning is blocked by a resource operation or hierarchy deletion",
			)
		}
	}
	current, err := hierarchy.DecodeEnvironment(read.Values[0].Value)
	if err != nil {
		return hierarchy.EnvironmentRecord{}, workqueue.Change{}, err
	}
	project, err := hierarchy.DecodeProject(read.Values[6].Value)
	if err != nil {
		return hierarchy.EnvironmentRecord{}, workqueue.Change{}, err
	}
	tenant, err := hierarchy.DecodeTenant(read.Values[7].Value)
	if err != nil {
		return hierarchy.EnvironmentRecord{}, workqueue.Change{}, err
	}
	if current.ID != input.Environment.ID || current.ProjectID != input.Scope.ProjectID ||
		current.VolumeDir != input.Environment.VolumeDir ||
		current.CreateTaskID != input.TaskID ||
		current.DeletionTaskID != "" ||
		project.ID != input.Scope.ProjectID ||
		project.Kind != hierarchy.ProjectKindTenant ||
		project.TenantID != input.Scope.TenantID ||
		project.DeletionTaskID != "" ||
		tenant.ID != input.Scope.TenantID ||
		tenant.DeletionTaskID != "" {
		return hierarchy.EnvironmentRecord{}, workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"environment provisioning resource ownership changed",
		)
	}
	epoch, err := backupruntime.DecodeEnvironmentMutationEpochRecord(read.Values[1].Value)
	if err != nil || epoch.EnvironmentID != current.ID {
		return hierarchy.EnvironmentRecord{}, workqueue.Change{}, errs.New(
			errs.KindInternal,
			"environment provisioning mutation epoch is corrupt",
		)
	}
	raw, err := backupruntime.EncodeEnvironmentMutationEpochRecord(epoch)
	if err != nil {
		return hierarchy.EnvironmentRecord{}, workqueue.Change{}, err
	}
	change := workqueue.Change{Mutations: []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: keys[1], Value: raw}}}
	for index, key := range keys {
		condition := keyvalue.Condition{Key: key}
		if read.Values[index] != nil {
			condition.ModRevision = read.Values[index].ModRevision
		}
		change.Conditions = append(change.Conditions, condition)
	}
	return current, change, nil
}
