package backingprovisionwork

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// ReadyForHost returns source/resource comparisons for admission and later
// forward grants. The initial epoch and this input are published in the SAME
// facade transaction; its actual revision is the captured desired-state fence.
// Updating runtime observations alone does not advance that desired epoch.
func (repository *Repository) ReadyForHost(ctx context.Context, view workqueue.View) (Input, workqueue.Change, error) {
	input, guard, err := repository.Load(ctx, view)
	if err != nil {
		return Input{}, workqueue.Change{}, err
	}
	keep := false
	defer func() {
		if !keep {
			clear(input.Wire)
		}
	}()
	procedure, err := input.Procedure()
	if err != nil {
		return Input{}, workqueue.Change{}, err
	}
	keys := []string{hierarchy.ProjectKey(procedure.ProjectId), hierarchy.EnvironmentKey(procedure.EnvironmentId),
		services.ServiceRuntimeKey(procedure.ServiceId), hierarchy.EnvironmentMutationEpochKey(procedure.EnvironmentId),
		blueprints.EnvironmentBlueprintHeadKey(procedure.EnvironmentId), hierarchy.EnvironmentOperationLockKey(procedure.EnvironmentId),
		deletions.TombstoneKey("project", procedure.ProjectId), deletions.TombstoneKey("environment", procedure.EnvironmentId),
		deletions.TombstoneKey("service", procedure.ServiceId), deletions.TombstoneKey("zone", procedure.Runtime.Networks[0].NetworkId)}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: view.ReadRevision})
	if err != nil {
		return Input{}, workqueue.Change{}, err
	}
	if read == nil || read.ReadRevision != view.ReadRevision || len(read.Values) != len(keys) {
		return Input{}, workqueue.Change{}, invalid("backing creation resource read is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	change := workqueue.Change{Conditions: []keyvalue.Condition{guard}}
	for index, key := range keys {
		value := read.Values[index]
		condition := keyvalue.Condition{Key: key}
		if value != nil {
			if value.Key != key || value.ModRevision <= 0 || value.ModRevision > view.ReadRevision {
				return Input{}, workqueue.Change{}, invalid("backing creation resource revision is invalid")
			}
			condition.ModRevision = value.ModRevision
		}
		if index < 5 && value == nil {
			return Input{}, workqueue.Change{}, invalid("backing creation lost an atomically published resource")
		}
		if index >= 5 && value != nil {
			return Input{}, workqueue.Change{}, invalid("backing creation is blocked by another resource operation or deletion")
		}
		change.Conditions = append(change.Conditions, condition)
	}
	project, err := hierarchy.DecodeProject(read.Values[0].Value)
	if err != nil {
		return Input{}, workqueue.Change{}, err
	}
	environment, err := hierarchy.DecodeEnvironment(read.Values[1].Value)
	if err != nil {
		return Input{}, workqueue.Change{}, err
	}
	service, err := services.DecodeServiceRuntimeRecord(read.Values[2].Value)
	if err != nil {
		return Input{}, workqueue.Change{}, err
	}
	epoch, err := backupruntime.DecodeEnvironmentMutationEpochRecord(read.Values[3].Value)
	if err != nil {
		return Input{}, workqueue.Change{}, err
	}
	head, err := idempotency.DecodeTaskReference(read.Values[4].Value)
	if err != nil {
		return Input{}, workqueue.Change{}, err
	}
	if project.ID != procedure.ProjectId || project.Kind != hierarchy.ProjectKindBacking || project.TenantID != "" || project.DeletionTaskID != "" ||
		environment.ID != procedure.EnvironmentId || environment.ProjectID != project.ID || environment.CreateTaskID != procedure.TaskId ||
		environment.VolumeDir != procedure.VolumeDirectory || environment.NetworkPool != input.Environment.NetworkPool || environment.DeletionTaskID != "" ||
		service.ServiceID != procedure.ServiceId || service.EnvironmentID != procedure.EnvironmentId ||
		service.BackingNetworkID != procedure.Runtime.Networks[0].NetworkId || epoch.EnvironmentID != procedure.EnvironmentId ||
		read.Values[3].ModRevision != guard.ModRevision || head != procedure.TaskId {
		return Input{}, workqueue.Change{}, invalid("backing creation source changed after acceptance; submit a new action instead of executing stale input")
	}
	keep = true
	return input, change, nil
}
