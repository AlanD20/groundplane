package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runtimeconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpins"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type MutationWorkConfiguration struct {
	Capture    runtimeconfiguration.WorkCapture
	Secrets    tasksecretpins.Prepared
	Conditions []keyvalue.Condition
}

// PrepareMutationWorkConfiguration captures files before the direct action is
// accepted. The sealed mutation claim, not a manufactured Task revision, fences
// Secret preparation. Returned conditions must join final publication.
func (repository *HierarchyRepository) PrepareMutationWorkConfiguration(ctx context.Context,
	task workexecution.Task, item workexecution.Item, claim blueprints.EnvironmentBlueprintStageClaim,
	readRevision int64, configurationID, operationID string, changes []taskmaterialization.Record,
) (MutationWorkConfiguration, error) {
	if ctx == nil || repository == nil || repository.store == nil || task.Validate() != nil || item.Validate() != nil ||
		blueprints.ValidateEnvironmentBlueprintStageClaim(claim) != nil || readRevision <= 0 ||
		!mutationConfigurationTarget(task, item, claim.EnvironmentID) || item.TaskID != task.ID ||
		task.Scope.EnvironmentID != claim.EnvironmentID || claim.TaskID != task.ID || claim.RevisionID != task.ID ||
		claim.SourceKind != blueprints.EnvironmentBlueprintSourceMutation || len(changes) == 0 ||
		task.Stop != nil || task.FinishedAt != nil || task.PublicationComplete || item.AttemptNumber != 0 ||
		item.ActiveAttemptID != "" || item.ForwardRevoked || item.Phase != workexecution.Waiting || item.Input != nil {
		return MutationWorkConfiguration{}, errs.New(errs.KindValidationFailed, "Mutation configuration requires its unaccepted sealed mutation")
	}
	keys := []string{blueprints.EnvironmentBlueprintDescriptorKeyByID(claim.DescriptorID),
		blueprints.EnvironmentBlueprintRootKey(claim.EnvironmentID, claim.RevisionID),
		blueprints.EnvironmentBlueprintHeadKey(claim.EnvironmentID)}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys})
	if err != nil {
		return MutationWorkConfiguration{}, err
	}
	if read == nil || read.ReadRevision < readRevision || len(read.Values) != len(keys) {
		return MutationWorkConfiguration{}, errs.New(errs.KindStateConflict, "Mutation configuration staging read is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	for index, value := range read.Values {
		if value == nil || value.Key != keys[index] || value.ModRevision <= 0 || value.ModRevision > read.ReadRevision {
			return MutationWorkConfiguration{}, errs.New(errs.KindStateConflict, "Mutation configuration staging owner is unavailable")
		}
	}
	descriptor, err := blueprints.DecodeEnvironmentBlueprintStageDescriptor(read.Values[0].Value)
	if err != nil {
		return MutationWorkConfiguration{}, err
	}
	seal, err := blueprints.DecodeEnvironmentBlueprintSeal(read.Values[1].Value)
	if err != nil || descriptor.State != blueprints.EnvironmentBlueprintStageSealed ||
		!blueprints.SameEnvironmentBlueprintStageClaim(descriptor.Claim, claim) ||
		seal != blueprints.EnvironmentBlueprintSealFromDescriptor(descriptor) || read.Values[2].ModRevision != claim.BaselineHeadRevision {
		return MutationWorkConfiguration{}, errs.New(errs.KindStateConflict, "Mutation configuration staging authority changed")
	}
	guards := []keyvalue.Condition{{Key: workqueue.TaskKey(task.ID)}, {Key: workqueue.ItemKey(item.ID)}}
	for index, key := range keys {
		guards = append(guards, keyvalue.Condition{Key: key, ModRevision: read.Values[index].ModRevision})
	}
	sources, err := runtimeconfiguration.New(repository.store)
	if err != nil {
		return MutationWorkConfiguration{}, err
	}
	capture, err := sources.CaptureWork(ctx, task, item, readRevision, configurationID, claim.RenderGeneration, changes)
	if err != nil {
		return MutationWorkConfiguration{}, err
	}
	pinGuards := make([]tasksecretpins.Condition, len(guards))
	for index, condition := range guards {
		pinGuards[index] = tasksecretpins.Condition{Key: condition.Key, ModRevision: condition.ModRevision}
	}
	view := workqueue.View{Task: workqueue.TaskSnapshot{Task: task}, Item: workqueue.ItemSnapshot{Item: item}, ReadRevision: readRevision}
	pins, err := prepareCapturedConfigurationPins(ctx, repository.store, view, operationID, capture, pinGuards)
	if err != nil {
		return MutationWorkConfiguration{}, err
	}
	return MutationWorkConfiguration{Capture: capture, Secrets: pins, Conditions: guards}, nil
}

func mutationConfigurationTarget(task workexecution.Task, item workexecution.Item, environmentID string) bool {
	switch item.Operation {
	case workexecution.EntryConfiguration:
		return item.TargetID == environmentID
	case workexecution.Component:
		if ids.Validate(ids.KindRoute, task.TargetID) != nil || ids.Validate(ids.KindComponent, item.TargetID) != nil ||
			task.Action != workexecution.ActionCreate && task.Action != workexecution.ActionUpdate && task.Action != workexecution.ActionRemove {
			return false
		}
		for _, claim := range item.Claims {
			if claim.HostID == "" && claim.ScopeID == task.TargetID {
				return true
			}
		}
	}
	return false
}
