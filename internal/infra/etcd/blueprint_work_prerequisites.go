package etcd

import (
	"bytes"
	"context"
	"encoding/json"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// BlueprintWorkPrerequisiteConditions observes prerequisites at the same fixed
// snapshot as Release preparation. Its comparisons must be committed together
// with the prepared input; this method never publishes or executes work.
func (ledger *ReleaseLedger) BlueprintWorkPrerequisiteConditions(ctx context.Context,
	work workqueue.View, requirements core.BlueprintRequirements,
) ([]keyvalue.Condition, error) {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return nil, err
	}
	task, item := work.Task.Task, work.Item.Item
	if ledger == nil || ledger.store == nil || task.Validate() != nil || item.Validate() != nil ||
		task.Action != workexecution.ActionApply || item.TaskID != task.ID || item.Operation != workexecution.Release ||
		work.Task.Revision <= 0 || work.Item.Revision <= 0 || work.ReadRevision < work.Task.Revision ||
		work.ReadRevision < work.Item.Revision || requirements.Validate() != nil {
		return nil, errs.New(errs.KindValidationFailed, "Blueprint prerequisites require captured Release work")
	}
	key := blueprintPrerequisiteAuthorityKey(task.Scope.EnvironmentID, task.ID)
	value, err := ledger.blueprintWorkPrerequisiteValue(ctx, key, work.ReadRevision)
	if err != nil {
		return nil, err
	}
	defer clear(value.Value)
	authority, err := decodeBlueprintPrerequisiteAuthority(value.Value)
	if err != nil {
		return nil, err
	}
	want, err := json.Marshal(requirements)
	if err != nil {
		return nil, err
	}
	got, err := json.Marshal(authority.Requirements)
	if err != nil {
		return nil, err
	}
	if authority.EnvironmentID != task.Scope.EnvironmentID || authority.RevisionID != task.ID ||
		!bytes.Equal(want, got) {
		return nil, errs.New(
			errs.KindStateConflict,
			"Blueprint prerequisite resolution differs from the captured runtime input",
		)
	}
	conditions := []keyvalue.Condition{{Key: key, ModRevision: value.ModRevision}}
	dependencies := make([]workqueue.ItemSnapshot, len(item.Dependencies))
	for index, id := range item.Dependencies {
		value, err := ledger.blueprintWorkPrerequisiteValue(ctx, workqueue.ItemKey(id), work.ReadRevision)
		if err != nil {
			return nil, err
		}
		dependency, decodeErr := workqueue.DecodeItem(value.Value)
		clear(value.Value)
		if decodeErr != nil {
			return nil, decodeErr
		}
		dependencies[index] = workqueue.ItemSnapshot{Item: dependency, Revision: value.ModRevision}
	}
	dependencyConditions, err := workqueue.DependencyConditions(item, dependencies)
	if err != nil {
		return nil, err
	}
	conditions = append(conditions, dependencyConditions...)
	for _, requirement := range requirements.Resolved {
		if !slices.Contains(requirement.Phases, core.RequirementPhaseDeploy) &&
			!slices.Contains(requirement.Phases, core.RequirementPhaseAlways) {
			continue
		}
		key := attachments.AttachKey(requirement.Target.ID)
		value, err := ledger.blueprintWorkPrerequisiteValue(ctx, key, work.ReadRevision)
		if err != nil {
			return nil, err
		}
		attach, decodeErr := attachments.DecodeAttachRecord(value.Value)
		clear(value.Value)
		if decodeErr != nil {
			return nil, decodeErr
		}
		if attach.ID != requirement.Target.ID || attach.TaskID != requirement.Target.TaskID ||
			attach.EnvironmentID != task.Scope.EnvironmentID || value.ModRevision < requirement.Target.Revision {
			return nil, errs.New(errs.KindStateConflict, "Blueprint prerequisite Attach was replaced")
		}
		conditions = append(conditions, keyvalue.Condition{Key: key, ModRevision: value.ModRevision})
		if requirement.Condition == core.RequirementReady && attach.Status != core.AttachReady {
			return nil, errs.New(errs.KindStateConflict, "Blueprint prerequisite Attach is not ready")
		}
		if requirement.Condition != core.RequirementCompletedSuccessfully {
			continue
		}
		if attach.TaskID == task.ID {
			// Waiting for this Apply to complete would deadlock its own Release.
			// The captured graph must instead include the successful Attach item.
			found := slices.ContainsFunc(dependencies, func(dependency workqueue.ItemSnapshot) bool {
				return dependency.Item.Operation == workexecution.Attach && dependency.Item.TargetID == attach.ID
			})
			if !found {
				return nil, errs.New(
					errs.KindStateConflict,
					"Blueprint prerequisite Attach has no completed dependency work",
				)
			}
			continue
		}
		key = workqueue.TaskKey(attach.TaskID)
		value, err = ledger.blueprintWorkPrerequisiteValue(ctx, key, work.ReadRevision)
		if err != nil {
			return nil, err
		}
		producer, decodeErr := workqueue.DecodeTask(value.Value)
		clear(value.Value)
		if decodeErr != nil {
			return nil, decodeErr
		}
		if producer.ID != attach.TaskID || producer.FinishedAt == nil || producer.Outcome != workexecution.Succeeded {
			return nil, errs.New(errs.KindStateConflict, "Blueprint prerequisite Attach producer has not succeeded")
		}
		conditions = append(conditions, keyvalue.Condition{Key: key, ModRevision: value.ModRevision})
	}
	return conditions, nil
}

func (ledger *ReleaseLedger) blueprintWorkPrerequisiteValue(ctx context.Context, key string,
	revision int64,
) (*keyvalue.KeyValue, error) {
	read, err := ledger.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: revision})
	if err != nil {
		return nil, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 || read.Values[0] == nil ||
		read.Values[0].Key != key || read.Values[0].ModRevision <= 0 || read.Values[0].ModRevision > revision {
		if read != nil {
			keyvalue.ClearValues(read.Values)
		}
		return nil, errs.New(
			errs.KindStateConflict,
			"Blueprint prerequisite evidence is unavailable at the work snapshot",
		)
	}
	return read.Values[0], nil
}
