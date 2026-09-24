package etcd

import (
	"bytes"
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PublishBlueprintDesiredPlan advances the current parent's durable unit plan.
// An incomplete plan may be expanded after a prerequisite hook returns facts;
// the parent claim and head are fenced with the plan and epoch in one write.
func (repository *TaskRepository) PublishBlueprintDesiredPlan(
	ctx context.Context, parentID string, desired blueprintunits.DesiredPlan,
) (keyvalue.Versioned[blueprintunits.DesiredPlan], error) {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return keyvalue.Versioned[blueprintunits.DesiredPlan]{}, err
	}
	if ids.Validate(ids.KindTask, parentID) != nil || desired.ParentTaskID != parentID ||
		ids.Validate(ids.KindEnvironment, desired.EnvironmentID) != nil {
		return keyvalue.Versioned[blueprintunits.DesiredPlan]{}, errs.New(
			errs.KindValidationFailed, "Blueprint desired plan identity is invalid",
		)
	}
	ledger, err := blueprintunits.NewRepository(repository.store)
	if err != nil {
		return keyvalue.Versioned[blueprintunits.DesiredPlan]{}, err
	}
	snapshot, err := ledger.Load(ctx, desired.EnvironmentID)
	if err != nil {
		return keyvalue.Versioned[blueprintunits.DesiredPlan]{}, err
	}
	if snapshot.HeadTaskID != parentID {
		return keyvalue.Versioned[blueprintunits.DesiredPlan]{}, errs.New(
			errs.KindStateConflict, "Blueprint parent is no longer current",
		)
	}
	parentKey := taskjournal.TaskStorageKey(parentID)
	claimKey := taskjournal.BlueprintParentClaimKey(parentID)
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{
		Keys: []string{parentKey, claimKey}, Revision: snapshot.ReadRevision,
	})
	if err != nil {
		return keyvalue.Versioned[blueprintunits.DesiredPlan]{}, err
	}
	if read == nil || len(read.Values) != 2 || read.Values[0] == nil || read.Values[1] == nil {
		return keyvalue.Versioned[blueprintunits.DesiredPlan]{}, errs.New(
			errs.KindStateConflict, "Blueprint parent claim is unavailable",
		)
	}
	defer keyvalue.ClearValues(read.Values)
	parent, err := DecodeTaskRecord(read.Values[0].Value)
	claimID, claimErr := idempotency.DecodeTaskReference(read.Values[1].Value)
	if err != nil || claimErr != nil || claimID != parentID ||
		validateBlueprintParentClaimTask(parent) != nil ||
		parent.Status != taskjournal.TaskStatusRunning ||
		parent.Owner.EnvironmentID != desired.EnvironmentID {
		return keyvalue.Versioned[blueprintunits.DesiredPlan]{}, errs.New(
			errs.KindStateConflict, "Blueprint parent claim changed",
		)
	}
	encoded, err := blueprintunits.EncodeDesiredPlan(desired)
	if err != nil {
		return keyvalue.Versioned[blueprintunits.DesiredPlan]{}, err
	}
	defer clear(encoded)
	if snapshot.Desired != nil {
		current, encodeErr := blueprintunits.EncodeDesiredPlan(snapshot.Desired.Record)
		if encodeErr != nil {
			return keyvalue.Versioned[blueprintunits.DesiredPlan]{}, encodeErr
		}
		matched := bytes.Equal(current, encoded)
		clear(current)
		if matched {
			return *snapshot.Desired, nil
		}
	}
	plan, err := blueprintunits.PrepareDesiredPlan(snapshot, desired)
	if err != nil {
		return keyvalue.Versioned[blueprintunits.DesiredPlan]{}, err
	}
	defer plan.Clear()
	conditions := append(plan.Conditions(),
		keyvalue.Condition{Key: parentKey, ModRevision: read.Values[0].ModRevision},
		keyvalue.Condition{Key: claimKey, ModRevision: read.Values[1].ModRevision},
	)
	result, err := repository.store.Transact(ctx, conditions, plan.Mutations())
	keyvalue.ClearValues(result.FailureReads)
	if err != nil {
		return keyvalue.Versioned[blueprintunits.DesiredPlan]{}, err
	}
	if !result.Succeeded {
		return keyvalue.Versioned[blueprintunits.DesiredPlan]{}, errs.New(
			errs.KindStateConflict, "Blueprint desired plan publication raced",
		)
	}
	return keyvalue.Versioned[blueprintunits.DesiredPlan]{
		Record: desired, Revision: result.Revision, ReadRevision: result.Revision,
	}, nil
}
