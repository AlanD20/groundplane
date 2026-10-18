package connectorchecks

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/s3connector"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PrepareUnstarted closes captured input and releases only this check's Secret
// memberships. The caller MUST join PrepareUnstartedSettlement in the same
// transaction: neither publication nor a racing assignment can see half cleanup.
func (repository *Repository) PrepareUnstarted(ctx context.Context, task workqueue.TaskSnapshot,
	item workqueue.ItemSnapshot, outcome workexecution.Outcome, at time.Time,
) (workqueue.Change, workexecution.Completion, error) {
	invalid := func() (workqueue.Change, workexecution.Completion, error) {
		return workqueue.Change{}, workexecution.Completion{}, errs.New(errs.KindStateConflict, "connector check cannot settle without execution")
	}
	if task.Revision <= 0 || item.Revision <= 0 || task.Task.ID != item.Item.TaskID ||
		item.Item.Operation != workexecution.ConnectorCheck || item.Item.AttemptNumber != 0 || item.Item.ActiveAttemptID != "" ||
		item.Item.Input == nil || item.Item.Phase != workexecution.Ready && item.Item.Phase != workexecution.Waiting {
		return invalid()
	}
	if outcome == workexecution.Failed {
		if at.Before(item.Item.Deadline) {
			return invalid()
		}
	} else if task.Task.Stop == nil || outcome != task.Task.Stop.Outcome {
		return invalid()
	}
	loaded, err := repository.Load(ctx, item.Item.TaskID)
	if err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	record := loaded.Record
	defer record.Clear()
	digest, err := PlanHash(record)
	if err != nil || record.Phase != Ready || record.Owner.WorkItemID != item.Item.ID ||
		record.Owner.PlanID != item.Item.Input.ID || digest != item.Item.Input.SHA256 || record.Connector.Connector.ID != item.Item.TargetID {
		return invalid()
	}
	pins, err := repository.secretPinConditions(ctx, record, loaded.ReadRevision)
	if err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	keys := []string{OperationKey(record.Owner.OperationID), ReferenceKey(item.Item.TargetID, task.Task.ID),
		EnvironmentReferenceKey(record.Owner.EnvironmentID, task.Task.ID)}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: loaded.ReadRevision})
	if err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	if read == nil || read.ReadRevision != loaded.ReadRevision || len(read.Values) != len(keys) {
		return invalid()
	}
	defer keyvalue.ClearValues(read.Values)
	change := workqueue.Change{Conditions: []keyvalue.Condition{
		{Key: Key(task.Task.ID), ModRevision: loaded.Revision},
		{Key: workqueue.TaskKey(task.Task.ID), ModRevision: task.Revision},
		{Key: workqueue.ItemKey(item.Item.ID), ModRevision: item.Revision},
	}}
	change.Conditions = append(change.Conditions, pins...)
	for index, key := range keys {
		if read.Values[index] == nil || string(read.Values[index].Value) != task.Task.ID {
			return invalid()
		}
		change.Conditions = append(change.Conditions, keyvalue.Condition{Key: key, ModRevision: read.Values[index].ModRevision})
		change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: key})
	}
	for _, pin := range pins {
		change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: pin.Key})
	}
	failure := s3connector.CheckFailureInterrupted
	if outcome == workexecution.Failed {
		failure = s3connector.CheckFailureForError(context.DeadlineExceeded)
	}
	if err := record.Stop(failure, at); err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	value, err := Encode(record)
	if err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationPut, Key: Key(task.Task.ID), Value: value})
	hash := sha256.Sum256(value)
	result := workexecution.Completion{Outcome: outcome, EffectsSettled: true,
		Evidence: workexecution.Input{ID: ids.DeriveAt(ids.KindPlan, record.CreatedAt, item.Item.ID, "connector-unstarted-result"), SHA256: hex.EncodeToString(hash[:])}}
	return change, result, nil
}
