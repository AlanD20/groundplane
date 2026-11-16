package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"time"
)

// PrepareBackupKeyRotationAuthority returns the captured key and lock writes
// to promote the original accepted Task; it creates no second Task or marker.
func (repository *BackupPolicyRepository) PrepareBackupKeyRotationAuthority(ctx context.Context,
	prepared PreparedBackupKeyRotation, accepted workexecution.Task, at time.Time,
) (BackupKeyRotationCapture, error) {
	publication := prepared.Publication
	if publication == nil || publication.state == nil {
		return BackupKeyRotationCapture{}, errs.New(
			errs.KindInternal,
			"backup key rotation publication is not prepared",
		)
	}
	publication.state.mu.Lock()
	if publication.state.consumed || publication.state.repository != repository {
		publication.state.mu.Unlock()
		return BackupKeyRotationCapture{}, errs.New(
			errs.KindStateConflict,
			"backup key rotation publication is unavailable",
		)
	}
	publication.state.consumed = true
	plan := publication.state.plan
	publication.state.repository, publication.state.plan = nil, backupKeyRotationPublicationPlan{}
	publication.state.mu.Unlock()
	defer plan.clear()
	reference, err := backupruntime.KeyRotationInput(plan.record)
	if err != nil {
		return BackupKeyRotationCapture{}, err
	}
	if plan.task.Intent != reference || plan.item.Input == nil || *plan.item.Input != reference ||
		accepted.ID != plan.task.ID || accepted.Scope != plan.task.Scope {
		return BackupKeyRotationCapture{}, errs.New(
			errs.KindStateConflict,
			"backup key rotation publication input changed",
		)
	}
	task := accepted
	task.Intent, task.ExpectedItems, task.UpdatedAt = reference, 1, at
	item := plan.item
	item.TargetName = task.TargetName
	item.Deadline = at.Add(time.Duration(backupKeyRotationTimeoutSeconds) * time.Second)
	if task.Deadline.Before(item.Deadline) {
		item.Deadline = task.Deadline
	}
	conditions := append([]keyvalue.Condition(nil), plan.conditions...)
	mutations := keyvalue.CloneMutations(plan.mutations)
	initiation, err := newTaskInitiation(plan.owner, taskjournal.TaskActorOperator)
	if err != nil {
		return BackupKeyRotationCapture{}, err
	}
	classify := func(_ int64, _ []*keyvalue.KeyValue) error {
		return errs.New(errs.KindStateConflict, "backup key rotation publication state changed")
	}
	conditions, _, err = prepareTaskInitiationFences(initiation, conditions, classify)
	if err != nil {
		return BackupKeyRotationCapture{}, err
	}
	return BackupKeyRotationCapture{
		Task:      task,
		Item:      item,
		Authority: workqueue.Change{Conditions: conditions, Mutations: mutations},
	}, nil

}

type BackupKeyRotationCapture struct {
	Task      workexecution.Task
	Item      workexecution.Item
	Authority workqueue.Change
}

func (plan *backupKeyRotationPublicationPlan) clear() {
	if plan == nil {
		return
	}
	keyvalue.ClearMutationValues(plan.mutations)
	clear(plan.record.NextEncryptedIdentity)
	plan.mutations = nil
}
