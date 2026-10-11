package etcd

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/dnsrecords"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Pending removal cannot claim the Environment writer until CoreDNS has
// withdrawn its answer. Skipping it here lets the newer resolver Task run.
func (repository *TaskRepository) removalDNSClaimConditions(ctx context.Context,
	task TaskRecord, revision int64, at time.Time,
) ([]keyvalue.Condition, bool, error) {
	if task.Executor == taskjournal.TaskExecutorAgent && task.Params[taskjournal.TaskZoneRemovalOperationParam] != "" {
		return repository.zoneRemovalDNSClaimConditions(ctx, task, revision, at)
	}
	if task.Type != taskjournal.TaskRemove || ids.Validate(ids.KindService, task.Target) != nil {
		return nil, true, nil
	}
	key := environmentchanges.ServiceRemovalIntentKey(task.ID)
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: revision})
	if err != nil {
		return nil, false, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 {
		return nil, false, errs.New(errs.KindInternal, "Service removal DNS gate read is incomplete")
	}
	conditions := []keyvalue.Condition{{Key: key, ModRevision: keyvalue.RevisionOf(read.Values[0])}}
	if read.Values[0] == nil {
		return conditions, true, nil // Other native removal owners retain their own guards.
	}
	intent, err := environmentchanges.DecodeServiceRemovalIntent(read.Values[0].Value)
	if err != nil {
		return nil, false, err
	}
	if err := validateServiceRemovalTaskOwner(task, intent); err != nil {
		return nil, false, err
	}
	if intent.Status != taskjournal.TaskStatusPending {
		return nil, false, errs.New(errs.KindStateConflict, "Service removal DNS gate is no longer pending")
	}
	if intent.Authored == nil && !at.Before(task.CreatedAt.Add(time.Duration(task.TimeoutSeconds)*time.Second)) {
		return nil, false, nil
	}
	dnsConditions, err := dnsrecords.NewReader(repository.store).
		RequireDNSUnreferenced(ctx, intent.ServiceID, "", revision)
	if kind, _ := errs.KindOf(err); kind == errs.KindResourceInUse {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return append(conditions, dnsConditions...), true, nil
}
