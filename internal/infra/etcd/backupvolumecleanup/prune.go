package backupvolumecleanup

import (
	"context"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupvolumemanifest"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type pruneStore interface {
	Get(context.Context, string) (*etcdstore.GetResult, error)
	GetMany(context.Context, etcdstore.GetManyRequest) (*etcdstore.GetManyResult, error)
	Range(context.Context, etcdstore.RangeRequest) (*etcdstore.RangeResult, error)
	Transact(context.Context, []etcdstore.Condition, []etcdstore.Mutation) (etcdstore.TransactionResult, error)
}

// PruneBatch deletes at most 24 records from one retired Point.
// Each page repeats absence checks; an uncertain transaction is safe to retry
// because it consumes only exact observed revisions under the same intent.
func PruneBatch(ctx context.Context, store pruneStore,
	startExclusive string,
) (string, error) {
	runtime := backupruntime.NewWriter(store)
	if startExclusive != "" && (!strings.HasPrefix(startExclusive, Prefix) ||
		ids.Validate(ids.KindRecoveryPoint, strings.TrimPrefix(startExclusive, Prefix)) != nil) {
		return startExclusive, errs.New(errs.KindValidationFailed, "Volume manifest cleanup cursor is invalid")
	}
	queue, err := store.Range(ctx, etcdstore.RangeRequest{
		Prefix: Prefix, StartExclusive: startExclusive, Limit: 1})
	if err != nil {
		return startExclusive, err
	}
	if queue == nil || queue.ReadRevision <= 0 || len(queue.Values) > 1 {
		return startExclusive, backupruntime.CorruptBackupRuntimeRecord()
	}
	defer etcdstore.ClearRangeValues(queue.Values)
	if len(queue.Values) == 0 {
		return "", nil
	}
	value := queue.Values[0]
	intent, err := recordcodec.Decode[Intent](value.Value, "backup-volume-manifest-cleanup")
	if err != nil || ids.Validate(ids.KindRecoveryPoint, intent.PointID) != nil || intent.Owner.Validate() != nil ||
		intent.Owner.Binding.Direction != agentpb.BackupVolumeManifestDirection_BACKUP_VOLUME_MANIFEST_DIRECTION_CAPTURE ||
		intent.CursorRevision <= 0 || value.Key != Prefix+intent.PointID ||
		value.Version != 1 || value.ModRevision <= intent.CursorRevision {
		return startExclusive, backupruntime.CorruptBackupRuntimeRecord()
	}
	keys := []string{
		backupruntime.BackupRecoveryPointKey(intent.PointID),
		backupruntime.BackupOrphanKey(intent.PointID),
		taskjournal.TaskStorageKey(intent.Owner.Binding.TaskID),
	}
	read, err := runtime.ReadFixedKeys(ctx, keys, queue.ReadRevision)
	if err != nil {
		return startExclusive, err
	}
	defer etcdstore.ClearValues(read.Values)
	if len(read.Values) != len(keys) {
		return startExclusive, backupruntime.CorruptBackupRuntimeRecord()
	}
	if read.Values[0] != nil || read.Values[1] != nil || read.Values[2] != nil {
		// A retained owner holds this cleanup, without starving other intents.
		// Task retention separately waits for acknowledged stage retirement.
		return value.Key, nil
	}
	prefix := backupvolumemanifest.Prefix(intent.Owner.Binding)
	page, err := store.Range(ctx, etcdstore.RangeRequest{
		Prefix: prefix, Revision: queue.ReadRevision, Limit: 24})
	if err != nil {
		return startExclusive, err
	}
	if page == nil || page.ReadRevision != queue.ReadRevision || len(page.Values) > 24 {
		return startExclusive, backupruntime.CorruptBackupRuntimeRecord()
	}
	defer etcdstore.ClearRangeValues(page.Values)
	conditions := []etcdstore.Condition{{Key: value.Key, ModRevision: value.ModRevision},
		{Key: keys[0]}, {Key: keys[1]}, {Key: keys[2]}}
	mutations := make([]etcdstore.Mutation, 0, len(page.Values)+1)
	for _, item := range page.Values {
		if item.ModRevision <= 0 || item.ModRevision > intent.CursorRevision ||
			backupvolumemanifest.ValidateCleanupEntry(intent.Owner, item.Key, item.Value) != nil {
			return startExclusive, backupruntime.CorruptBackupRuntimeRecord()
		}
		conditions = append(conditions, etcdstore.Condition{Key: item.Key, ModRevision: item.ModRevision})
		mutations = append(mutations, etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: item.Key})
	}
	next := startExclusive
	if len(page.Values) == 0 {
		if page.More {
			return startExclusive, backupruntime.CorruptBackupRuntimeRecord()
		}
		conditions = append(conditions, etcdstore.Condition{Key: prefix, Prefix: true})
		mutations = append(mutations, etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: value.Key})
		next = value.Key
	}
	result, err := runtime.TransactRuntime(ctx, conditions, mutations)
	etcdstore.ClearValues(result.FailureReads)
	if err != nil {
		return startExclusive, err
	}
	if !result.Succeeded {
		return startExclusive, errs.New(errs.KindStateConflict, "Volume manifest cleanup ownership changed")
	}
	return next, nil
}
