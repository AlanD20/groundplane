package etcd

import (
	"context"
	"encoding/hex"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/backupvolumetransfer"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupvolumemanifest"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

const volumeManifestCleanupPrefix = "/v1/runtime/backup-volume-manifest-cleanup/"

type volumeManifestCleanup struct {
	PointID        string                     `json:"point_id"`
	Owner          backupvolumemanifest.Owner `json:"owner"`
	CursorRevision int64                      `json:"cursor_revision"`
}

// prepareVolumeManifestCleanup joins the transaction that deletes the exact
// verified-absent Point. Its independent intent survives Prune Task retention.
func (repository *BackupRuntimeRepository) prepareVolumeManifestCleanup(ctx context.Context,
	point backupruntime.BackupRecoveryPointSnapshot, revision int64,
) ([]etcdstore.Condition, []etcdstore.Mutation, error) {
	if point.SourceKind != backupruntime.BackupRuntimeSourceVolume {
		return nil, nil, nil
	}
	ref := point.VolumeArchive.Manifest
	if !ref.Valid() {
		return nil, nil, backupruntime.CorruptBackupRuntimeRecord()
	}
	binding := backupvolumetransfer.Binding{TaskID: ref.TaskID, AssignmentID: ref.AssignmentID,
		StepID: ref.StepID, TransferID: ref.TransferID,
		Direction: agentpb.BackupVolumeManifestDirection_BACKUP_VOLUME_MANIFEST_DIRECTION_CAPTURE}
	if _, err := hex.Decode(binding.AuthorityDigest[:], []byte(ref.AuthoritySHA256)); err != nil {
		return nil, nil, backupruntime.CorruptBackupRuntimeRecord()
	}
	owner := backupvolumemanifest.Owner{Binding: binding, AgentID: ref.AgentID,
		AgentGeneration: ref.AgentGeneration, AssignmentGeneration: ref.AssignmentGeneration}
	cursor, found, err := repository.VolumeManifestRepository().Read(ctx, owner, revision)
	if err != nil {
		return nil, nil, err
	}
	if !found || cursor.Revision != ref.CursorRevision || !cursor.Record.Complete || cursor.Record.PointID != point.ID {
		return nil, nil, backupruntime.CorruptBackupRuntimeRecord()
	}
	intent := volumeManifestCleanup{PointID: point.ID, Owner: owner, CursorRevision: ref.CursorRevision}
	encoded, err := recordcodec.Encode("backup-volume-manifest-cleanup", intent)
	if err != nil {
		return nil, nil, err
	}
	key := volumeManifestCleanupPrefix + point.ID
	return []etcdstore.Condition{{Key: key}, {Key: backupruntime.BackupOrphanKey(point.ID)},
			{Key: backupvolumemanifest.CursorKey(binding), ModRevision: ref.CursorRevision}},
		[]etcdstore.Mutation{{Type: etcdstore.MutationPut, Key: key, Value: encoded}}, nil
}

// PruneVolumeManifestBatch deletes at most 24 records from one retired Point.
// Each page repeats absence checks; an uncertain transaction is safe to retry
// because it consumes only exact observed revisions under the same intent.
func (repository *BackupRuntimeRepository) PruneVolumeManifestBatch(ctx context.Context,
	startExclusive string,
) (string, error) {
	if startExclusive != "" && (!strings.HasPrefix(startExclusive, volumeManifestCleanupPrefix) ||
		ids.Validate(ids.KindRecoveryPoint, strings.TrimPrefix(startExclusive, volumeManifestCleanupPrefix)) != nil) {
		return startExclusive, errs.New(errs.KindValidationFailed, "Volume manifest cleanup cursor is invalid")
	}
	queue, err := repository.store.Range(ctx, etcdstore.RangeRequest{
		Prefix: volumeManifestCleanupPrefix, StartExclusive: startExclusive, Limit: 1})
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
	intent, err := recordcodec.Decode[volumeManifestCleanup](value.Value, "backup-volume-manifest-cleanup")
	if err != nil || ids.Validate(ids.KindRecoveryPoint, intent.PointID) != nil || intent.Owner.Validate() != nil ||
		intent.Owner.Binding.Direction != agentpb.BackupVolumeManifestDirection_BACKUP_VOLUME_MANIFEST_DIRECTION_CAPTURE ||
		intent.CursorRevision <= 0 || value.Key != volumeManifestCleanupPrefix+intent.PointID ||
		value.Version != 1 || value.ModRevision <= intent.CursorRevision {
		return startExclusive, backupruntime.CorruptBackupRuntimeRecord()
	}
	keys := []string{
		backupruntime.BackupRecoveryPointKey(intent.PointID),
		backupruntime.BackupOrphanKey(intent.PointID),
		taskjournal.TaskStorageKey(intent.Owner.Binding.TaskID),
	}
	read, err := repository.ReadFixedKeys(ctx, keys, queue.ReadRevision)
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
	page, err := repository.store.Range(ctx, etcdstore.RangeRequest{
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
	result, err := repository.TransactRuntime(ctx, conditions, mutations)
	etcdstore.ClearValues(result.FailureReads)
	if err != nil {
		return startExclusive, err
	}
	if !result.Succeeded {
		return startExclusive, errs.New(errs.KindStateConflict, "Volume manifest cleanup ownership changed")
	}
	return next, nil
}
