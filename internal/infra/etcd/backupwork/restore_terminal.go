package backupwork

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/databaserestoreauthority"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentfence"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/postgresbackingguard"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

func (repository *InputRepository) PrepareRestoreCompletion(ctx context.Context,
	view workqueue.ResumeView, at time.Time,
) (workqueue.Change, error) {
	record, assignment, change, err := repository.LoadRestoreSource(ctx, view)
	if err != nil {
		return workqueue.Change{}, err
	}
	if view.Attempt.Attempt.StartedAt == nil || len(assignment.Sources) != 1 {
		return workqueue.Change{}, conflict("Restore completion requires its started source")
	}
	var next backupruntime.BackupRestoreRecord
	switch record.Point.SourceKind {
	case backupruntime.BackupRuntimeSourceConfig:
		next, err = backupruntime.CompleteConfigRestore(record, at)
	case backupruntime.BackupRuntimeSourceVolume:
		next, err = backupruntime.CompleteVolumeRestore(record, at)
	case backupruntime.BackupRuntimeSourceAttach:
		next, err = backupruntime.CompleteDatabaseRestore(record, at)
	default:
		return workqueue.Change{}, conflict("Restore completion source is unsupported")
	}
	if err != nil {
		return workqueue.Change{}, err
	}
	return repository.prepareRestoreRelease(ctx, view.Work, record, next, change, true)
}

func (repository *InputRepository) prepareRestoreRelease(ctx context.Context, view workqueue.View,
	record, next backupruntime.BackupRestoreRecord, change workqueue.Change, completed bool,
) (workqueue.Change, error) {
	guards, err := repository.restoreTerminalGuards(ctx, record, view.ReadRevision, completed)
	if err != nil {
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, guards...)
	fence, err := environmentfence.LoadOwned(ctx, repository.store, record.EnvironmentID, view.ReadRevision,
		environmentfence.Owner{Kind: backupruntime.BackupOperationRestore, OperationID: record.OperationID, TaskID: record.TaskID})
	if err != nil {
		return workqueue.Change{}, err
	}
	epoch, err := fence.EpochRewriteMutation()
	if err != nil {
		return workqueue.Change{}, err
	}
	if record.Point.SourceKind == backupruntime.BackupRuntimeSourceAttach {
		environment, found, err := backupruntime.DatabaseRestoreBackingEnvironmentID(record)
		if err != nil {
			return workqueue.Change{}, err
		}
		if !found {
			return workqueue.Change{}, conflict("Restore backing ownership is missing")
		}
		backing, err := postgresbackingguard.PrepareOwnership(ctx, repository.store, []string{environment}, record.EnvironmentID,
			postgresbackingguard.Owner(backupruntime.BackupOperationRestore, record.OperationID, record.TaskID), view.ReadRevision, true)
		if err != nil {
			return workqueue.Change{}, err
		}
		defer backing.Clear()
		change.Conditions = append(change.Conditions, backing.Conditions...)
		for _, mutation := range backing.Mutations {
			change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: mutation.Type, Key: mutation.Key, Value: append([]byte(nil), mutation.Value...)})
		}
	}
	encoded, err := backupruntime.EncodeBackupRestoreRecord(next)
	if err != nil {
		keyvalue.ClearMutationValues(change.Mutations)
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, fence.TransactionConditions()...)
	change.Mutations = append(change.Mutations,
		keyvalue.Mutation{Type: keyvalue.MutationPut, Key: backupruntime.BackupRestoreKey(record.TaskID), Value: encoded},
		keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: hierarchy.EnvironmentOperationLockKey(record.EnvironmentID)},
		keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: credentialGrantKey(view.Item.Item.ID, 0)}, epoch)
	return change, nil
}

func (repository *InputRepository) restoreTerminalGuards(ctx context.Context,
	record backupruntime.BackupRestoreRecord, revision int64, completed bool,
) ([]keyvalue.Condition, error) {
	membership, err := backupruntime.BackupRestoreEnvironmentIndexKey(record.EnvironmentID, record.TaskID)
	if err != nil {
		return nil, err
	}
	keys := []string{membership}
	if record.Point.SourceKind != backupruntime.BackupRuntimeSourceAttach {
		keys = append(keys, blueprints.EnvironmentBlueprintHeadKey(record.EnvironmentID))
	}
	read, err := repository.readSettlementKeys(ctx, keys, revision)
	if err != nil {
		return nil, err
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil || read.Values[0].Version != 1 || string(read.Values[0].Value) != record.TaskID {
		return nil, conflict("Restore Environment membership changed")
	}
	guards := []keyvalue.Condition{{Key: membership, ModRevision: read.Values[0].ModRevision}}
	if record.Point.SourceKind == backupruntime.BackupRuntimeSourceAttach {
		authority, err := databaserestoreauthority.Read(ctx, repository.store, record, revision)
		if err != nil {
			return nil, err
		}
		return append(guards, authority...), nil
	}
	head := read.Values[1]
	if head == nil {
		return nil, conflict("Restore destination head is unavailable")
	}
	if record.Point.SourceKind == backupruntime.BackupRuntimeSourceConfig {
		id, err := idempotency.DecodeTaskReference(head.Value)
		if record.ConfigProgress == nil || record.CurrentTarget.Config == nil {
			return nil, conflict("Restore configuration progress is unavailable")
		}
		expectedID := record.CurrentTarget.Config.BaselineRevisionID
		expectedRevision := record.CurrentTarget.Config.BaselineHeadRevision
		if completed || record.ConfigProgress.PublishedHeadRevision > 0 {
			expectedID, expectedRevision = record.TaskID, record.ConfigProgress.PublishedHeadRevision
		}
		if !completed && record.MutationStarted && id == record.TaskID && record.ConfigProgress.PublishedHeadRevision == 0 &&
			record.ConfigProgress.DeleteEntryOrdinal == record.ConfigProgress.DeleteEntryCount &&
			record.ConfigProgress.UpsertEntryOrdinal == record.Point.ConfigArchive.EntryCount {
			// Publication and the source record share one transaction. An absent
			// receipt is not permission to invent a successful publication.
			publication, readErr := repository.readSettlementKeys(ctx, []string{backupruntime.BackupRestoreKey(record.TaskID)}, revision)
			if readErr != nil {
				return nil, readErr
			}
			defer keyvalue.ClearValues(publication.Values)
			if publication.Values[0] != nil && publication.Values[0].ModRevision == head.ModRevision &&
				head.ModRevision > record.ConfigProgress.RevisionRootRevision {
				expectedID, expectedRevision = record.TaskID, head.ModRevision
				guards = append(guards, keyvalue.Condition{Key: backupruntime.BackupRestoreKey(record.TaskID), ModRevision: head.ModRevision})
			}
		}
		if err != nil || id != expectedID || head.ModRevision != expectedRevision {
			return nil, conflict("Restore configuration publication changed")
		}
	} else {
		target := record.CurrentTarget.Volume
		digest := sha256.Sum256(head.Value)
		if target == nil || target.HeadRevision != head.ModRevision || target.HeadSHA256 != hex.EncodeToString(digest[:]) {
			return nil, conflict("Restore Volume destination changed")
		}
	}
	return append(guards, keyvalue.Condition{Key: keys[1], ModRevision: head.ModRevision}), nil
}
