package etcd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"

	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentfence"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"google.golang.org/protobuf/proto"
)

func (repository *BackupRuntimeRepository) PrepareVolumeRestoreProgress(ctx context.Context,
	view workqueue.ResumeView, admitted backupwork.CheckpointAdmission,
	current etcdstore.Versioned[backupruntime.BackupRestoreRecord],
	old *backupruntime.BackupVolumeOldManifestProof, at time.Time,
) (workqueue.Change, error) {
	if admitted.Input() == nil || admitted.Request == nil || admitted.Replay != nil ||
		current.Revision <= 0 || current.ReadRevision != view.Work.ReadRevision ||
		current.Record.TaskID != view.Work.Task.Task.ID || current.Record.Point.SourceKind != backupruntime.BackupRuntimeSourceVolume {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Volume Restore checkpoint authority is invalid")
	}
	membership, err := backupruntime.BackupRestoreEnvironmentIndexKey(
		current.Record.EnvironmentID,
		view.Work.Task.Task.ID,
	)
	if err != nil {
		return workqueue.Change{}, err
	}
	keys := []string{backupruntime.BackupRestoreKey(view.Work.Task.Task.ID), membership,
		backupwork.InputKey(view.Work.Item.Item.ID), workqueue.TaskKey(view.Work.Task.Task.ID),
		blueprints.EnvironmentBlueprintHeadKey(current.Record.EnvironmentID)}
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: view.Work.ReadRevision})
	if err != nil {
		return workqueue.Change{}, err
	}
	if read == nil || read.ReadRevision != view.Work.ReadRevision || len(read.Values) != len(keys) {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Volume Restore checkpoint snapshot is unavailable")
	}
	defer etcdstore.ClearValues(read.Values)
	for index, value := range read.Values {
		if value == nil || value.Key != keys[index] || value.ModRevision <= 0 {
			return workqueue.Change{}, errs.New(
				errs.KindStateConflict,
				"Volume Restore checkpoint authority is incomplete",
			)
		}
	}
	stored, err := backupruntime.DecodeBackupRestoreRecord(read.Values[0].Value)
	if err != nil || read.Values[0].ModRevision != current.Revision ||
		!backupruntime.BackupRestoreRecordsEqual(stored, current.Record) || read.Values[1].Version != 1 ||
		string(read.Values[1].Value) != view.Work.Task.Task.ID || read.Values[1].ModRevision > current.Revision {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Volume Restore native cursor changed")
	}
	captured, err := executionplan.DecodeBackupWorkInput(read.Values[2].Value)
	if err != nil || read.Values[2].Version != 1 || !proto.Equal(captured, admitted.Input()) ||
		read.Values[3].ModRevision != view.Work.Task.Revision ||
		backupruntime.ValidateRestoreWorkInput(stored, captured) != nil {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Volume Restore captured work changed")
	}
	headSHA := sha256.Sum256(read.Values[4].Value)
	if read.Values[4].ModRevision != stored.CurrentTarget.Volume.HeadRevision ||
		hex.EncodeToString(headSHA[:]) != stored.CurrentTarget.Volume.HeadSHA256 {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Volume Restore destination head changed")
	}
	var next backupruntime.BackupRestoreRecord
	if admitted.Recovery != nil {
		next, err = backupruntime.PrepareVolumeRestoreRecoveryCheckpoint(stored, captured, admitted.Request, old, at)
	} else {
		next, err = backupruntime.PrepareVolumeRestoreCheckpoint(stored, captured, admitted.Request, old, at)
	}
	if err != nil {
		return workqueue.Change{}, err
	}
	fence, err := environmentfence.LoadOwned(ctx, repository.store, stored.EnvironmentID,
		read.ReadRevision, environmentfence.Owner{Kind: backupruntime.BackupOperationRestore,
			OperationID: stored.OperationID, TaskID: view.Work.Task.Task.ID})
	if err != nil {
		return workqueue.Change{}, err
	}
	conditions := make([]etcdstore.Condition, 0, len(keys)+4)
	for index, key := range keys {
		conditions = append(conditions, etcdstore.Condition{Key: key, ModRevision: read.Values[index].ModRevision})
	}
	conditions, err = environmentfence.AppendConditions(conditions, fence)
	if err != nil {
		return workqueue.Change{}, err
	}
	value, err := backupruntime.EncodeBackupRestoreRecord(next)
	if err != nil {
		return workqueue.Change{}, err
	}
	return workqueue.Change{Conditions: conditions, Mutations: []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: keys[0], Value: value},
	}}, nil
}
