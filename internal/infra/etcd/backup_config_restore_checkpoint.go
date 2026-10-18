package etcd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd/backupconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentfence"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type ConfigRestoreCheckpointPublication struct {
	Generation       etcdstore.Versioned[backupconfiguration.ConfigRestoreGenerationRecord]
	Root             blueprints.EnvironmentBlueprintSeal
	RootRevision     int64
	DeleteEntryCount uint32
	Projection       environmentprojection.EnvironmentComposeProjection
	Identities       environmentprojection.EnvironmentOwnedIdentities
	FileProof        *agentpb.BackupConfigMaterializationVerified
}

// PrepareConfigRestoreProgress joins resource publication to the native work
// checkpoint. It never commits independently or acknowledges an Agent report.
func (repository *BackupRuntimeRepository) PrepareConfigRestoreProgress(ctx context.Context,
	view workqueue.ResumeView, admitted backupwork.CheckpointAdmission,
	current etcdstore.Versioned[backupruntime.BackupRestoreRecord],
	publication *ConfigRestoreCheckpointPublication, at time.Time,
) (workqueue.Change, error) {
	if admitted.Input() == nil || admitted.Request == nil || admitted.Replay != nil ||
		current.Revision <= 0 || current.ReadRevision != view.Work.ReadRevision ||
		current.Record.TaskID != view.Work.Task.Task.ID ||
		current.Record.Point.SourceKind != backupruntime.BackupRuntimeSourceConfig ||
		backupruntime.ValidateRestoreWorkInput(current.Record, admitted.Input()) != nil {
		return workqueue.Change{}, configTransferAuthorityConflict()
	}
	request := admitted.Request
	taskID := view.Work.Task.Task.ID
	memberKey, err := backupruntime.BackupRestoreEnvironmentIndexKey(current.Record.EnvironmentID, taskID)
	if err != nil {
		return workqueue.Change{}, err
	}
	keys := []string{backupruntime.BackupRestoreKey(taskID), memberKey,
		backupwork.InputKey(view.Work.Item.Item.ID), workqueue.TaskKey(taskID),
		blueprints.EnvironmentBlueprintHeadKey(current.Record.EnvironmentID)}
	if publication != nil {
		keys = append(keys, backupconfiguration.ConfigRestoreGenerationKey(publication.Generation.Record.Owner),
			blueprints.EnvironmentBlueprintRootKey(current.Record.EnvironmentID, taskID))
	}
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: view.Work.ReadRevision})
	if err != nil {
		return workqueue.Change{}, err
	}
	if read == nil || read.ReadRevision != view.Work.ReadRevision || len(read.Values) != len(keys) {
		return workqueue.Change{}, configTransferAuthorityConflict()
	}
	defer etcdstore.ClearValues(read.Values)
	for index, value := range read.Values {
		if value == nil || value.Key != keys[index] || value.ModRevision <= 0 {
			return workqueue.Change{}, configTransferAuthorityConflict()
		}
	}
	stored, err := backupruntime.DecodeBackupRestoreRecord(read.Values[0].Value)
	if err != nil || read.Values[0].ModRevision != current.Revision ||
		!backupruntime.BackupRestoreRecordsEqual(stored, current.Record) || read.Values[1].Version != 1 ||
		string(read.Values[1].Value) != taskID || read.Values[1].ModRevision > current.Revision {
		return workqueue.Change{}, configTransferAuthorityConflict()
	}
	sealed, err := backupwork.DecodeBackupWorkInput(read.Values[2].Value)
	if err != nil || read.Values[2].Version != 1 || !proto.Equal(sealed, admitted.Input()) ||
		read.Values[3].ModRevision != view.Work.Task.Revision ||
		backupruntime.ValidateRestoreWorkInput(stored, sealed) != nil {
		return workqueue.Change{}, configTransferAuthorityConflict()
	}
	binding, err := admitted.ConfigBinding(view.Attempt.Attempt.Executor)
	if err != nil {
		return workqueue.Change{}, err
	}
	target := stored.CurrentTarget.Config
	head, headErr := idempotency.DecodeTaskReference(read.Values[4].Value)
	expectedHead, expectedHeadRevision := target.BaselineRevisionID, target.BaselineHeadRevision
	if request.GetConfig().GetMaterializationVerified() != nil ||
		request.GetSourceCleanupCompleted() != nil {
		expectedHead, expectedHeadRevision = taskID, stored.ConfigProgress.PublishedHeadRevision
	}
	if headErr != nil || head != expectedHead || expectedHeadRevision <= 0 ||
		read.Values[4].ModRevision != expectedHeadRevision {
		return workqueue.Change{}, configTransferAuthorityConflict()
	}
	progress := backupruntime.BackupRestoreConfigProgress{}
	conditions := []etcdstore.Condition{
		{Key: keys[0], ModRevision: current.Revision}, {Key: keys[1], ModRevision: read.Values[1].ModRevision},
		{
			Key:         keys[2],
			ModRevision: read.Values[2].ModRevision,
		}, {Key: keys[4], ModRevision: expectedHeadRevision},
	}
	if publication != nil {
		generation, err := backupconfiguration.DecodeConfigRestoreGeneration(read.Values[5].Value)
		owner := generation.Owner
		root, rootErr := blueprints.DecodeEnvironmentBlueprintSeal(read.Values[6].Value)
		if err != nil || rootErr != nil || generation.Owner != publication.Generation.Record.Owner ||
			!proto.Equal(generation.Completed, publication.Generation.Record.Completed) ||
			read.Values[5].Version != 1 || read.Values[5].ModRevision != publication.Generation.Revision ||
			read.Values[6].Version != 1 || read.Values[6].ModRevision != publication.RootRevision || root != publication.Root ||
			root.EnvironmentID != stored.EnvironmentID || root.RevisionID != taskID ||
			root.SourceKind != blueprints.EnvironmentBlueprintSourceMutation ||
			root.RenderGeneration != target.RenderGeneration || root.BaselineHeadRevision != target.BaselineHeadRevision ||
			owner.EnvironmentID != stored.EnvironmentID || owner.GenerationID != stored.RestoreGenerationID ||
			owner.Transfer.Binding != binding ||
			(request.GetConfig().GetTransferCompleted() != nil && !proto.Equal(generation.Completed, request.GetConfig().GetTransferCompleted())) ||
			publication.FileProof == nil || publication.FileProof.RestoreGenerationId != stored.RestoreGenerationID ||
			publication.FileProof.MaterializedEntryCount != stored.Point.ConfigArchive.EntryCount || len(publication.FileProof.MaterializationSha256) != 32 {
			return workqueue.Change{}, configTransferAuthorityConflict()
		}
		var more []etcdstore.Condition
		if admitted.Recovery != nil {
			more, err = repository.configRestoreExecutionGuard(ctx, owner, read.ReadRevision, &view.Attempt.Attempt)
		} else {
			more, err = repository.configRestoreTransferGuard(ctx, owner, read.ReadRevision)
		}
		if err != nil {
			return workqueue.Change{}, err
		}
		conditions = append(conditions, more...)
		digest := sha256.Sum256(read.Values[6].Value)
		progress = backupruntime.BackupRestoreConfigProgress{RevisionRootSHA256: hex.EncodeToString(digest[:]),
			RevisionRootRevision: publication.RootRevision, DeleteEntryCount: publication.DeleteEntryCount}
		metadata, err := prepareConfigRestoreMetadata(publication.Generation, publication.Root,
			publication.Projection, publication.Identities)
		if err != nil {
			return workqueue.Change{}, err
		}
		progress.ProjectionSHA256, progress.IdentitiesSHA256 = metadata.projectionSHA256, metadata.identitiesSHA256
		progress.ExpectedMaterializationSHA256 = hex.EncodeToString(publication.FileProof.MaterializationSha256)
		metadata.clear()
		if materialized := request.GetConfig().GetMaterializationVerified(); materialized != nil {
			if progress.RevisionRootSHA256 != stored.ConfigProgress.RevisionRootSHA256 ||
				progress.RevisionRootRevision != stored.ConfigProgress.RevisionRootRevision ||
				progress.ProjectionSHA256 != stored.ConfigProgress.ProjectionSHA256 || progress.IdentitiesSHA256 != stored.ConfigProgress.IdentitiesSHA256 ||
				progress.ExpectedMaterializationSHA256 != stored.ConfigProgress.ExpectedMaterializationSHA256 ||
				!proto.Equal(materialized, publication.FileProof) {
				return workqueue.Change{}, configTransferAuthorityConflict()
			}
			progress = backupruntime.BackupRestoreConfigProgress{}
		}
		conditions = append(conditions, etcdstore.Condition{Key: keys[5], ModRevision: publication.Generation.Revision},
			etcdstore.Condition{Key: keys[6], ModRevision: publication.RootRevision})
	}
	if request.GetConfig().GetMaterializationVerified() != nil && publication == nil {
		return workqueue.Change{}, configTransferAuthorityConflict()
	}
	var next backupruntime.BackupRestoreRecord
	if admitted.Recovery != nil {
		next, err = backupruntime.PrepareConfigRestoreRecoveryCheckpoint(stored, admitted.Input(), request, progress, at)
	} else {
		next, err = backupruntime.PrepareConfigRestoreCheckpoint(stored, admitted.Input(), request, progress, at)
	}
	if err != nil {
		return workqueue.Change{}, err
	}
	fence, err := environmentfence.LoadOwned(
		ctx,
		repository.store,
		stored.EnvironmentID,
		read.ReadRevision,
		environmentfence.Owner{
			Kind:        backupruntime.BackupOperationRestore,
			OperationID: stored.OperationID,
			TaskID:      taskID,
		},
	)
	if err != nil {
		return workqueue.Change{}, err
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
