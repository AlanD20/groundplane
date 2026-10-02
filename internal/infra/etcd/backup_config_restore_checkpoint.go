package etcd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd/backupconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentfence"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
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

// CheckpointConfigRestore commits the native Restore transition and its exact
// receipt together. A complete transfer cannot stand in for live publication.
func (repository *BackupRuntimeRepository) CheckpointConfigRestore(ctx context.Context,
	input backupruntime.BackupCheckpointInput, current etcdstore.Versioned[backupruntime.BackupRestoreRecord],
	publication *ConfigRestoreCheckpointPublication, at time.Time,
) (int64, error) {
	if backupruntime.ValidateBackupCheckpointInput(input) != nil || current.Revision <= 0 ||
		current.Record.TaskID != input.TaskID || current.Record.Point.SourceKind != backupruntime.BackupRuntimeSourceConfig {
		return 0, configTransferAuthorityConflict()
	}
	point := current.Record.Point
	memberKey, err := backupruntime.BackupRestoreEnvironmentIndexKey(current.Record.EnvironmentID, input.TaskID)
	if err != nil {
		return 0, err
	}
	keys := []string{backupruntime.BackupRestoreKey(input.TaskID), memberKey,
		backupruntime.BackupExecutionPlanKey(input.TaskID), taskjournal.TaskStorageKey(input.TaskID),
		blueprints.EnvironmentBlueprintHeadKey(current.Record.EnvironmentID)}
	if publication != nil {
		keys = append(keys, backupconfiguration.ConfigRestoreGenerationKey(publication.Generation.Record.Owner),
			blueprints.EnvironmentBlueprintRootKey(current.Record.EnvironmentID, input.TaskID))
	}
	read, err := repository.ReadCurrentKeys(ctx, keys)
	if err != nil {
		return 0, err
	}
	defer etcdstore.ClearValues(read.Values)
	for index, value := range read.Values {
		if value == nil || value.Key != keys[index] || value.ModRevision <= 0 {
			return 0, configTransferAuthorityConflict()
		}
	}
	stored, err := backupruntime.DecodeBackupRestoreRecord(read.Values[0].Value)
	if err != nil || read.Values[0].ModRevision != current.Revision ||
		!backupruntime.BackupRestoreRecordsEqual(stored, current.Record) || read.Values[1].Version != 1 ||
		string(read.Values[1].Value) != input.TaskID || read.Values[1].ModRevision > current.Revision {
		return 0, configTransferAuthorityConflict()
	}
	sealed, err := backupruntime.DecodeBackupExecutionPlan(read.Values[2].Value)
	if err != nil || read.Values[2].Version != 1 ||
		backupruntime.ValidateConfigRestoreExecutionPlan(stored, sealed) != nil {
		return 0, configTransferAuthorityConflict()
	}
	task, err := DecodeTaskRecord(read.Values[3].Value)
	if err != nil || task.OperationID != stored.OperationID || task.Type != taskjournal.TaskRestore ||
		task.PlanID != sealed.PlanId || task.PlanHash != hex.EncodeToString(sealed.PlanHash) ||
		task.Target != stored.EnvironmentID || task.Owner.EnvironmentID != stored.EnvironmentID ||
		sealed.Steps[0].StepId != input.StepID || sealed.Steps[0].GetBackupStep().ExecutionId != input.ExecutionID ||
		hex.EncodeToString(sealed.Steps[0].GetBackupStep().StepDigest) != input.AuthoritySHA256 {
		return 0, configTransferAuthorityConflict()
	}
	target := stored.CurrentTarget.Config
	head, headErr := idempotency.DecodeTaskReference(read.Values[4].Value)
	expectedHead, expectedHeadRevision := target.BaselineRevisionID, target.BaselineHeadRevision
	if input.Request.GetConfig().GetMaterializationVerified() != nil ||
		input.Request.GetSourceCleanupCompleted() != nil {
		expectedHead, expectedHeadRevision = input.TaskID, stored.ConfigProgress.PublishedHeadRevision
	}
	if headErr != nil || head != expectedHead || expectedHeadRevision <= 0 ||
		read.Values[4].ModRevision != expectedHeadRevision {
		return 0, configTransferAuthorityConflict()
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
			root.EnvironmentID != stored.EnvironmentID || root.RevisionID != input.TaskID ||
			root.SourceKind != blueprints.EnvironmentBlueprintSourceMutation ||
			root.RenderGeneration != target.RenderGeneration || root.BaselineHeadRevision != target.BaselineHeadRevision ||
			owner.EnvironmentID != stored.EnvironmentID || owner.GenerationID != stored.RestoreGenerationID ||
			owner.Transfer.Binding.TaskID != input.TaskID || owner.Transfer.Binding.AssignmentID != input.AssignmentID ||
			owner.Transfer.Binding.StepID != input.StepID || owner.Transfer.Binding.ExecutionID != input.ExecutionID ||
			owner.Transfer.AgentID != input.AgentID || owner.Transfer.AgentGeneration != input.AgentGeneration ||
			owner.Transfer.AssignmentGeneration != input.AssignmentGeneration || owner.Transfer.AuthoritySHA256 != input.AuthoritySHA256 ||
			(input.Request.GetConfig().GetTransferCompleted() != nil && !proto.Equal(generation.Completed, input.Request.GetConfig().GetTransferCompleted())) ||
			publication.FileProof == nil || publication.FileProof.RestoreGenerationId != stored.RestoreGenerationID ||
			publication.FileProof.MaterializedEntryCount != stored.Point.ConfigArchive.EntryCount || len(publication.FileProof.MaterializationSha256) != 32 {
			return 0, configTransferAuthorityConflict()
		}
		more, err := repository.configRestoreTransferGuard(ctx, owner, read.ReadRevision)
		if err != nil {
			return 0, err
		}
		conditions = append(conditions, more...)
		digest := sha256.Sum256(read.Values[6].Value)
		progress = backupruntime.BackupRestoreConfigProgress{RevisionRootSHA256: hex.EncodeToString(digest[:]),
			RevisionRootRevision: publication.RootRevision, DeleteEntryCount: publication.DeleteEntryCount}
		metadata, err := prepareConfigRestoreMetadata(publication.Generation, publication.Root,
			publication.Projection, publication.Identities)
		if err != nil {
			return 0, err
		}
		progress.ProjectionSHA256, progress.IdentitiesSHA256 = metadata.projectionSHA256, metadata.identitiesSHA256
		progress.ExpectedMaterializationSHA256 = hex.EncodeToString(publication.FileProof.MaterializationSha256)
		metadata.clear()
		if materialized := input.Request.GetConfig().GetMaterializationVerified(); materialized != nil {
			if progress.RevisionRootSHA256 != stored.ConfigProgress.RevisionRootSHA256 ||
				progress.RevisionRootRevision != stored.ConfigProgress.RevisionRootRevision ||
				progress.ProjectionSHA256 != stored.ConfigProgress.ProjectionSHA256 || progress.IdentitiesSHA256 != stored.ConfigProgress.IdentitiesSHA256 ||
				progress.ExpectedMaterializationSHA256 != stored.ConfigProgress.ExpectedMaterializationSHA256 ||
				!proto.Equal(materialized, publication.FileProof) {
				return 0, configTransferAuthorityConflict()
			}
			progress = backupruntime.BackupRestoreConfigProgress{}
		}
		conditions = append(conditions, etcdstore.Condition{Key: keys[5], ModRevision: publication.Generation.Revision},
			etcdstore.Condition{Key: keys[6], ModRevision: publication.RootRevision})
	}
	if input.Request.GetConfig().GetMaterializationVerified() != nil && publication == nil {
		return 0, configTransferAuthorityConflict()
	}
	next, err := backupruntime.PrepareConfigRestoreCheckpoint(stored, input.Request, progress, at)
	if err != nil {
		return 0, err
	}
	fence, err := environmentfence.LoadOwned(
		ctx,
		repository.store,
		stored.EnvironmentID,
		read.ReadRevision,
		environmentfence.Owner{
			Kind:        backupruntime.BackupOperationRestore,
			OperationID: stored.OperationID,
			TaskID:      input.TaskID,
		},
	)
	if err != nil {
		return 0, err
	}
	conditions, err = environmentfence.AppendConditions(conditions, fence)
	if err != nil {
		return 0, err
	}
	checkpoint, err := repository.loadBackupCheckpointPlan(ctx, input, read.ReadRevision,
		backupCheckpointBinding{taskType: taskjournal.TaskRestore, pointID: point.ID})
	if err != nil {
		return 0, err
	}
	defer checkpoint.clear()
	if checkpoint.duplicate {
		return checkpoint.commitRevision, nil
	}
	value, err := backupruntime.EncodeBackupRestoreRecord(next)
	if err != nil {
		return 0, err
	}
	defer clear(value)
	conditions, mutations, err := checkpoint.composeTransaction(conditions,
		[]etcdstore.Mutation{{Type: etcdstore.MutationPut, Key: keys[0], Value: value}})
	if err != nil {
		return 0, err
	}
	defer etcdstore.ClearMutationValues(mutations)
	result, err := repository.TransactRuntime(ctx, conditions, mutations)
	etcdstore.ClearValues(result.FailureReads)
	if err != nil {
		return 0, err
	}
	if !result.Succeeded {
		return 0, errs.New(errs.KindStateConflict, "restore checkpoint authority changed")
	}
	return result.Revision, nil
}
