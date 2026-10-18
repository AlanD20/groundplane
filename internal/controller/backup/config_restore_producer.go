package backup

import (
	"bytes"
	"context"

	"github.com/AlanD20/groundplane/internal/common/backupconfigmaterialization"
	"github.com/AlanD20/groundplane/internal/common/backupconfigtransfer"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type configRestorePublicationStore interface {
	configRestoreGenerationStore
	configRestoreValueStore
	StageConfigRestoreRevision(context.Context, etcdstore.Versioned[backupconfiguration.ConfigRestoreGenerationRecord],
		string, blueprints.ConfigRestoreRevision) (int64, error)
}

type configRestoreBaselineStore interface {
	GetMany(context.Context, etcdstore.GetManyRequest) (*etcdstore.GetManyResult, error)
	Range(context.Context, etcdstore.RangeRequest) (*etcdstore.RangeResult, error)
}

type ConfigRestoreProducer struct {
	sources     configRestoreBaselineStore
	publication configRestorePublicationStore
	protector   *secretvalue.Protector
}

func NewConfigRestoreProducer(sources configRestoreBaselineStore, publication configRestorePublicationStore,
	protector *secretvalue.Protector,
) (*ConfigRestoreProducer, error) {
	if sources == nil || publication == nil || protector == nil {
		return nil, configSnapshotInvalid()
	}
	return &ConfigRestoreProducer{sources: sources, publication: publication, protector: protector}, nil
}

// PreparePublished reads only the sealed predecessor's immutable revision.
// Latest reads establish a usable MVCC view, never select a newer desired head.
// The native writer fences every value against the already-published Restore.
func (producer *ConfigRestoreProducer) PreparePublished(ctx context.Context,
	owner backupconfiguration.ConfigRestoreTransferOwner, expected *agentpb.BackupConfigContentAuthority,
	pointID string,
) (*ConfigRestorePublication, error) {
	publication, err := producer.PrepareCandidate(ctx, owner, expected, pointID)
	if err != nil {
		return nil, err
	}
	if err := publication.StageValues(ctx, publication.generation, producer.publication, producer.protector, publication.createdAt); err != nil {
		publication.Clear()
		return nil, err
	}
	if err := producer.ProveCandidateFiles(ctx, publication); err != nil {
		publication.Clear()
		return nil, err
	}
	rootRevision, err := producer.publication.StageConfigRestoreRevision(
		ctx,
		publication.SourceSeal,
		pointID,
		publication.Revision,
	)
	if err != nil {
		publication.Clear()
		return nil, err
	}
	publication.RootRevision = rootRevision
	return publication, nil
}

// PrepareCandidate reconstructs exactly the pinned candidate without writes.
// Publication retries use this after the predecessor head has already moved;
// they cannot re-stage values or select a newer baseline.
func (producer *ConfigRestoreProducer) PrepareCandidate(ctx context.Context,
	owner backupconfiguration.ConfigRestoreTransferOwner, expected *agentpb.BackupConfigContentAuthority,
	pointID string,
) (*ConfigRestorePublication, error) {
	return producer.prepareCandidate(ctx, owner, expected, pointID, false)
}

// PrepareRecoveryCandidate reconstructs the same immutable generation even
// after forward cancellation. It grants no publication permission; every write
// must still be fenced by the active recovery attempt.
func (producer *ConfigRestoreProducer) PrepareRecoveryCandidate(ctx context.Context,
	owner backupconfiguration.ConfigRestoreTransferOwner, expected *agentpb.BackupConfigContentAuthority,
	pointID string, assignment *agentpb.BackupRecoveryAssignment, identity workexecution.Identity,
) (*ConfigRestorePublication, error) {
	binding, err := backupconfigtransfer.BindRecoverySource(assignment, identity, owner.Transfer.Binding.SourceOrdinal,
		agentpb.BackupConfigDirection_BACKUP_CONFIG_DIRECTION_RESTORE)
	if err != nil {
		return nil, err
	}
	if binding != owner.Transfer.Binding {
		return nil, configSnapshotGuardConflict()
	}
	return producer.prepareCandidate(ctx, owner, expected, pointID, true)
}

func (producer *ConfigRestoreProducer) prepareCandidate(ctx context.Context,
	owner backupconfiguration.ConfigRestoreTransferOwner, expected *agentpb.BackupConfigContentAuthority,
	pointID string, recovering bool,
) (*ConfigRestorePublication, error) {
	if ctx == nil || producer == nil || backupconfiguration.ValidateConfigRestoreTransferOwner(owner) != nil {
		return nil, configSnapshotInvalid()
	}
	generation, err := OpenConfigRestoreGeneration(ctx, owner, expected, producer.publication, producer.protector)
	if err != nil {
		return nil, err
	}
	binding := owner.Transfer.Binding
	key := workqueue.TaskKey(binding.Attempt.TaskID)
	planKey := backupwork.InputKey(binding.Attempt.WorkItemID)
	read, err := producer.sources.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{key, planKey}})
	if err != nil {
		return nil, err
	}
	if read == nil || read.ReadRevision <= 0 || len(read.Values) != 2 || read.Values[0] == nil ||
		read.Values[1] == nil ||
		read.Values[0].Key != key ||
		read.Values[1].Key != planKey ||
		read.Values[1].Version != 1 {
		return nil, configSnapshotGuardConflict()
	}
	defer etcdstore.ClearValues(read.Values)
	task, err := workqueue.DecodeTask(read.Values[0].Value)
	if err != nil || task.ID != binding.Attempt.TaskID || task.Action != workexecution.ActionRestore ||
		task.FinishedAt != nil || (task.Stop != nil && !recovering) || task.TargetID != owner.EnvironmentID || task.CreatedAt.IsZero() {
		return nil, configSnapshotGuardConflict()
	}
	sealed, err := executionplan.DecodeBackupWorkInput(read.Values[1].Value)
	if err != nil || len(sealed.Steps) != 1 || binding.SourceOrdinal != 0 {
		return nil, configSnapshotGuardConflict()
	}
	if _, err := executionplan.OpenBackupWorkInput(binding.Attempt, sealed); err != nil {
		return nil, err
	}
	step := sealed.Steps[0]
	config := step.GetRestore().GetConfig()
	if config == nil || step.ExecutionId != binding.TransferID || !bytes.Equal(step.StepDigest, binding.AuthorityDigest[:]) ||
		config.DestinationEnvironmentId != owner.EnvironmentID ||
		config.RestoreGenerationId != owner.GenerationID ||
		config.RenderGeneration != owner.RenderGeneration ||
		config.BaselineRevisionId != owner.BaselineRevisionID ||
		config.BaselineHeadRevision != owner.BaselineHeadRevision {
		return nil, configSnapshotGuardConflict()
	}
	input, found, err := blueprints.ReadDesiredInputRevision(
		ctx,
		producer.sources,
		owner.EnvironmentID,
		owner.BaselineRevisionID,
		read.ReadRevision,
	)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, configSnapshotGuardConflict()
	}
	projection, found, err := blueprints.ReadEffectiveProjectionRevision(
		ctx,
		producer.sources,
		owner.EnvironmentID,
		owner.BaselineRevisionID,
		read.ReadRevision,
	)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, configSnapshotGuardConflict()
	}
	files, err := BuildConfigRestoreFileContext(config.Files.VolumeRoot, projection.Record)
	if err != nil || !proto.Equal(files, config.Files) {
		return nil, configSnapshotGuardConflict()
	}
	identities, found, err := blueprints.ReadOwnedIdentitiesRevision(
		ctx,
		producer.sources,
		owner.EnvironmentID,
		owner.BaselineRevisionID,
		read.ReadRevision,
	)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, configSnapshotGuardConflict()
	}
	publication, err := BuildConfigRestorePublication(
		ctx,
		generation,
		input.Record,
		projection.Record,
		identities.Record,
		pointID,
		ids.DeriveAt(ids.KindPlan, task.CreatedAt, binding.Attempt.WorkItemID, "config-restore-projection"),
	)
	if err != nil {
		return nil, err
	}
	publication.generation, publication.createdAt = generation, task.CreatedAt
	publication.filePlan, err = backupconfigmaterialization.NewPlan(ctx, config, publication.fileEntries)
	if err != nil {
		publication.Clear()
		return nil, err
	}
	publication.fileStepID = step.StepId
	return publication, nil
}
