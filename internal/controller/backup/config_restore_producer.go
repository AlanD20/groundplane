package backup

import (
	"context"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/backupconfigmaterialization"
	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
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
	if ctx == nil || producer == nil || backupconfiguration.ValidateConfigRestoreTransferOwner(owner) != nil {
		return nil, configSnapshotInvalid()
	}
	generation, err := OpenConfigRestoreGeneration(ctx, owner, expected, producer.publication, producer.protector)
	if err != nil {
		return nil, err
	}
	key := taskjournal.TaskStorageKey(owner.Transfer.Binding.TaskID)
	planKey := backupruntime.BackupExecutionPlanKey(owner.Transfer.Binding.TaskID)
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
	task, err := etcd.DecodeTaskRecord(read.Values[0].Value)
	if err != nil || task.ID != owner.Transfer.Binding.TaskID || task.Type != taskjournal.TaskRestore ||
		task.Status != taskjournal.TaskStatusRunning || task.Target != owner.EnvironmentID || task.CreatedAt.IsZero() {
		return nil, configSnapshotGuardConflict()
	}
	sealed, err := backupruntime.DecodeBackupExecutionPlan(read.Values[1].Value)
	if err != nil || sealed.PlanId != task.PlanID || hex.EncodeToString(sealed.PlanHash) != task.PlanHash ||
		len(sealed.Steps) != 1 {
		return nil, configSnapshotGuardConflict()
	}
	step := sealed.Steps[0].GetBackupStep()
	config := step.GetRestore().GetConfig()
	if config == nil || step.StepId != owner.Transfer.Binding.StepID ||
		step.ExecutionId != owner.Transfer.Binding.ExecutionID ||
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
		task.PlanID,
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
