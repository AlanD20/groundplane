package etcd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/infra/etcd/backupconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentfence"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"google.golang.org/protobuf/proto"
)

type configRestorePublicationAuthority struct {
	current                etcdstore.Versioned[backupruntime.BackupRestoreRecord]
	root                   blueprints.EnvironmentBlueprintSeal
	revision, headRevision int64
	head                   string
	conditions             []etcdstore.Condition
	fence                  environmentfence.Evidence
}

// Every publication batch closes the same immutable source, native assignment,
// operation lock and predecessor. After the head switch, only its exact receipt
// may replace the predecessor fence; an unrelated later head is never accepted.
func (repository *BackupRuntimeRepository) loadConfigRestorePublication(ctx context.Context,
	generation etcdstore.Versioned[backupconfiguration.ConfigRestoreGenerationRecord],
) (configRestorePublicationAuthority, error) {
	var zero configRestorePublicationAuthority
	owner := generation.Record.Owner
	if ctx == nil || repository == nil || generation.Revision <= 0 ||
		backupconfiguration.ValidateConfigRestoreTransferOwner(owner) != nil {
		return zero, configTransferAuthorityConflict()
	}
	taskID := owner.Transfer.Binding.TaskID
	memberKey, err := backupruntime.BackupRestoreEnvironmentIndexKey(owner.EnvironmentID, taskID)
	if err != nil {
		return zero, err
	}
	keys := []string{backupruntime.BackupRestoreKey(taskID), memberKey, backupruntime.BackupExecutionPlanKey(taskID),
		backupconfiguration.ConfigRestoreGenerationKey(
			owner,
		), blueprints.EnvironmentBlueprintRootKey(owner.EnvironmentID, taskID),
		blueprints.EnvironmentBlueprintHeadKey(owner.EnvironmentID),
		blueprints.EnvironmentBlueprintEffectiveProjectionKey(owner.EnvironmentID, taskID),
		blueprints.EnvironmentBlueprintOwnedIdentitiesKey(owner.EnvironmentID, taskID)}
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys})
	if err != nil {
		return zero, err
	}
	if read == nil || read.ReadRevision <= 0 || len(read.Values) != len(keys) {
		return zero, configTransferAuthorityConflict()
	}
	defer etcdstore.ClearValues(read.Values)
	for index, value := range read.Values {
		if value == nil {
			if index < 6 {
				return zero, configTransferAuthorityConflict()
			}
			continue
		}
		if value.Key != keys[index] || value.ModRevision <= 0 || value.ModRevision > read.ReadRevision {
			return zero, configTransferAuthorityConflict()
		}
	}
	current, err := backupruntime.DecodeBackupRestoreRecord(read.Values[0].Value)
	if err != nil || current.TaskID != taskID || current.EnvironmentID != owner.EnvironmentID ||
		current.Point.SourceKind != backupruntime.BackupRuntimeSourceConfig || current.ConfigProgress.RevisionRootRevision <= 0 ||
		read.Values[1].Version != 1 || string(read.Values[1].Value) != taskID || read.Values[1].ModRevision > read.Values[0].ModRevision {
		return zero, configTransferAuthorityConflict()
	}
	switch current.State {
	case backupruntime.BackupRestoreStaged, backupruntime.BackupRestoreApplyingDeletes,
		backupruntime.BackupRestoreApplyingUpserts, backupruntime.BackupRestoreCanonicalComplete,
		backupruntime.BackupRestoreMaterializing, backupruntime.BackupRestoreVerified, backupruntime.BackupRestoreCompleted:
	default:
		return zero, configTransferAuthorityConflict()
	}
	sealed, err := backupruntime.DecodeBackupExecutionPlan(read.Values[2].Value)
	if err != nil || read.Values[2].Version != 1 ||
		backupruntime.ValidateConfigRestoreExecutionPlan(current, sealed) != nil {
		return zero, configTransferAuthorityConflict()
	}
	source, err := backupconfiguration.DecodeConfigRestoreGeneration(read.Values[3].Value)
	if err != nil || read.Values[3].Version != 1 || read.Values[3].ModRevision != generation.Revision ||
		source.Owner != owner || !proto.Equal(source.Completed, generation.Record.Completed) {
		return zero, configTransferAuthorityConflict()
	}
	root, err := blueprints.DecodeEnvironmentBlueprintSeal(read.Values[4].Value)
	rootSHA := sha256.Sum256(read.Values[4].Value)
	progress := current.ConfigProgress
	if err != nil || read.Values[4].Version != 1 || read.Values[4].ModRevision != progress.RevisionRootRevision ||
		hex.EncodeToString(rootSHA[:]) != progress.RevisionRootSHA256 || root.EnvironmentID != owner.EnvironmentID ||
		root.RevisionID != taskID || root.RenderGeneration != owner.RenderGeneration ||
		root.BaselineHeadRevision != owner.BaselineHeadRevision {
		return zero, configTransferAuthorityConflict()
	}
	head, err := idempotency.DecodeTaskReference(read.Values[5].Value)
	if err != nil {
		return zero, configTransferAuthorityConflict()
	}
	if head == owner.BaselineRevisionID {
		if read.Values[5].ModRevision != owner.BaselineHeadRevision || progress.PublishedHeadRevision != 0 {
			return zero, configTransferAuthorityConflict()
		}
	} else if head == taskID {
		if !current.MutationStarted || progress.DeleteEntryOrdinal != progress.DeleteEntryCount ||
			progress.UpsertEntryOrdinal != current.Point.ConfigArchive.EntryCount || read.Values[5].ModRevision <= progress.RevisionRootRevision ||
			(progress.PublishedHeadRevision == 0 && read.Values[5].ModRevision != read.Values[0].ModRevision) ||
			(progress.PublishedHeadRevision != 0 && read.Values[5].ModRevision != progress.PublishedHeadRevision) {
			return zero, configTransferAuthorityConflict()
		}
	} else {
		return zero, configTransferAuthorityConflict()
	}
	for index, expected := range []string{progress.ProjectionSHA256, progress.IdentitiesSHA256} {
		value := read.Values[index+6]
		if value == nil {
			if current.MutationStarted {
				return zero, configTransferAuthorityConflict()
			}
			continue
		}
		digest := sha256.Sum256(value.Value)
		if value.Version != 1 || hex.EncodeToString(digest[:]) != expected {
			return zero, configTransferAuthorityConflict()
		}
	}
	conditions, err := repository.configTransferGuard(ctx, owner.Transfer, read.ReadRevision)
	if err != nil {
		return zero, err
	}
	more, err := repository.configRestoreTransferGuard(ctx, owner, read.ReadRevision)
	if err != nil {
		return zero, err
	}
	conditions = append(conditions, more...)
	fence, err := environmentfence.LoadOwned(
		ctx,
		repository.store,
		owner.EnvironmentID,
		read.ReadRevision,
		environmentfence.Owner{
			Kind:        backupruntime.BackupOperationRestore,
			OperationID: current.OperationID,
			TaskID:      taskID,
		},
	)
	if err != nil {
		return zero, err
	}
	for index, key := range keys {
		revision := revisionOf(read.Values[index])
		found := false
		for _, condition := range conditions {
			if condition.Key != key {
				continue
			}
			if condition.Prefix || condition.ModRevision != revision {
				return zero, configTransferAuthorityConflict()
			}
			found = true
			break
		}
		if !found {
			conditions = append(conditions, etcdstore.Condition{Key: key, ModRevision: revision})
		}
	}
	conditions, err = environmentfence.AppendConditions(conditions, fence)
	if err != nil {
		return zero, err
	}
	return configRestorePublicationAuthority{
		current: etcdstore.Versioned[backupruntime.BackupRestoreRecord]{Record: current,
			Revision: read.Values[0].ModRevision, ReadRevision: read.ReadRevision},
		root:         root,
		revision:     read.ReadRevision,
		head:         head,
		headRevision: read.Values[5].ModRevision,
		conditions:   conditions,
		fence:        fence,
	}, nil
}
