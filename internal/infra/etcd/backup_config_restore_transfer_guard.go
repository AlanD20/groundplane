package etcd

import (
	"context"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/backupconfigtransfer"

	"github.com/AlanD20/groundplane/internal/infra/etcd/backupconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

// configTransferGuard already fences the immutable procedure at this same
// revision. This second check binds staging to its allocated generations; it
// does not admit a Restore or manufacture a generation from a transfer id.
func (repository *BackupRuntimeRepository) configRestoreTransferGuard(
	ctx context.Context,
	owner backupconfiguration.ConfigRestoreTransferOwner,
	revision int64,
) ([]etcdstore.Condition, error) {
	if backupconfiguration.ValidateConfigRestoreTransferOwner(owner) != nil || revision <= 0 {
		return nil, configTransferAuthorityConflict()
	}
	key := backupruntime.BackupExecutionPlanKey(owner.Transfer.Binding.TaskID)
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{key}, Revision: revision})
	if err != nil {
		return nil, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 || read.Values[0] == nil {
		return nil, configTransferAuthorityConflict()
	}
	defer etcdstore.ClearValues(read.Values)
	value := read.Values[0]
	plan, err := backupruntime.DecodeBackupExecutionPlan(value.Value)
	if err != nil || value.Key != key || value.Version != 1 || value.ModRevision <= 0 {
		return nil, configTransferAuthorityConflict()
	}
	for _, step := range plan.Steps {
		authority := step.GetBackupStep()
		if authority == nil {
			continue
		}
		if authority.StepId != owner.Transfer.Binding.StepID ||
			authority.ExecutionId != owner.Transfer.Binding.ExecutionID {
			continue
		}
		config := authority.GetRestore().GetConfig()
		if config == nil || config.RestoreGenerationId != owner.GenerationID ||
			config.DestinationEnvironmentId != owner.EnvironmentID ||
			config.RenderGeneration != owner.RenderGeneration ||
			config.BaselineRevisionId != owner.BaselineRevisionID || config.BaselineHeadRevision != owner.BaselineHeadRevision {
			return nil, configTransferAuthorityConflict()
		}
		contentSHA, err := backupconfigtransfer.ContentSHA256(config.GetExpectedArchive().GetContent())
		evidence := authority.GetRestore().GetExpectedEvidence()
		if err != nil || hex.EncodeToString(contentSHA) != owner.ContentSHA256 ||
			evidence.GetSourceSizeBytes() != owner.SourceSizeBytes || hex.EncodeToString(evidence.GetSourceSha256()) != owner.SourceSHA256 {
			return nil, configTransferAuthorityConflict()
		}
		return nil, nil
	}
	return nil, configTransferAuthorityConflict()
}
