package etcd

import (
	"context"
	"encoding/hex"
	"github.com/AlanD20/groundplane/internal/common/backupconfigtransfer"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupwork"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Bind protected Restore records to the captured destination generation and
// archive. Transfer identifiers never allocate a new generation.
func (repository *BackupRuntimeRepository) configRestoreTransferGuard(ctx context.Context,
	owner backupconfiguration.ConfigRestoreTransferOwner, revision int64,
) ([]etcdstore.Condition, error) {
	return repository.configRestoreExecutionGuard(ctx, owner, revision, nil)
}

func (repository *BackupRuntimeRepository) configRestoreExecutionGuard(ctx context.Context,
	owner backupconfiguration.ConfigRestoreTransferOwner, revision int64, recovery *workexecution.Attempt,
) ([]etcdstore.Condition, error) {
	if backupconfiguration.ValidateConfigRestoreTransferOwner(owner) != nil || revision <= 0 {
		return nil, configTransferAuthorityConflict()
	}
	inputs, err := backupwork.NewInputRepository(repository.store)
	if err != nil {
		return nil, err
	}
	queue, err := workqueue.NewRepository(repository.store)
	if err != nil {
		return nil, err
	}
	var input *agentpb.BackupWorkInput
	var conditions []etcdstore.Condition
	if recovery != nil {
		assignment, guards, loadErr := inputs.LoadConfigRecoveryTransfer(
			ctx,
			queue,
			owner.Transfer.Binding,
			*recovery,
			revision,
		)
		if loadErr != nil {
			return nil, loadErr
		}
		input, conditions = assignment.Input, guards
	} else {
		assignment, guards, loadErr := inputs.LoadConfigTransfer(ctx, queue, owner.Transfer.Binding, revision)
		if loadErr != nil {
			return nil, loadErr
		}
		input, conditions = assignment.Input, guards
	}
	authority := input.Steps[owner.Transfer.Binding.SourceOrdinal]
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
	return conditions, nil
}
