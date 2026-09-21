package etcd

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	backuppolicy "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	deletions "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	environmentqueries "github.com/AlanD20/groundplane/internal/infra/etcd/environmentqueries"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *BackupPolicyRepository) validateBackupPolicySelectionTarget(
	ctx context.Context,
	environmentID string,
	selection backuppolicy.BackupPolicySourceSelection,
) error {
	if selection.Kind == core.BackupSourceConfig {
		return nil
	}
	if selection.Kind == core.BackupSourceVolume {
		evidence, err := environmentqueries.LoadBackupVolumeProjectionEvidence(
			ctx, repository.store, environmentID, selection.TargetID, 0,
		)
		if err != nil {
			return err
		}
		if evidence.Projection.Record.EnvironmentID != environmentID {
			return errs.New(errs.KindScopeUnauthorized, "backup policy source belongs to another environment")
		}
		return nil
	}
	primaryKey := ""
	ownerKey := ""
	notFound := errs.KindAttachNotFound
	if selection.Kind == core.BackupSourceAttach {
		primaryKey = attachrecord.AttachKey(selection.TargetID)
		ownerKey = attachrecord.AttachOwnerKey(environmentID, selection.TargetID)
	}
	result, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{
		primaryKey,
		ownerKey,
		deletions.TombstoneKey(string(selection.Kind), selection.TargetID),
	}})
	if err != nil {
		return err
	}
	if result == nil || len(result.Values) != 3 {
		return errs.New(errs.KindInternal, "backup policy source target read is incomplete")
	}
	if result.Values[0] == nil {
		return errs.New(notFound, string(selection.Kind)+" was not found")
	}
	ownerEnvironmentID := ""
	if selection.Kind == core.BackupSourceAttach {
		record, decodeErr := attachrecord.DecodeAttachRecord(result.Values[0].Value)
		if decodeErr != nil || record.ID != selection.TargetID {
			return recordcodec.CorruptRecord()
		}
		if !record.OwnsCredential() {
			return errs.New(errs.KindValidationFailed, "backup policy requires a credential-owning Attach")
		}
		ownerEnvironmentID = record.EnvironmentID
	}
	if ownerEnvironmentID != environmentID {
		return errs.New(errs.KindScopeUnauthorized, "backup policy source belongs to another environment")
	}
	if result.Values[1] == nil || result.Values[1].Key != ownerKey ||
		string(result.Values[1].Value) != selection.TargetID {
		return recordcodec.CorruptRecord()
	}
	if result.Values[2] != nil {
		return errs.New(errs.KindResourceInUse, "backup policy source deletion is in progress")
	}
	return nil
}
