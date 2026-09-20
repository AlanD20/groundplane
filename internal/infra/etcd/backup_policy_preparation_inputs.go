package etcd

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	backuppolicy "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
	"time"
)

func validateBackupPolicyReplacementInput(
	ctx context.Context,
	input BackupPolicyReplacementInput,
) error {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return err
	}
	if err := recordcodec.ValidateID(ids.KindEnvironment, input.EnvironmentID); err != nil {
		return err
	}
	if len(input.Sources) > backuppolicy.MaximumBackupPolicySources {
		return errs.New(
			errs.KindValidationFailed,
			"backup policy may select at most 12 sources",
		)
	}
	configured := input.Frequency != "" || input.Keep != 0 || input.Encryption != "" ||
		input.ConnectorID != "" || len(input.Sources) != 0
	if input.Enabled || configured {
		if input.Frequency == "" || input.Keep <= 0 || input.Keep > backuppolicy.MaximumBackupPolicyKeep ||
			(input.Encryption != "age" && input.Encryption != "none") ||
			input.Enabled && (len(input.Sources) == 0 || input.ConnectorID == "") {
			return errs.New(errs.KindValidationFailed, "configured backup policy is incomplete")
		}
		if err := backuppolicy.ValidateFrequency(input.Frequency); err != nil {
			return err
		}
		if err := recordcodec.ValidateID(ids.KindConnector, input.ConnectorID); err != nil {
			return err
		}
	}
	seen := make(map[struct {
		kind     core.BackupSourceKind
		targetID string
	}]struct{}, len(input.Sources))
	for _, source := range input.Sources {
		probe := backuppolicy.BackupSourceRecord{
			ID:            ids.New(ids.KindBackupSource),
			EnvironmentID: input.EnvironmentID,
			Kind:          source.Kind,
			TargetID:      source.TargetID,
			CreatedAt:     time.Now().UTC(),
		}
		if err := backuppolicy.ValidateBackupSourceRecord(probe); err != nil {
			return err
		}
		identity := struct {
			kind     core.BackupSourceKind
			targetID string
		}{kind: source.Kind, targetID: source.TargetID}
		if _, duplicate := seen[identity]; duplicate {
			return errs.New(errs.KindValidationFailed, "backup policy source identities must be unique")
		}
		seen[identity] = struct{}{}
		if source.Kind == core.BackupSourceConfig && input.Encryption != "age" {
			return errs.New(errs.KindValidationFailed, "config backup source requires age encryption")
		}
	}
	return nil
}

func (repository *BackupPolicyRepository) validateBackupPolicySelectionTarget(
	ctx context.Context,
	environmentID string,
	selection BackupPolicySourceSelection,
) error {
	if selection.Kind == core.BackupSourceConfig {
		return nil
	}
	if selection.Kind == core.BackupSourceVolume {
		evidence, err := loadBackupVolumeProjectionEvidence(
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
		deletionTombstoneKey(string(selection.Kind), selection.TargetID),
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
