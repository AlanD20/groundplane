package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// EnvironmentBlueprintBackupPolicySnapshot is a fixed-revision, public-safe
// authoring view. ConnectorFound distinguishes a deleted retained Connector
// from an unconfigured policy without exposing the stable id in authored YAML.
type EnvironmentBlueprintBackupPolicySnapshot struct {
	Policy         BackupPolicyRecord
	Sources        []BackupSourceRecord
	ConnectorName  string
	ConnectorFound bool
	Found          bool
}

func (repository *BackupPolicyRepository) GetEnvironmentBlueprintBackupPolicySnapshot(
	ctx context.Context,
	environmentID string,
	revision int64,
) (EnvironmentBlueprintBackupPolicySnapshot, error) {
	if err := validateContext(ctx); err != nil {
		return EnvironmentBlueprintBackupPolicySnapshot{}, err
	}
	if ids.Validate(ids.KindEnvironment, environmentID) != nil || revision < 0 {
		return EnvironmentBlueprintBackupPolicySnapshot{}, errs.New(
			errs.KindValidationFailed, "Blueprint Backup snapshot identity is invalid",
		)
	}
	base, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys: []string{backupPolicyKey(environmentID)}, Revision: revision,
	})
	if err != nil {
		return EnvironmentBlueprintBackupPolicySnapshot{}, err
	}
	if base == nil || base.ReadRevision <= 0 || len(base.Values) != 1 {
		return EnvironmentBlueprintBackupPolicySnapshot{}, errs.New(
			errs.KindInternal, "Blueprint Backup snapshot read is incomplete",
		)
	}
	defer clearKeyValues(base.Values)
	if base.Values[0] == nil {
		return EnvironmentBlueprintBackupPolicySnapshot{}, nil
	}
	policy, err := decodeBackupPolicyRecord(base.Values[0].Value)
	if err != nil || policy.EnvironmentID != environmentID {
		return EnvironmentBlueprintBackupPolicySnapshot{}, corruptRecord()
	}
	keys := make([]string, 0, len(policy.SourceIDs)*3+2)
	for _, sourceID := range policy.SourceIDs {
		keys = append(keys, backupSourceKey(sourceID), backupSourceEnvironmentKey(environmentID, sourceID))
	}
	if policy.ConnectorID != "" {
		keys = append(keys, connectorRecordKey(policy.ConnectorID), connectorEnvironmentKey(environmentID, policy.ConnectorID))
	}
	support := &GetManyResult{ReadRevision: base.ReadRevision}
	if len(keys) != 0 {
		support, err = repository.store.GetMany(ctx, GetManyRequest{Keys: keys, Revision: base.ReadRevision})
		if err != nil {
			return EnvironmentBlueprintBackupPolicySnapshot{}, err
		}
		if support == nil || support.ReadRevision != base.ReadRevision || len(support.Values) != len(keys) {
			return EnvironmentBlueprintBackupPolicySnapshot{}, errs.New(
				errs.KindInternal, "Blueprint Backup snapshot support read is incomplete",
			)
		}
	}
	defer clearKeyValues(support.Values)
	snapshot := EnvironmentBlueprintBackupPolicySnapshot{
		Policy: policy, Sources: make([]BackupSourceRecord, len(policy.SourceIDs)), Found: true,
	}
	offset := 0
	selections := make([]BackupPolicySourceSelection, len(policy.SourceIDs))
	for index, sourceID := range policy.SourceIDs {
		primary, owner := support.Values[offset], support.Values[offset+1]
		offset += 2
		if primary == nil || owner == nil || string(owner.Value) != sourceID {
			return EnvironmentBlueprintBackupPolicySnapshot{}, corruptRecord()
		}
		source, decodeErr := decodeBackupSourceRecord(primary.Value)
		if decodeErr != nil || source.ID != sourceID || source.EnvironmentID != environmentID {
			return EnvironmentBlueprintBackupPolicySnapshot{}, corruptRecord()
		}
		snapshot.Sources[index] = source
		selections[index] = BackupPolicySourceSelection{Kind: source.Kind, TargetID: source.TargetID}
	}
	if err := validateBackupPolicyReplacementInput(ctx, BackupPolicyReplacementInput{
		EnvironmentID: environmentID, Enabled: policy.Enabled, Frequency: policy.Frequency,
		Keep: policy.Keep, Encryption: policy.Encryption, ConnectorID: policy.ConnectorID,
		Sources: selections,
	}); err != nil {
		return EnvironmentBlueprintBackupPolicySnapshot{}, corruptRecord()
	}
	if policy.ConnectorID == "" {
		return snapshot, nil
	}
	primary, owner := support.Values[offset], support.Values[offset+1]
	if primary == nil {
		if owner != nil || policy.Enabled {
			return EnvironmentBlueprintBackupPolicySnapshot{}, corruptConnectorRecord()
		}
		return snapshot, nil
	}
	record, err := decodeConnectorRecord(primary.Value)
	if err != nil || record.Connector.ID != policy.ConnectorID ||
		record.Connector.EnvironmentID != environmentID || owner == nil || string(owner.Value) != policy.ConnectorID {
		return EnvironmentBlueprintBackupPolicySnapshot{}, corruptConnectorRecord()
	}
	snapshot.ConnectorName = record.Connector.Name
	snapshot.ConnectorFound = true
	return snapshot, nil
}
