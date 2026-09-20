package etcd

import (
	"context"
	connectorrecord "github.com/AlanD20/groundplane/internal/infra/etcd/connectors"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"time"

	"github.com/AlanD20/groundplane/internal/common/backupschedule"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// BackupPolicySourceProjection is the stable public-safe identity of one
// selected source. Mutable labels are deliberately absent.
type BackupPolicySourceProjection struct {
	ID       string
	Kind     core.BackupSourceKind
	TargetID string
}

// BackupPolicyProjection is the complete read model for the Environment
// singleton. An Environment without a stored singleton projects as disabled
// and unconfigured with a non-nil empty Sources slice.
type BackupPolicyProjection struct {
	EnvironmentID string
	Enabled       bool
	Frequency     string
	Keep          int64
	Encryption    string
	ConnectorID   string
	Sources       []BackupPolicySourceProjection
	AgeRecipient  string
	KeyEra        int
	KeyCreatedAt  time.Time
	KeyRotatedAt  time.Time
	NextRunAt     *time.Time
}

func (repository *BackupPolicyRepository) GetBackupPolicyProjection(
	ctx context.Context,
	environmentID string,
) (BackupPolicyProjection, error) {
	if err := validateContext(ctx); err != nil {
		return BackupPolicyProjection{}, err
	}
	if err := recordcodec.ValidateID(ids.KindEnvironment, environmentID); err != nil {
		return BackupPolicyProjection{}, err
	}
	base, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{
		environmentKey(environmentID),
		backupPolicyKey(environmentID),
		backupKeyKey(environmentID),
		backupKeyValueKey(environmentID),
		environmentCoordinationKey(environmentID),
	}})
	if err != nil {
		return BackupPolicyProjection{}, err
	}
	if base == nil || len(base.Values) != 5 {
		return BackupPolicyProjection{}, errs.New(errs.KindInternal, "backup policy projection read is incomplete")
	}
	if base.Values[0] == nil {
		return BackupPolicyProjection{}, errs.New(errs.KindEnvironmentNotFound, "environment was not found")
	}
	environment, err := decodeEnvironment(base.Values[0].Value)
	if err != nil || environment.ID != environmentID {
		return BackupPolicyProjection{}, recordcodec.CorruptRecord()
	}

	var policy *BackupPolicyRecord
	if base.Values[1] != nil {
		decoded, decodeErr := decodeBackupPolicyRecord(base.Values[1].Value)
		if decodeErr != nil || decoded.EnvironmentID != environmentID {
			return BackupPolicyProjection{}, recordcodec.CorruptRecord()
		}
		policy = &decoded
	}
	coordination := EnvironmentCoordinationRecord{EnvironmentID: environmentID}
	if base.Values[4] != nil {
		decoded, decodeErr := decodeEnvironmentCoordinationRecord(base.Values[4].Value)
		if decodeErr != nil || decoded.EnvironmentID != environmentID {
			return BackupPolicyProjection{}, corruptEnvironmentCoordination()
		}
		coordination = decoded
	} else if policy != nil {
		return BackupPolicyProjection{}, corruptEnvironmentCoordination()
	}
	key, err := decodeBackupPolicyProjectionKey(environmentID, base.Values[2], base.Values[3])
	if err != nil {
		return BackupPolicyProjection{}, err
	}
	if policy == nil && key != nil {
		return BackupPolicyProjection{}, corruptBackupKey()
	}

	keys := []string{projectKey(environment.ProjectID)}
	if policy != nil {
		for _, sourceID := range policy.SourceIDs {
			keys = append(keys,
				backupSourceKey(sourceID),
				backupSourceEnvironmentKey(environmentID, sourceID),
			)
		}
		if policy.Enabled {
			keys = append(keys,
				connectorrecord.RecordKey(policy.ConnectorID),
				connectorEnvironmentKey(environmentID, policy.ConnectorID),
				backupPolicyConnectorReferenceKey(policy.ConnectorID, environmentID),
				deletionTombstoneKey(string(DeletionTargetConnector), policy.ConnectorID),
			)
		} else if policy.ConnectorID != "" {
			keys = append(keys, backupPolicyConnectorReferenceKey(policy.ConnectorID, environmentID))
		}
	}
	support, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: base.ReadRevision})
	if err != nil {
		return BackupPolicyProjection{}, err
	}
	if support == nil || support.ReadRevision != base.ReadRevision || len(support.Values) != len(keys) ||
		support.Values[0] == nil {
		return BackupPolicyProjection{}, recordcodec.CorruptRecord()
	}
	project, err := decodeProject(support.Values[0].Value)
	if err != nil || project.ID != environment.ProjectID {
		return BackupPolicyProjection{}, recordcodec.CorruptRecord()
	}
	if project.Kind != ProjectKindTenant || project.TenantID == "" {
		return BackupPolicyProjection{}, errs.New(
			errs.KindValidationFailed,
			"backing environments cannot own backup policies",
		)
	}
	if policy == nil {
		if coordination.CurrentBackupScheduleState != nil {
			return BackupPolicyProjection{}, corruptEnvironmentCoordination()
		}
		return BackupPolicyProjection{EnvironmentID: environmentID, Sources: []BackupPolicySourceProjection{}}, nil
	}
	if policy.Frequency != "" {
		if err := validateBackupPolicyFrequency(policy.Frequency); err != nil {
			return BackupPolicyProjection{}, recordcodec.CorruptRecord()
		}
	}

	projection := BackupPolicyProjection{
		EnvironmentID: environmentID,
		Enabled:       policy.Enabled,
		Frequency:     policy.Frequency,
		Keep:          policy.Keep,
		Encryption:    policy.Encryption,
		ConnectorID:   policy.ConnectorID,
		Sources:       make([]BackupPolicySourceProjection, 0, len(policy.SourceIDs)),
	}
	if policy.Enabled {
		state := coordination.CurrentBackupScheduleState
		digest, digestErr := backupPolicyScheduleDigest(*policy)
		if digestErr != nil || state == nil || state.PolicyDigest != digest ||
			state.Frequency != policy.Frequency {
			return BackupPolicyProjection{}, corruptEnvironmentCoordination()
		}
		boundary := state.LastEvaluatedAt
		if coordination.ScheduleClockFloor.After(boundary) {
			boundary = coordination.ScheduleClockFloor
		}
		schedule, parseErr := backupschedule.Parse(policy.Frequency)
		if parseErr != nil {
			return BackupPolicyProjection{}, corruptEnvironmentCoordination()
		}
		next, nextErr := schedule.NextOccurrence(boundary)
		if nextErr != nil {
			return BackupPolicyProjection{}, corruptEnvironmentCoordination()
		}
		projection.NextRunAt = &next
	} else if coordination.CurrentBackupScheduleState != nil {
		return BackupPolicyProjection{}, corruptEnvironmentCoordination()
	}
	seen := make(map[struct {
		kind     core.BackupSourceKind
		targetID string
	}]struct{}, len(policy.SourceIDs))
	identityKeys := make([]string, 0, len(policy.SourceIDs))
	offset := 1
	for _, sourceID := range policy.SourceIDs {
		primary := support.Values[offset]
		owner := support.Values[offset+1]
		offset += 2
		if primary == nil || owner == nil || owner.Key != backupSourceEnvironmentKey(environmentID, sourceID) ||
			string(owner.Value) != sourceID {
			return BackupPolicyProjection{}, recordcodec.CorruptRecord()
		}
		source, decodeErr := decodeBackupSourceRecord(primary.Value)
		if decodeErr != nil || source.ID != sourceID || source.EnvironmentID != environmentID {
			return BackupPolicyProjection{}, recordcodec.CorruptRecord()
		}
		identity := struct {
			kind     core.BackupSourceKind
			targetID string
		}{kind: source.Kind, targetID: source.TargetID}
		if _, duplicate := seen[identity]; duplicate {
			return BackupPolicyProjection{}, recordcodec.CorruptRecord()
		}
		seen[identity] = struct{}{}
		identityKeys = append(identityKeys, backupSourceIdentityKey(environmentID, source.Kind, source.TargetID))
		if source.Kind == core.BackupSourceConfig && policy.Encryption != "age" {
			return BackupPolicyProjection{}, recordcodec.CorruptRecord()
		}
		projection.Sources = append(projection.Sources, BackupPolicySourceProjection{
			ID: source.ID, Kind: source.Kind, TargetID: source.TargetID,
		})
	}
	if len(identityKeys) > 0 {
		identityIndexes, readErr := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
			Keys: identityKeys, Revision: base.ReadRevision,
		})
		if readErr != nil {
			return BackupPolicyProjection{}, readErr
		}
		if identityIndexes == nil || identityIndexes.ReadRevision != base.ReadRevision ||
			len(identityIndexes.Values) != len(identityKeys) {
			return BackupPolicyProjection{}, recordcodec.CorruptRecord()
		}
		for index, source := range projection.Sources {
			identityIndex := identityIndexes.Values[index]
			if identityIndex == nil || identityIndex.Key != identityKeys[index] ||
				string(identityIndex.Value) != source.ID {
				return BackupPolicyProjection{}, recordcodec.CorruptRecord()
			}
		}
	}
	if policy.Enabled {
		connectorValue := support.Values[offset]
		ownerIndex := support.Values[offset+1]
		reference := support.Values[offset+2]
		deletionFence := support.Values[offset+3]
		if connectorValue == nil || ownerIndex == nil || reference == nil {
			return BackupPolicyProjection{}, connectorrecord.CorruptRecord()
		}
		connector, decodeErr := connectorrecord.DecodeRecord(connectorValue.Value)
		if decodeErr != nil || connector.Connector.ID != policy.ConnectorID ||
			connector.Connector.EnvironmentID != environmentID ||
			ownerIndex.Key != connectorEnvironmentKey(environmentID, policy.ConnectorID) ||
			string(ownerIndex.Value) != policy.ConnectorID ||
			reference.Key != backupPolicyConnectorReferenceKey(policy.ConnectorID, environmentID) ||
			string(reference.Value) != environmentID {
			return BackupPolicyProjection{}, connectorrecord.CorruptRecord()
		}
		if deletionFence != nil {
			return BackupPolicyProjection{}, errs.New(
				errs.KindResourceInUse,
				"backup policy connector deletion is in progress",
			)
		}
	} else if policy.ConnectorID != "" {
		if reference := support.Values[offset]; reference != nil {
			return BackupPolicyProjection{}, recordcodec.CorruptRecord()
		}
	}
	if key != nil {
		projection.AgeRecipient = key.Recipient
		projection.KeyEra = key.KeyEra
		projection.KeyCreatedAt = key.CreatedAt
		projection.KeyRotatedAt = key.RotatedAt
	} else if policy.Enabled && policy.Encryption == "age" {
		return BackupPolicyProjection{}, corruptBackupKey()
	}
	return projection, nil
}

func decodeBackupPolicyProjectionKey(
	environmentID string,
	recordValue *etcdstore.KeyValue,
	encryptedValue *etcdstore.KeyValue,
) (*BackupKeyRecord, error) {
	if recordValue == nil && encryptedValue == nil {
		return nil, nil
	}
	if recordValue == nil || encryptedValue == nil {
		return nil, corruptBackupKey()
	}
	record, err := decodeBackupKeyRecord(recordValue.Value)
	if err != nil {
		return nil, corruptBackupKey()
	}
	encrypted, err := decodeBackupKeyEncryptedValue(encryptedValue.Value)
	if err != nil {
		return nil, corruptBackupKey()
	}
	defer clear(encrypted.Ciphertext)
	if record.EnvironmentID != environmentID || encrypted.EnvironmentID != environmentID ||
		record.KeyEra != encrypted.KeyEra {
		return nil, corruptBackupKey()
	}
	return &record, nil
}

func cloneBackupPolicyProjection(projection BackupPolicyProjection) BackupPolicyProjection {
	projection.Sources = append([]BackupPolicySourceProjection(nil), projection.Sources...)
	if projection.NextRunAt != nil {
		next := *projection.NextRunAt
		projection.NextRunAt = &next
	}
	if projection.Sources == nil {
		projection.Sources = []BackupPolicySourceProjection{}
	}
	return projection
}
