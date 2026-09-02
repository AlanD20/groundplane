package etcd

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *BackupPolicyRepository) loadBlueprintBackupBase(
	ctx context.Context,
	environmentID string,
	revision int64,
	createdAt time.Time,
) (*Versioned[BackupPolicyRecord], Versioned[EnvironmentCoordinationRecord], *VersionedBackupKey, error) {
	result, err := repository.store.GetMany(ctx, GetManyRequest{Keys: []string{
		backupPolicyKey(environmentID), environmentCoordinationKey(environmentID),
		backupKeyKey(environmentID), backupKeyValueKey(environmentID),
	}, Revision: revision})
	if err != nil {
		return nil, Versioned[EnvironmentCoordinationRecord]{}, nil, err
	}
	if result == nil || result.ReadRevision != revision || len(result.Values) != 4 {
		return nil, Versioned[EnvironmentCoordinationRecord]{}, nil, errs.New(
			errs.KindInternal, "Blueprint Backup base read is incomplete",
		)
	}
	defer clearKeyValues(result.Values)
	var current *Versioned[BackupPolicyRecord]
	if result.Values[0] != nil {
		record, decodeErr := decodeBackupPolicyRecord(result.Values[0].Value)
		if decodeErr != nil || record.EnvironmentID != environmentID {
			return nil, Versioned[EnvironmentCoordinationRecord]{}, nil, corruptRecord()
		}
		value := Versioned[BackupPolicyRecord]{
			Record: record, Revision: result.Values[0].ModRevision, ReadRevision: revision,
		}
		current = &value
	}
	coordination := Versioned[EnvironmentCoordinationRecord]{
		Record: EnvironmentCoordinationRecord{
			EnvironmentID: environmentID, ScheduleClockFloor: createdAt,
		},
		ReadRevision: revision,
	}
	if result.Values[1] != nil {
		coordination.Record, err = decodeEnvironmentCoordinationRecord(result.Values[1].Value)
		if err != nil || coordination.Record.EnvironmentID != environmentID {
			return nil, coordination, nil, corruptEnvironmentCoordination()
		}
		coordination.Revision = result.Values[1].ModRevision
	}
	if current == nil {
		if coordination.Record.CurrentBackupScheduleState != nil {
			return nil, coordination, nil, corruptEnvironmentCoordination()
		}
	} else if current.Record.Enabled {
		digest, digestErr := backupPolicyScheduleDigest(current.Record)
		state := coordination.Record.CurrentBackupScheduleState
		if digestErr != nil || state == nil || state.PolicyDigest != digest || state.Frequency != current.Record.Frequency {
			return nil, coordination, nil, corruptEnvironmentCoordination()
		}
	} else if coordination.Record.CurrentBackupScheduleState != nil {
		return nil, coordination, nil, corruptEnvironmentCoordination()
	}
	if (result.Values[2] == nil) != (result.Values[3] == nil) {
		return nil, coordination, nil, corruptRecord()
	}
	var key *VersionedBackupKey
	if result.Values[2] != nil {
		record, recordErr := decodeBackupKeyRecord(result.Values[2].Value)
		encrypted, encryptedErr := decodeBackupKeyEncryptedValue(result.Values[3].Value)
		if recordErr != nil || encryptedErr != nil || record.EnvironmentID != environmentID ||
			encrypted.EnvironmentID != environmentID || record.KeyEra != encrypted.KeyEra {
			clear(encrypted.Ciphertext)
			return nil, coordination, nil, corruptRecord()
		}
		key = &VersionedBackupKey{
			Record: record, Encrypted: encrypted, RecordRevision: result.Values[2].ModRevision,
			EncryptedRevision: result.Values[3].ModRevision, ReadRevision: revision,
		}
	}
	return current, coordination, key, nil
}

func (repository *BackupPolicyRepository) prepareRetainedBlueprintBackupPolicy(
	ctx context.Context,
	state *blueprintBackupPolicyPreparationState,
	revision int64,
) error {
	if state.candidate.Current == nil {
		state.desired = nil
		return nil
	}
	current := state.candidate.Current.Record
	state.candidate.Replacement = current
	var err error
	state.sources, err = repository.loadRetainedBlueprintBackupSources(ctx, current, revision)
	if err != nil {
		return err
	}
	state.desired = &EnvironmentBlueprintBackupPolicy{
		Enabled: current.Enabled, Frequency: current.Frequency, Keep: current.Keep,
		Encryption: current.Encryption, ConnectorID: current.ConnectorID,
		Sources: make([]EnvironmentBlueprintBackupPolicySource, len(state.sources)),
	}
	for index, source := range state.sources {
		state.desired.Sources[index] = EnvironmentBlueprintBackupPolicySource{
			ID: source.record.ID, Kind: source.record.Kind, TargetID: source.record.TargetID,
		}
	}
	if err := validateEnvironmentBlueprintBackupPolicy(state.environmentID, state.desired); err != nil {
		return err
	}
	if current.ConnectorID != "" {
		state.retainedConnectorID = current.ConnectorID
		state.candidate.Connector, state.candidate.ConnectorOwnerIndex, state.connectorNameIndex,
			state.connectorTombstone, err = repository.loadRetainedBlueprintBackupConnector(
			ctx, state.environmentID, current.ConnectorID, current.Enabled, revision,
		)
		if err != nil {
			return err
		}
	}
	state.candidate.ConnectorReferences, err = repository.loadBackupPolicyConnectorReferences(
		ctx, state.candidate, revision,
	)
	return err
}

func (repository *BackupPolicyRepository) loadRetainedBlueprintBackupSources(
	ctx context.Context,
	policy BackupPolicyRecord,
	revision int64,
) ([]blueprintBackupPolicySourceEvidence, error) {
	result := make([]blueprintBackupPolicySourceEvidence, len(policy.SourceIDs))
	for index, sourceID := range policy.SourceIDs {
		values, err := repository.store.GetMany(ctx, GetManyRequest{Keys: []string{
			backupSourceKey(sourceID), backupSourceEnvironmentKey(policy.EnvironmentID, sourceID),
		}, Revision: revision})
		if err != nil {
			return nil, err
		}
		if values == nil || values.ReadRevision != revision || len(values.Values) != 2 ||
			values.Values[0] == nil || values.Values[1] == nil {
			return nil, corruptRecord()
		}
		source, decodeErr := decodeBackupSourceRecord(values.Values[0].Value)
		if decodeErr != nil || source.ID != sourceID || source.EnvironmentID != policy.EnvironmentID ||
			string(values.Values[1].Value) != sourceID {
			clearKeyValues(values.Values)
			return nil, corruptRecord()
		}
		identity, identityErr := repository.store.GetMany(ctx, GetManyRequest{Keys: []string{
			backupSourceIdentityKey(policy.EnvironmentID, source.Kind, source.TargetID),
		}, Revision: revision})
		if identityErr != nil || identity == nil || identity.ReadRevision != revision || len(identity.Values) != 1 ||
			identity.Values[0] == nil || string(identity.Values[0].Value) != sourceID {
			clearKeyValues(values.Values)
			if identity != nil {
				clearKeyValues(identity.Values)
			}
			if identityErr != nil {
				return nil, identityErr
			}
			return nil, corruptRecord()
		}
		result[index] = blueprintBackupPolicySourceEvidence{
			record: source, primary: cloneBackupPolicyEvidenceKeyValue(values.Values[0]),
			environmentIndex: cloneBackupPolicyEvidenceKeyValue(values.Values[1]),
			identityIndex:    cloneBackupPolicyEvidenceKeyValue(identity.Values[0]),
		}
		clearKeyValues(values.Values)
		clearKeyValues(identity.Values)
	}
	return result, nil
}

func (repository *BackupPolicyRepository) loadRetainedBlueprintBackupConnector(
	ctx context.Context,
	environmentID string,
	connectorID string,
	enabled bool,
	revision int64,
) (*Versioned[ConnectorRecord], *KeyValue, *KeyValue, *KeyValue, error) {
	keys := []string{
		connectorRecordKey(connectorID), connectorEnvironmentKey(environmentID, connectorID),
		deletionTombstoneKey(string(DeletionTargetConnector), connectorID),
	}
	result, err := repository.store.GetMany(ctx, GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return nil, nil, nil, nil, err
	}
	if result == nil || result.ReadRevision != revision || len(result.Values) != len(keys) {
		return nil, nil, nil, nil, errs.New(errs.KindInternal, "retained Blueprint Backup Connector read is incomplete")
	}
	defer clearKeyValues(result.Values)
	tombstone := cloneBackupPolicyEvidenceKeyValue(result.Values[2])
	if result.Values[0] == nil {
		if enabled || result.Values[1] != nil {
			return nil, nil, nil, nil, corruptConnectorRecord()
		}
		return nil, nil, nil, tombstone, nil
	}
	record, err := decodeConnectorRecord(result.Values[0].Value)
	if err != nil || record.Connector.ID != connectorID || record.Connector.EnvironmentID != environmentID ||
		result.Values[1] == nil || string(result.Values[1].Value) != connectorID || result.Values[2] != nil {
		return nil, nil, nil, nil, corruptConnectorRecord()
	}
	name, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys: []string{connectorNameKey(environmentID, record.Connector.Name)}, Revision: revision,
	})
	if err != nil {
		return nil, nil, nil, nil, err
	}
	if name == nil || name.ReadRevision != revision || len(name.Values) != 1 || name.Values[0] == nil ||
		string(name.Values[0].Value) != connectorID {
		return nil, nil, nil, nil, corruptConnectorRecord()
	}
	defer clearKeyValues(name.Values)
	return &Versioned[ConnectorRecord]{
		Record: record, Revision: result.Values[0].ModRevision, ReadRevision: revision,
	}, cloneBackupPolicyEvidenceKeyValue(result.Values[1]), cloneBackupPolicyEvidenceKeyValue(name.Values[0]), tombstone, nil
}

func (repository *BackupPolicyRepository) loadBlueprintBackupSources(
	ctx context.Context,
	input EnvironmentBlueprintBackupPolicyInput,
	revision int64,
) ([]blueprintBackupPolicySourceEvidence, error) {
	result := make([]blueprintBackupPolicySourceEvidence, len(input.Sources))
	for index, selection := range input.Sources {
		if err := validateBlueprintBackupTarget(input, selection); err != nil {
			return nil, err
		}
		identityKey := backupSourceIdentityKey(input.EnvironmentID, selection.Kind, selection.TargetID)
		identity, err := repository.store.GetMany(ctx, GetManyRequest{Keys: []string{identityKey}, Revision: revision})
		if err != nil {
			return nil, err
		}
		if identity == nil || identity.ReadRevision != revision || len(identity.Values) != 1 {
			return nil, errs.New(errs.KindInternal, "Blueprint Backup source identity read is incomplete")
		}
		sourceID := selection.CandidateID
		if identity.Values[0] != nil {
			sourceID = string(identity.Values[0].Value)
		}
		if ids.Validate(ids.KindBackupSource, sourceID) != nil {
			return nil, errs.New(errs.KindValidationFailed, "Blueprint Backup source id is invalid")
		}
		record := BackupSourceRecord{
			ID: sourceID, EnvironmentID: input.EnvironmentID, Kind: selection.Kind,
			TargetID: selection.TargetID, CreatedAt: input.CreatedAt,
		}
		item := blueprintBackupPolicySourceEvidence{record: record, identityIndex: cloneBackupPolicyEvidenceKeyValue(identity.Values[0])}
		if identity.Values[0] != nil {
			triples, readErr := repository.store.GetMany(ctx, GetManyRequest{Keys: []string{
				backupSourceKey(sourceID), backupSourceEnvironmentKey(input.EnvironmentID, sourceID),
			}, Revision: revision})
			if readErr != nil {
				return nil, readErr
			}
			if triples == nil || triples.ReadRevision != revision || len(triples.Values) != 2 ||
				triples.Values[0] == nil || triples.Values[1] == nil {
				return nil, corruptRecord()
			}
			stored, decodeErr := decodeBackupSourceRecord(triples.Values[0].Value)
			if decodeErr != nil || stored.ID != sourceID || stored.EnvironmentID != input.EnvironmentID ||
				stored.Kind != selection.Kind || stored.TargetID != selection.TargetID ||
				string(triples.Values[1].Value) != sourceID {
				return nil, corruptRecord()
			}
			item.record = stored
			item.primary = cloneBackupPolicyEvidenceKeyValue(triples.Values[0])
			item.environmentIndex = cloneBackupPolicyEvidenceKeyValue(triples.Values[1])
		}
		if selection.Kind == core.BackupSourceAttach {
			candidate := blueprintBackupAttachCandidate(input.AttachPreparation, selection.TargetID)
			if candidate != nil {
				item.candidateAttach = true
			} else {
				attachRead, readErr := repository.store.GetMany(ctx, GetManyRequest{Keys: []string{
					attachKey(selection.TargetID), attachOwnerKey(input.EnvironmentID, selection.TargetID),
					deletionTombstoneKey("attach", selection.TargetID),
				}, Revision: revision})
				if readErr != nil {
					return nil, readErr
				}
				if attachRead == nil || attachRead.ReadRevision != revision || len(attachRead.Values) != 3 ||
					attachRead.Values[0] == nil || attachRead.Values[1] == nil || attachRead.Values[2] != nil {
					return nil, errs.New(errs.KindAttachNotFound, "backup Attach source was not found")
				}
				attach, decodeErr := decodeAttachRecord(attachRead.Values[0].Value)
				if decodeErr != nil || attach.ID != selection.TargetID || attach.EnvironmentID != input.EnvironmentID ||
					!attach.OwnsCredential() || string(attachRead.Values[1].Value) != selection.TargetID {
					return nil, errs.New(errs.KindValidationFailed, "backup Attach source must own credentials")
				}
				versioned := Versioned[AttachRecord]{Record: attach, Revision: attachRead.Values[0].ModRevision, ReadRevision: revision}
				item.attach = &versioned
				item.attachOwner = cloneBackupPolicyEvidenceKeyValue(attachRead.Values[1])
			}
		}
		result[index] = item
	}
	return result, nil
}

func validateBlueprintBackupTarget(
	input EnvironmentBlueprintBackupPolicyInput,
	selection EnvironmentBlueprintBackupPolicySourceInput,
) error {
	switch selection.Kind {
	case core.BackupSourceConfig:
		if selection.TargetID == input.EnvironmentID {
			return nil
		}
	case core.BackupSourceVolume:
		for _, volume := range input.Projection.Volumes {
			if volume.ID == selection.TargetID {
				return nil
			}
		}
	case core.BackupSourceAttach:
		candidate := blueprintBackupAttachCandidate(input.AttachPreparation, selection.TargetID)
		if candidate == nil || candidate.Record.EnvironmentID == input.EnvironmentID && candidate.Record.OwnsCredential() {
			return nil
		}
	}
	return errs.New(errs.KindValidationFailed, "Blueprint Backup source target is invalid")
}

func blueprintBackupAttachCandidate(
	preparation BlueprintAttachTaskPreparation,
	attachID string,
) *EnvironmentBlueprintAttachCandidateInput {
	for index := range preparation.candidates {
		if preparation.candidates[index].Record.ID == attachID {
			return &preparation.candidates[index]
		}
	}
	return nil
}
