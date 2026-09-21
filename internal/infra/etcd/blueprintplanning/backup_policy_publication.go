package blueprintplanning

import (
	"github.com/AlanD20/groundplane/internal/core"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	backuppolicy "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	backuppolicymutations "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicymutations"
	connectorrecord "github.com/AlanD20/groundplane/internal/infra/etcd/connectors"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	coordinationrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentcoordination"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type BackupPolicyPublication struct {
	conditions []etcdstore.Condition
	mutations  []etcdstore.Mutation
	evidence   []backuppolicymutations.BackupPolicyReplacementCompare
}

func PrepareBlueprintBackupPolicyPublication(
	task TaskIdentity,
	projection projectionrecord.EnvironmentComposeProjection,
	attaches BlueprintAttachTaskPreparation,
	prepared BlueprintBackupPolicyPreparation,
) (BackupPolicyPublication, error) {
	if prepared.state == nil {
		return BackupPolicyPublication{}, nil
	}
	state := prepared.state
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.consumed {
		return BackupPolicyPublication{}, errs.New(
			errs.KindStateConflict, "Blueprint Backup preparation was already consumed",
		)
	}
	state.consumed = true
	if task.ID != state.taskID || task.Target != state.environmentID ||
		projection.EnvironmentID != state.environmentID || projection.RevisionID != state.taskID ||
		!EqualEnvironmentBlueprintBackupPolicy(projection.Backup, state.desired) ||
		state.requiresInitialKey != (state.candidate.InitialKey != nil) {
		return BackupPolicyPublication{}, errs.New(
			errs.KindValidationFailed, "Blueprint Backup publication identity is invalid",
		)
	}
	publication := BackupPolicyPublication{}
	if !state.retain {
		policyValue, err := backuppolicy.EncodeBackupPolicyRecord(state.candidate.Replacement)
		if err != nil {
			return BackupPolicyPublication{}, err
		}
		coordinationValue, err := coordinationrecord.Encode(state.candidate.NextCoordination)
		if err != nil {
			clear(policyValue)
			return BackupPolicyPublication{}, err
		}
		publication.mutations = append(
			publication.mutations,
			etcdstore.Mutation{
				Type:  etcdstore.MutationPut,
				Key:   backuppolicy.BackupPolicyKey(state.environmentID),
				Value: policyValue,
			},
			etcdstore.Mutation{
				Type:  etcdstore.MutationPut,
				Key:   coordinationrecord.Key(state.environmentID),
				Value: coordinationValue,
			},
		)
	}
	compare := func(kind backuppolicymutations.BackupPolicyReplacementCompareKind, id, key string, revision int64) {
		publication.conditions = append(publication.conditions, etcdstore.Condition{Key: key, ModRevision: revision})
		publication.evidence = append(publication.evidence, backuppolicymutations.BackupPolicyReplacementCompare{
			Kind: kind, ID: id, ExpectedRevision: revision,
		})
	}
	policyRevision := int64(0)
	if state.candidate.Current != nil {
		policyRevision = state.candidate.Current.Revision
	}
	compare(
		backuppolicymutations.BackupPolicyComparePolicy,
		state.environmentID,
		backuppolicy.BackupPolicyKey(state.environmentID),
		policyRevision,
	)
	compare(
		backuppolicymutations.BackupPolicyCompareCoordination,
		state.environmentID,
		coordinationrecord.Key(state.environmentID),
		state.candidate.Coordination.Revision,
	)
	for _, source := range state.sources {
		primaryRevision, environmentRevision, identityRevision := int64(0), int64(0), int64(0)
		if source.primary != nil {
			primaryRevision = source.primary.ModRevision
			environmentRevision = source.environmentIndex.ModRevision
			identityRevision = source.identityIndex.ModRevision
		}
		compare(
			backuppolicymutations.BackupPolicyCompareSource,
			source.record.ID,
			backuppolicy.BackupSourceKey(source.record.ID),
			primaryRevision,
		)
		compare(
			backuppolicymutations.BackupPolicyCompareSourceEnvironmentIndex,
			source.record.ID,
			backuppolicy.BackupSourceEnvironmentKey(state.environmentID, source.record.ID),
			environmentRevision,
		)
		compare(
			backuppolicymutations.BackupPolicyCompareSourceIdentityIndex,
			source.record.ID,
			backuppolicy.BackupSourceIdentityKey(state.environmentID, source.record.Kind, source.record.TargetID),
			identityRevision,
		)
		if source.primary == nil {
			sourceValue, encodeErr := backuppolicy.EncodeBackupSourceRecord(source.record)
			if encodeErr != nil {
				ClearPreparedBlueprintBackupPolicyPublication(publication)
				return BackupPolicyPublication{}, encodeErr
			}
			publication.mutations = append(
				publication.mutations,
				etcdstore.Mutation{
					Type:  etcdstore.MutationPut,
					Key:   backuppolicy.BackupSourceKey(source.record.ID),
					Value: sourceValue,
				},
				etcdstore.Mutation{
					Type:  etcdstore.MutationPut,
					Key:   backuppolicy.BackupSourceEnvironmentKey(state.environmentID, source.record.ID),
					Value: []byte(source.record.ID),
				},
				etcdstore.Mutation{
					Type: etcdstore.MutationPut,
					Key: backuppolicy.BackupSourceIdentityKey(
						state.environmentID,
						source.record.Kind,
						source.record.TargetID,
					),
					Value: []byte(source.record.ID),
				},
			)
		}
		if !state.retain && source.record.Kind == core.BackupSourceAttach {
			if source.candidateAttach {
				candidate := blueprintBackupAttachCandidate(attaches, source.record.TargetID)
				if candidate == nil || !candidate.Record.OwnsCredential() {
					ClearPreparedBlueprintBackupPolicyPublication(publication)
					return BackupPolicyPublication{}, errs.New(
						errs.KindValidationFailed,
						"Blueprint Backup candidate Attach changed",
					)
				}
			} else {
				compare(backuppolicymutations.BackupPolicyCompareAttach, source.record.TargetID, attachrecord.AttachKey(source.record.TargetID), source.attach.Revision)
				compare(backuppolicymutations.BackupPolicyCompareTargetOwnerIndex, source.record.TargetID, attachrecord.AttachOwnerKey(state.environmentID, source.record.TargetID), source.attachOwner.ModRevision)
				compare(backuppolicymutations.BackupPolicyCompareTargetTombstone, source.record.TargetID, deletionrecord.TombstoneKey("attach", source.record.TargetID), 0)
			}
		}
	}
	if state.retain && state.retainedConnectorID != "" {
		connectorRevision, ownerRevision, tombstoneRevision := int64(0), int64(0), int64(0)
		if state.candidate.Connector != nil {
			connectorRevision = state.candidate.Connector.Revision
			ownerRevision = state.candidate.ConnectorOwnerIndex.ModRevision
			compare(
				backuppolicymutations.BackupPolicyCompareConnector,
				state.retainedConnectorID,
				state.connectorNameIndex.Key,
				state.connectorNameIndex.ModRevision,
			)
		}
		if state.connectorTombstone != nil {
			tombstoneRevision = state.connectorTombstone.ModRevision
		}
		compare(
			backuppolicymutations.BackupPolicyCompareConnector,
			state.retainedConnectorID,
			connectorrecord.RecordKey(state.retainedConnectorID),
			connectorRevision,
		)
		compare(
			backuppolicymutations.BackupPolicyCompareConnectorOwnerIndex,
			state.retainedConnectorID,
			connectorrecord.ConnectorEnvironmentKey(state.environmentID, state.retainedConnectorID),
			ownerRevision,
		)
		compare(
			backuppolicymutations.BackupPolicyCompareConnectorTombstone,
			state.retainedConnectorID,
			deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetConnector), state.retainedConnectorID),
			tombstoneRevision,
		)
	} else if state.candidate.Connector != nil {
		connectorID := state.candidate.Connector.Record.Connector.ID
		compare(backuppolicymutations.BackupPolicyCompareConnector, connectorID, state.connectorNameIndex.Key, state.connectorNameIndex.ModRevision)
		compare(backuppolicymutations.BackupPolicyCompareConnector, connectorID, connectorrecord.RecordKey(connectorID), state.candidate.Connector.Revision)
		compare(backuppolicymutations.BackupPolicyCompareConnectorOwnerIndex, connectorID, state.candidate.ConnectorOwnerIndex.Key, state.candidate.ConnectorOwnerIndex.ModRevision)
		compare(backuppolicymutations.BackupPolicyCompareConnectorTombstone, connectorID, deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetConnector), connectorID), 0)
	}
	for _, reference := range state.candidate.ConnectorReferences {
		revision := int64(0)
		if reference.Entry != nil {
			revision = reference.Entry.ModRevision
		}
		compare(
			backuppolicymutations.BackupPolicyCompareConnectorReference,
			reference.ConnectorID,
			backuppolicy.BackupPolicyConnectorReferenceKey(reference.ConnectorID, state.environmentID),
			revision,
		)
	}
	if !state.retain {
		oldConnectorID := ""
		if state.candidate.Current != nil && state.candidate.Current.Record.Enabled {
			oldConnectorID = state.candidate.Current.Record.ConnectorID
		}
		newConnectorID := ""
		if state.candidate.Replacement.Enabled {
			newConnectorID = state.candidate.Replacement.ConnectorID
		}
		if oldConnectorID != "" && oldConnectorID != newConnectorID {
			publication.mutations = append(
				publication.mutations,
				etcdstore.Mutation{
					Type: etcdstore.MutationDelete,
					Key:  backuppolicy.BackupPolicyConnectorReferenceKey(oldConnectorID, state.environmentID),
				},
			)
		}
		if newConnectorID != "" && newConnectorID != oldConnectorID {
			publication.mutations = append(
				publication.mutations,
				etcdstore.Mutation{
					Type:  etcdstore.MutationPut,
					Key:   backuppolicy.BackupPolicyConnectorReferenceKey(newConnectorID, state.environmentID),
					Value: []byte(state.environmentID),
				},
			)
		}
	}
	recordRevision, encryptedRevision := int64(0), int64(0)
	if state.candidate.ExistingKey != nil {
		recordRevision = state.candidate.ExistingKey.RecordRevision
		encryptedRevision = state.candidate.ExistingKey.EncryptedRevision
	}
	compare(
		backuppolicymutations.BackupPolicyCompareKey,
		state.environmentID,
		backuppolicy.BackupKeyKey(state.environmentID),
		recordRevision,
	)
	compare(
		backuppolicymutations.BackupPolicyCompareKey,
		state.environmentID,
		backuppolicy.BackupKeyValueKey(state.environmentID),
		encryptedRevision,
	)
	if state.candidate.InitialKey != nil {
		recordValue, encodeErr := backuppolicy.EncodeBackupKeyRecord(state.candidate.InitialKey.Record)
		if encodeErr != nil {
			ClearPreparedBlueprintBackupPolicyPublication(publication)
			return BackupPolicyPublication{}, encodeErr
		}
		encryptedValue, encodeErr := backuppolicy.EncodeBackupKeyEncryptedValue(state.candidate.InitialKey.Encrypted)
		if encodeErr != nil {
			clear(recordValue)
			ClearPreparedBlueprintBackupPolicyPublication(publication)
			return BackupPolicyPublication{}, encodeErr
		}
		publication.mutations = append(
			publication.mutations,
			etcdstore.Mutation{
				Type:  etcdstore.MutationPut,
				Key:   backuppolicy.BackupKeyKey(state.environmentID),
				Value: recordValue,
			},
			etcdstore.Mutation{
				Type:  etcdstore.MutationPut,
				Key:   backuppolicy.BackupKeyValueKey(state.environmentID),
				Value: encryptedValue,
			},
		)
	}
	return publication, nil
}

func EqualEnvironmentBlueprintBackupPolicy(left, right *projectionrecord.EnvironmentBlueprintBackupPolicy) bool {
	if left == nil || right == nil {
		return left == right
	}
	if left.Enabled != right.Enabled || left.Frequency != right.Frequency || left.Keep != right.Keep ||
		left.Encryption != right.Encryption || left.ConnectorID != right.ConnectorID || len(left.Sources) != len(right.Sources) {
		return false
	}
	for index := range left.Sources {
		if left.Sources[index] != right.Sources[index] {
			return false
		}
	}
	return true
}

func ClearPreparedBlueprintBackupPolicyPublication(publication BackupPolicyPublication) {
	etcdstore.ClearMutationValues(publication.mutations)
}

func ClassifyEnvironmentBlueprintBackupPolicyPublication(
	base func(int64, []*etcdstore.KeyValue) error,
	publication BackupPolicyPublication,
) func(int64, []*etcdstore.KeyValue) error {
	count := len(publication.conditions)
	return func(revision int64, values []*etcdstore.KeyValue) error {
		if len(values) < count {
			return errs.New(errs.KindInternal, "Blueprint Backup compare evidence is incomplete")
		}
		baseCount := len(values) - count
		if err := base(revision, values[:baseCount]); err != nil {
			return err
		}
		return backuppolicymutations.ClassifyBackupPolicyReplacementConflict(values[baseCount:], publication.evidence)
	}
}
