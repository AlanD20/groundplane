package backuppolicymutations

import (
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	backuppolicy "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	connectorrecord "github.com/AlanD20/groundplane/internal/infra/etcd/connectors"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	coordinationrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentcoordination"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

func mustBackupPolicyScheduleTransition(
	candidate ReplacementCandidate,
) coordinationrecord.EnvironmentCoordinationRecord {
	next, _, err := coordinationrecord.ReplaceSchedule(
		candidate.Coordination.Record, candidate.Replacement, candidate.Replacement.UpdatedAt,
	)
	if err != nil {
		return coordinationrecord.EnvironmentCoordinationRecord{}
	}
	return next
}

func PrepareBackupPolicyReplacement(
	candidate ReplacementCandidate,
) (backupPolicyReplacementPlan, error) {
	policyValue, err := backuppolicy.EncodeBackupPolicyRecord(candidate.Replacement)
	if err != nil {
		return backupPolicyReplacementPlan{}, err
	}
	coordinationValue, err := coordinationrecord.Encode(candidate.NextCoordination)
	if err != nil {
		clear(policyValue)
		return backupPolicyReplacementPlan{}, err
	}
	plan := backupPolicyReplacementPlan{
		conditions: make([]etcdstore.Condition, 0, 18+len(candidate.Sources)*3),
		mutations: []etcdstore.Mutation{
			{
				Type: etcdstore.MutationPut, Key: backuppolicy.BackupPolicyKey(candidate.Replacement.EnvironmentID), Value: policyValue,
			},
			{
				Type: etcdstore.MutationPut, Key: coordinationrecord.Key(candidate.Replacement.EnvironmentID),
				Value: coordinationValue,
			},
		},
		evidence: make([]BackupPolicyReplacementCompare, 0, 18+len(candidate.Sources)*3),
	}
	policyRevision := int64(0)
	if candidate.Current != nil {
		policyRevision = candidate.Current.Revision
	}
	plan.compare(
		BackupPolicyComparePolicy,
		candidate.Replacement.EnvironmentID,
		backuppolicy.BackupPolicyKey(candidate.Replacement.EnvironmentID),
		policyRevision,
	)
	plan.compare(
		backupPolicyCompareEnvironment,
		candidate.Environment.Record.ID,
		hierarchyrecord.EnvironmentKey(candidate.Environment.Record.ID),
		candidate.Environment.Revision,
	)
	plan.compare(
		backupPolicyCompareProject,
		candidate.Project.Record.ID,
		hierarchyrecord.ProjectKey(candidate.Project.Record.ID),
		candidate.Project.Revision,
	)
	plan.compare(
		BackupPolicyCompareCoordination,
		candidate.Replacement.EnvironmentID,
		coordinationrecord.Key(candidate.Replacement.EnvironmentID),
		candidate.Coordination.Revision,
	)
	plan.compare(
		backupPolicyCompareOperationLock,
		candidate.Replacement.EnvironmentID,
		hierarchyrecord.EnvironmentOperationLockKey(candidate.Replacement.EnvironmentID),
		0,
	)
	for _, fence := range []struct {
		kind deletionrecord.DeletionTargetKind
		id   string
	}{
		{kind: deletionrecord.DeletionTargetEnvironment, id: candidate.Environment.Record.ID},
		{kind: deletionrecord.DeletionTargetProject, id: candidate.Project.Record.ID},
		{kind: deletionrecord.DeletionTargetTenant, id: candidate.Project.Record.TenantID},
	} {
		plan.compare(
			backupPolicyCompareHierarchyTombstone,
			fence.id,
			deletionrecord.TombstoneKey(string(fence.kind), fence.id),
			0,
		)
	}
	for _, source := range candidate.Sources {
		plan.compare(
			BackupPolicyCompareSource,
			source.Source.Record.ID,
			backuppolicy.BackupSourceKey(source.Source.Record.ID),
			source.Source.Revision,
		)
		plan.compare(
			BackupPolicyCompareSourceEnvironmentIndex,
			source.Source.Record.ID,
			source.EnvironmentIndex.Key,
			source.EnvironmentIndex.ModRevision,
		)
		plan.compare(
			BackupPolicyCompareSourceIdentityIndex,
			source.Source.Record.ID,
			source.IdentityIndex.Key,
			source.IdentityIndex.ModRevision,
		)
		switch string(source.Source.Record.Kind) {
		case "attach":
			plan.compare(
				BackupPolicyCompareAttach,
				source.Attach.Record.ID,
				attachrecord.AttachKey(source.Attach.Record.ID),
				source.Attach.Revision,
			)
			plan.compare(
				BackupPolicyCompareTargetOwnerIndex,
				source.Attach.Record.ID,
				source.TargetOwnerIndex.Key,
				source.TargetOwnerIndex.ModRevision,
			)
			plan.compare(
				BackupPolicyCompareTargetTombstone,
				source.Attach.Record.ID,
				deletionrecord.TombstoneKey("attach", source.Attach.Record.ID),
				0,
			)
		case "volume":
			plan.compare(
				backupPolicyCompareVolume,
				source.Volume.Volume.ID,
				blueprints.EnvironmentBlueprintHeadKey(source.Source.Record.EnvironmentID),
				source.Volume.Projection.Revision,
			)
			plan.compare(
				backupPolicyCompareVolumeRoot,
				source.Volume.Volume.ID,
				blueprints.EnvironmentBlueprintRootKey(
					source.Source.Record.EnvironmentID,
					source.Volume.Projection.Record.RevisionID,
				),
				source.Volume.ProjectionRoot,
			)
		}
	}
	if candidate.Connector != nil {
		connectorID := candidate.Connector.Record.Connector.ID
		plan.compare(
			BackupPolicyCompareConnector,
			connectorID,
			connectorrecord.RecordKey(connectorID),
			candidate.Connector.Revision,
		)
		plan.compare(
			BackupPolicyCompareConnectorOwnerIndex,
			connectorID,
			candidate.ConnectorOwnerIndex.Key,
			candidate.ConnectorOwnerIndex.ModRevision,
		)
		plan.compare(
			BackupPolicyCompareConnectorTombstone,
			connectorID,
			deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetConnector), connectorID),
			0,
		)
	}
	for _, reference := range candidate.ConnectorReferences {
		revision := int64(0)
		if reference.Entry != nil {
			revision = reference.Entry.ModRevision
		}
		plan.compare(
			BackupPolicyCompareConnectorReference,
			reference.ConnectorID,
			backuppolicy.BackupPolicyConnectorReferenceKey(reference.ConnectorID, candidate.Replacement.EnvironmentID),
			revision,
		)
	}
	oldConnectorID := ""
	if candidate.Current != nil && candidate.Current.Record.Enabled {
		oldConnectorID = candidate.Current.Record.ConnectorID
	}
	newConnectorID := ""
	if candidate.Replacement.Enabled {
		newConnectorID = candidate.Replacement.ConnectorID
	}
	if oldConnectorID != "" && oldConnectorID != newConnectorID {
		plan.mutations = append(plan.mutations, etcdstore.Mutation{
			Type: etcdstore.MutationDelete,
			Key:  backuppolicy.BackupPolicyConnectorReferenceKey(oldConnectorID, candidate.Replacement.EnvironmentID),
		})
	}
	if newConnectorID != "" && newConnectorID != oldConnectorID {
		plan.mutations = append(plan.mutations, etcdstore.Mutation{
			Type:  etcdstore.MutationPut,
			Key:   backuppolicy.BackupPolicyConnectorReferenceKey(newConnectorID, candidate.Replacement.EnvironmentID),
			Value: []byte(candidate.Replacement.EnvironmentID),
		})
	}
	keyRecordRevision := int64(0)
	keyValueRevision := int64(0)
	if candidate.ExistingKey != nil {
		keyRecordRevision = candidate.ExistingKey.RecordRevision
		keyValueRevision = candidate.ExistingKey.EncryptedRevision
	}
	plan.compare(
		BackupPolicyCompareKey,
		candidate.Replacement.EnvironmentID,
		backuppolicy.BackupKeyKey(candidate.Replacement.EnvironmentID),
		keyRecordRevision,
	)
	plan.compare(
		BackupPolicyCompareKey,
		candidate.Replacement.EnvironmentID,
		backuppolicy.BackupKeyValueKey(candidate.Replacement.EnvironmentID),
		keyValueRevision,
	)
	if candidate.InitialKey != nil {
		initial := InitialKey{
			Record:    candidate.InitialKey.Record,
			Encrypted: candidate.InitialKey.Encrypted,
		}
		initial.Encrypted.Ciphertext = append([]byte(nil), candidate.InitialKey.Encrypted.Ciphertext...)
		defer clear(initial.Encrypted.Ciphertext)
		recordValue, encodeErr := backuppolicy.EncodeBackupKeyRecord(initial.Record)
		if encodeErr != nil {
			etcdstore.ClearMutationValues(plan.mutations)
			return backupPolicyReplacementPlan{}, encodeErr
		}
		encryptedValue, encodeErr := backuppolicy.EncodeBackupKeyEncryptedValue(initial.Encrypted)
		if encodeErr != nil {
			clear(recordValue)
			etcdstore.ClearMutationValues(plan.mutations)
			return backupPolicyReplacementPlan{}, encodeErr
		}
		plan.mutations = append(
			plan.mutations,
			etcdstore.Mutation{Type: etcdstore.MutationPut, Key: backuppolicy.BackupKeyKey(candidate.Replacement.EnvironmentID), Value: recordValue},
			etcdstore.Mutation{
				Type:  etcdstore.MutationPut,
				Key:   backuppolicy.BackupKeyValueKey(candidate.Replacement.EnvironmentID),
				Value: encryptedValue,
			},
		)
	}
	return plan, nil
}

func backupPolicyReplacementOperationCount(
	plan backupPolicyReplacementPlan,
	marker idempotencyrecord.IdempotencyMarker,
) int {
	count := len(plan.conditions) + len(plan.mutations) + 2
	if marker.ReplayTarget != nil {
		count += 2
	}
	if !marker.RetainUntil.IsZero() {
		count++
	}
	return count
}
