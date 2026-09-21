package etcd

import (
	"context"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	backupconfigrecord "github.com/AlanD20/groundplane/internal/infra/etcd/backupconfiguration"
	backupplanning "github.com/AlanD20/groundplane/internal/infra/etcd/backupplanning"
	backuppolicy "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/pkg/errs"
	"slices"
)

func (repository *BackupRuntimeRepository) loadBackupRunPublicationEvidence(
	ctx context.Context,
	run backupruntime.BackupRunRecord,
	retrySource *backupRunRetrySource,
	fixedRevision int64,
) ([]etcdstore.Condition, []etcdstore.Mutation, error) {
	conditions := make([]etcdstore.Condition, 0, len(run.Sources)*2+3)
	mutations := make([]etcdstore.Mutation, 0, 3)
	conditionRevisions := make(map[string]int64, len(run.Sources)*2+3)
	addCondition := func(key string, revision int64) error {
		if previous, exists := conditionRevisions[key]; exists {
			if previous != revision {
				return errs.New(
					errs.KindStateConflict,
					"backup publication revision evidence differs",
				)
			}
			return nil
		}
		conditionRevisions[key] = revision
		conditions = append(conditions, etcdstore.Condition{Key: key, ModRevision: revision})
		return nil
	}
	if retrySource == nil {
		sourceIDs := make([]string, len(run.Sources))
		for index := range run.Sources {
			sourceIDs[index] = run.Sources[index].SourceID
		}
		policyRead, err := repository.ReadFixedKeys(
			ctx,
			[]string{backuppolicy.BackupPolicyKey(run.EnvironmentID)},
			fixedRevision,
		)
		if err != nil {
			return nil, nil, err
		}
		defer etcdstore.ClearValues(policyRead.Values)
		if policyRead.Values[0] == nil || policyRead.Values[0].ModRevision != run.PolicyRevision {
			return nil, nil, errs.New(errs.KindStateConflict, "backup policy snapshot changed")
		}
		policy, decodeErr := backuppolicy.DecodeBackupPolicyRecord(policyRead.Values[0].Value)
		if decodeErr != nil {
			return nil, nil, backupruntime.CorruptBackupRuntimeRecord()
		}
		policyKeep, keepErr := checkedBackupRuntimeRetentionKeep(policy.Keep)
		if keepErr != nil {
			return nil, nil, keepErr
		}
		if !policy.Enabled || policy.EnvironmentID != run.EnvironmentID ||
			policyKeep != run.RetentionKeep || policy.ConnectorID != run.ConnectorID ||
			policy.Encryption != string(run.Encryption) ||
			!slices.Equal(policy.SourceIDs, sourceIDs) {
			return nil, nil, errs.New(errs.KindStateConflict, "backup policy snapshot changed")
		}
		if err := addCondition(backuppolicy.BackupPolicyKey(run.EnvironmentID), run.PolicyRevision); err != nil {
			return nil, nil, err
		}
	}
	if run.Encryption == backupruntime.BackupRuntimeEncryptionAge {
		keyRead, readErr := repository.ReadFixedKeys(ctx, []string{
			backuppolicy.BackupKeyKey(run.EnvironmentID), backuppolicy.BackupKeyValueKey(run.EnvironmentID),
		}, fixedRevision)
		if readErr != nil {
			return nil, nil, readErr
		}
		defer etcdstore.ClearValues(keyRead.Values)
		if keyRead.Values[0] == nil || keyRead.Values[1] == nil ||
			keyRead.Values[0].ModRevision != run.BackupKeyRecordRevision ||
			keyRead.Values[1].ModRevision != run.BackupKeyValueRevision {
			return nil, nil, errs.New(errs.KindStateConflict, "backup key snapshot changed")
		}
		keyRecord, decodeErr := backuppolicy.DecodeBackupKeyRecord(keyRead.Values[0].Value)
		keyValue, valueErr := backuppolicy.DecodeBackupKeyEncryptedValue(keyRead.Values[1].Value)
		defer clear(keyValue.Ciphertext)
		if decodeErr != nil || valueErr != nil || keyRecord.EnvironmentID != run.EnvironmentID ||
			keyValue.EnvironmentID != run.EnvironmentID || keyRecord.KeyEra != run.KeyEra ||
			keyValue.KeyEra != run.KeyEra || keyRecord.Recipient != run.Recipient {
			return nil, nil, errs.New(errs.KindStateConflict, "backup key snapshot changed")
		}
		if err := addCondition(
			backuppolicy.BackupKeyKey(run.EnvironmentID),
			run.BackupKeyRecordRevision,
		); err != nil {
			return nil, nil, err
		}
		if err := addCondition(
			backuppolicy.BackupKeyValueKey(run.EnvironmentID),
			run.BackupKeyValueRevision,
		); err != nil {
			return nil, nil, err
		}
	}
	for _, source := range run.Sources {
		sourceRead, readErr := repository.ReadFixedKeys(
			ctx,
			[]string{backuppolicy.BackupSourceKey(source.SourceID)},
			fixedRevision,
		)
		if readErr != nil {
			etcdstore.ClearMutationValues(mutations)
			return nil, nil, readErr
		}
		if sourceRead.Values[0] == nil ||
			sourceRead.Values[0].ModRevision != source.SourceRevision {
			etcdstore.ClearValues(sourceRead.Values)
			etcdstore.ClearMutationValues(mutations)
			return nil, nil, errs.New(errs.KindStateConflict, "backup source snapshot changed")
		}
		storedSource, decodeErr := backuppolicy.DecodeBackupSourceRecord(sourceRead.Values[0].Value)
		etcdstore.ClearValues(sourceRead.Values)
		if decodeErr != nil || storedSource.ID != source.SourceID ||
			storedSource.EnvironmentID != run.EnvironmentID ||
			string(
				storedSource.Kind,
			) != string(
				source.Kind,
			) || storedSource.TargetID != source.TargetID {
			etcdstore.ClearMutationValues(mutations)
			return nil, nil, errs.New(errs.KindStateConflict, "backup source snapshot changed")
		}
		if err := addCondition(
			backuppolicy.BackupSourceKey(source.SourceID),
			source.SourceRevision,
		); err != nil {
			etcdstore.ClearMutationValues(mutations)
			return nil, nil, err
		}
		switch source.Kind {
		case backupruntime.BackupRuntimeSourceAttach:
			snapshot := source.Snapshot.Postgres
			read, readErr := repository.ReadFixedKeys(ctx, []string{
				attachrecord.AttachKey(source.TargetID),
				attachrecord.AttachFactsKey(source.TargetID),
				hierarchyrecord.ProjectKey(
					snapshot.BackingProjectID,
				),
				hierarchyrecord.EnvironmentKey(snapshot.BackingEnvironmentID),
				blueprints.EnvironmentBlueprintHeadKey(snapshot.BackingEnvironmentID),
			}, fixedRevision)
			if readErr != nil {
				etcdstore.ClearMutationValues(mutations)
				return nil, nil, readErr
			}
			if err := backupplanning.ValidateBackupPostgresPublicationEvidence(
				read.Values,
				source,
				*snapshot,
			); err != nil {
				etcdstore.ClearValues(read.Values)
				etcdstore.ClearMutationValues(mutations)
				return nil, nil, err
			}
			etcdstore.ClearValues(read.Values)
			for _, fact := range []struct {
				key      string
				revision int64
			}{
				{attachrecord.AttachKey(source.TargetID), source.TargetRevision},
				{attachrecord.AttachFactsKey(source.TargetID), snapshot.AttachFactsRevision},
				{hierarchyrecord.ProjectKey(snapshot.BackingProjectID), snapshot.BackingProjectRevision},
				{hierarchyrecord.EnvironmentKey(snapshot.BackingEnvironmentID), snapshot.BackingEnvironmentRevision},
				{blueprints.EnvironmentBlueprintHeadKey(snapshot.BackingEnvironmentID), snapshot.BackingServiceRevision},
			} {
				if err := addCondition(fact.key, fact.revision); err != nil {
					etcdstore.ClearMutationValues(mutations)
					return nil, nil, err
				}
			}
		case backupruntime.BackupRuntimeSourceVolume:
			snapshot := source.Snapshot.Volume
			keys := []string{
				hierarchyrecord.EnvironmentKey(snapshot.EnvironmentID),
				blueprints.EnvironmentBlueprintHeadKey(snapshot.EnvironmentID),
				blueprints.EnvironmentBlueprintRootKey(snapshot.EnvironmentID, snapshot.DesiredRevisionID),
			}
			for _, service := range snapshot.Services {
				keys = append(keys, servicerecord.ServiceRuntimeKey(service.ServiceID))
			}
			read, readErr := repository.ReadFixedKeys(ctx, keys, fixedRevision)
			if readErr != nil {
				etcdstore.ClearMutationValues(mutations)
				return nil, nil, readErr
			}
			if err := backupplanning.ValidateBackupVolumePublicationEvidence(
				read.Values,
				source,
				*snapshot,
			); err != nil {
				etcdstore.ClearValues(read.Values)
				etcdstore.ClearMutationValues(mutations)
				return nil, nil, err
			}
			etcdstore.ClearValues(read.Values)
			if err := addCondition(hierarchyrecord.EnvironmentKey(snapshot.EnvironmentID), snapshot.EnvironmentRevision); err != nil {
				etcdstore.ClearMutationValues(mutations)
				return nil, nil, err
			}
			if err := addCondition(blueprints.EnvironmentBlueprintHeadKey(snapshot.EnvironmentID), source.TargetRevision); err != nil {
				etcdstore.ClearMutationValues(mutations)
				return nil, nil, err
			}
			if err := addCondition(
				blueprints.EnvironmentBlueprintRootKey(snapshot.EnvironmentID, snapshot.DesiredRevisionID),
				snapshot.ProjectionRoot,
			); err != nil {
				etcdstore.ClearMutationValues(mutations)
				return nil, nil, err
			}
			for _, service := range snapshot.Services {
				if err := addCondition(
					servicerecord.ServiceRuntimeKey(service.ServiceID),
					service.ServiceRevision,
				); err != nil {
					etcdstore.ClearMutationValues(mutations)
					return nil, nil, err
				}
			}
		case backupruntime.BackupRuntimeSourceConfig:
			snapshot := source.Snapshot.Config
			if retrySource != nil {
				configConditions, configMutations, retryErr := repository.prepareBackupRetryConfigReferences(
					ctx, run, source, retrySource.run.Record, fixedRevision,
				)
				if retryErr != nil {
					etcdstore.ClearMutationValues(mutations)
					return nil, nil, retryErr
				}
				for _, condition := range configConditions {
					if err := addCondition(condition.Key, condition.ModRevision); err != nil {
						etcdstore.ClearMutationValues(configMutations)
						etcdstore.ClearMutationValues(mutations)
						return nil, nil, err
					}
				}
				mutations = append(mutations, configMutations...)
				continue
			}
			if snapshot.ConfigSnapshotID != run.TaskID || snapshot.ReadRevision != fixedRevision {
				etcdstore.ClearMutationValues(mutations)
				return nil, nil, errs.New(
					errs.KindStateConflict,
					"backup config snapshot revision changed",
				)
			}
			targetRead, readErr := repository.ReadFixedKeys(
				ctx,
				[]string{hierarchyrecord.EnvironmentKey(run.EnvironmentID)},
				fixedRevision,
			)
			if readErr != nil {
				etcdstore.ClearMutationValues(mutations)
				return nil, nil, readErr
			}
			if targetRead.Values[0] == nil ||
				targetRead.Values[0].ModRevision != source.TargetRevision {
				etcdstore.ClearValues(targetRead.Values)
				etcdstore.ClearMutationValues(mutations)
				return nil, nil, errs.New(errs.KindStateConflict, "backup config target changed")
			}
			environment, decodeErr := hierarchyrecord.DecodeEnvironment(targetRead.Values[0].Value)
			etcdstore.ClearValues(targetRead.Values)
			if decodeErr != nil || environment.ID != run.EnvironmentID {
				etcdstore.ClearMutationValues(mutations)
				return nil, nil, backupruntime.CorruptBackupRuntimeRecord()
			}
			config := backupconfigrecord.BackupConfigSnapshotRecord{
				SnapshotID:    snapshot.ConfigSnapshotID,
				EnvironmentID: run.EnvironmentID,
				SourceID:      source.SourceID,
				State:         backupconfigrecord.BackupConfigSnapshotBuilding,
				ReadRevision:  snapshot.ReadRevision,
				CreatedAt:     run.CreatedAt,
				UpdatedAt:     run.CreatedAt,
			}
			value, encodeErr := backupconfigrecord.EncodeBackupConfigSnapshotRecord(config)
			if encodeErr != nil {
				etcdstore.ClearMutationValues(mutations)
				return nil, nil, encodeErr
			}
			mutations = append(
				mutations,
				etcdstore.Mutation{
					Type:  etcdstore.MutationPut,
					Key:   backupconfigrecord.BackupConfigSnapshotKey(snapshot.ConfigSnapshotID),
					Value: value,
				},
				etcdstore.Mutation{
					Type: etcdstore.MutationPut,
					Key: backupconfigrecord.BackupConfigSnapshotTaskReferenceKey(
						run.TaskID,
						snapshot.ConfigSnapshotID,
					),
					Value: []byte(snapshot.ConfigSnapshotID),
				},
				etcdstore.Mutation{
					Type: etcdstore.MutationPut,
					Key: backupconfigrecord.BackupConfigSnapshotReferenceTaskKey(
						snapshot.ConfigSnapshotID,
						run.TaskID,
					),
					Value: []byte(run.TaskID),
				},
			)
		}
	}
	return conditions, mutations, nil
}

func checkedBackupRuntimeRetentionKeep(keep int64) (int64, error) {
	if keep <= 0 || keep > backuppolicy.MaximumBackupPolicyKeep {
		return 0, backupruntime.CorruptBackupRuntimeRecord()
	}
	return keep, nil
}
