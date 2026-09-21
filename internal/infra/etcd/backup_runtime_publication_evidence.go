package etcd

import (
	"context"
	"encoding/hex"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	backuppolicy "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
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
		policyRead, err := repository.readFixedKeys(
			ctx,
			[]string{backuppolicy.BackupPolicyKey(run.EnvironmentID)},
			fixedRevision,
		)
		if err != nil {
			return nil, nil, err
		}
		defer clearKeyValues(policyRead.Values)
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
		keyRead, readErr := repository.readFixedKeys(ctx, []string{
			backuppolicy.BackupKeyKey(run.EnvironmentID), backuppolicy.BackupKeyValueKey(run.EnvironmentID),
		}, fixedRevision)
		if readErr != nil {
			return nil, nil, readErr
		}
		defer clearKeyValues(keyRead.Values)
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
		sourceRead, readErr := repository.readFixedKeys(
			ctx,
			[]string{backuppolicy.BackupSourceKey(source.SourceID)},
			fixedRevision,
		)
		if readErr != nil {
			clearBackupRuntimeMutations(mutations)
			return nil, nil, readErr
		}
		if sourceRead.Values[0] == nil ||
			sourceRead.Values[0].ModRevision != source.SourceRevision {
			clearKeyValues(sourceRead.Values)
			clearBackupRuntimeMutations(mutations)
			return nil, nil, errs.New(errs.KindStateConflict, "backup source snapshot changed")
		}
		storedSource, decodeErr := backuppolicy.DecodeBackupSourceRecord(sourceRead.Values[0].Value)
		clearKeyValues(sourceRead.Values)
		if decodeErr != nil || storedSource.ID != source.SourceID ||
			storedSource.EnvironmentID != run.EnvironmentID ||
			string(
				storedSource.Kind,
			) != string(
				source.Kind,
			) || storedSource.TargetID != source.TargetID {
			clearBackupRuntimeMutations(mutations)
			return nil, nil, errs.New(errs.KindStateConflict, "backup source snapshot changed")
		}
		if err := addCondition(
			backuppolicy.BackupSourceKey(source.SourceID),
			source.SourceRevision,
		); err != nil {
			clearBackupRuntimeMutations(mutations)
			return nil, nil, err
		}
		switch source.Kind {
		case BackupRuntimeSourceAttach:
			snapshot := source.Snapshot.Postgres
			read, readErr := repository.readFixedKeys(ctx, []string{
				attachrecord.AttachKey(source.TargetID),
				attachrecord.AttachFactsKey(source.TargetID),
				hierarchyrecord.ProjectKey(
					snapshot.BackingProjectID,
				),
				hierarchyrecord.EnvironmentKey(snapshot.BackingEnvironmentID),
				environmentBlueprintHeadKey(snapshot.BackingEnvironmentID),
			}, fixedRevision)
			if readErr != nil {
				clearBackupRuntimeMutations(mutations)
				return nil, nil, readErr
			}
			if err := validateBackupPostgresPublicationEvidence(
				read.Values,
				source,
				*snapshot,
			); err != nil {
				clearKeyValues(read.Values)
				clearBackupRuntimeMutations(mutations)
				return nil, nil, err
			}
			clearKeyValues(read.Values)
			for _, fact := range []struct {
				key      string
				revision int64
			}{
				{attachrecord.AttachKey(source.TargetID), source.TargetRevision},
				{attachrecord.AttachFactsKey(source.TargetID), snapshot.AttachFactsRevision},
				{hierarchyrecord.ProjectKey(snapshot.BackingProjectID), snapshot.BackingProjectRevision},
				{hierarchyrecord.EnvironmentKey(snapshot.BackingEnvironmentID), snapshot.BackingEnvironmentRevision},
				{environmentBlueprintHeadKey(snapshot.BackingEnvironmentID), snapshot.BackingServiceRevision},
			} {
				if err := addCondition(fact.key, fact.revision); err != nil {
					clearBackupRuntimeMutations(mutations)
					return nil, nil, err
				}
			}
		case BackupRuntimeSourceVolume:
			snapshot := source.Snapshot.Volume
			keys := []string{
				hierarchyrecord.EnvironmentKey(snapshot.EnvironmentID),
				environmentBlueprintHeadKey(snapshot.EnvironmentID),
				environmentBlueprintRootKey(snapshot.EnvironmentID, snapshot.DesiredRevisionID),
			}
			for _, service := range snapshot.Services {
				keys = append(keys, servicerecord.ServiceRuntimeKey(service.ServiceID))
			}
			read, readErr := repository.readFixedKeys(ctx, keys, fixedRevision)
			if readErr != nil {
				clearBackupRuntimeMutations(mutations)
				return nil, nil, readErr
			}
			if err := validateBackupVolumePublicationEvidence(
				read.Values,
				source,
				*snapshot,
			); err != nil {
				clearKeyValues(read.Values)
				clearBackupRuntimeMutations(mutations)
				return nil, nil, err
			}
			clearKeyValues(read.Values)
			if err := addCondition(hierarchyrecord.EnvironmentKey(snapshot.EnvironmentID), snapshot.EnvironmentRevision); err != nil {
				clearBackupRuntimeMutations(mutations)
				return nil, nil, err
			}
			if err := addCondition(environmentBlueprintHeadKey(snapshot.EnvironmentID), source.TargetRevision); err != nil {
				clearBackupRuntimeMutations(mutations)
				return nil, nil, err
			}
			if err := addCondition(
				environmentBlueprintRootKey(snapshot.EnvironmentID, snapshot.DesiredRevisionID),
				snapshot.ProjectionRoot,
			); err != nil {
				clearBackupRuntimeMutations(mutations)
				return nil, nil, err
			}
			for _, service := range snapshot.Services {
				if err := addCondition(
					servicerecord.ServiceRuntimeKey(service.ServiceID),
					service.ServiceRevision,
				); err != nil {
					clearBackupRuntimeMutations(mutations)
					return nil, nil, err
				}
			}
		case BackupRuntimeSourceConfig:
			snapshot := source.Snapshot.Config
			if retrySource != nil {
				configConditions, configMutations, retryErr := repository.prepareBackupRetryConfigReferences(
					ctx, run, source, retrySource.run.Record, fixedRevision,
				)
				if retryErr != nil {
					clearBackupRuntimeMutations(mutations)
					return nil, nil, retryErr
				}
				for _, condition := range configConditions {
					if err := addCondition(condition.Key, condition.ModRevision); err != nil {
						clearBackupRuntimeMutations(configMutations)
						clearBackupRuntimeMutations(mutations)
						return nil, nil, err
					}
				}
				mutations = append(mutations, configMutations...)
				continue
			}
			if snapshot.ConfigSnapshotID != run.TaskID || snapshot.ReadRevision != fixedRevision {
				clearBackupRuntimeMutations(mutations)
				return nil, nil, errs.New(
					errs.KindStateConflict,
					"backup config snapshot revision changed",
				)
			}
			targetRead, readErr := repository.readFixedKeys(
				ctx,
				[]string{hierarchyrecord.EnvironmentKey(run.EnvironmentID)},
				fixedRevision,
			)
			if readErr != nil {
				clearBackupRuntimeMutations(mutations)
				return nil, nil, readErr
			}
			if targetRead.Values[0] == nil ||
				targetRead.Values[0].ModRevision != source.TargetRevision {
				clearKeyValues(targetRead.Values)
				clearBackupRuntimeMutations(mutations)
				return nil, nil, errs.New(errs.KindStateConflict, "backup config target changed")
			}
			environment, decodeErr := hierarchyrecord.DecodeEnvironment(targetRead.Values[0].Value)
			clearKeyValues(targetRead.Values)
			if decodeErr != nil || environment.ID != run.EnvironmentID {
				clearBackupRuntimeMutations(mutations)
				return nil, nil, backupruntime.CorruptBackupRuntimeRecord()
			}
			config := BackupConfigSnapshotRecord{
				SnapshotID:    snapshot.ConfigSnapshotID,
				EnvironmentID: run.EnvironmentID,
				SourceID:      source.SourceID,
				State:         BackupConfigSnapshotBuilding,
				ReadRevision:  snapshot.ReadRevision,
				CreatedAt:     run.CreatedAt,
				UpdatedAt:     run.CreatedAt,
			}
			value, encodeErr := encodeBackupConfigSnapshotRecord(config)
			if encodeErr != nil {
				clearBackupRuntimeMutations(mutations)
				return nil, nil, encodeErr
			}
			mutations = append(
				mutations,
				etcdstore.Mutation{
					Type:  etcdstore.MutationPut,
					Key:   backupConfigSnapshotKey(snapshot.ConfigSnapshotID),
					Value: value,
				},
				etcdstore.Mutation{
					Type: etcdstore.MutationPut,
					Key: backupConfigSnapshotTaskReferenceKey(
						run.TaskID,
						snapshot.ConfigSnapshotID,
					),
					Value: []byte(snapshot.ConfigSnapshotID),
				},
				etcdstore.Mutation{
					Type: etcdstore.MutationPut,
					Key: backupConfigSnapshotReferenceTaskKey(
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

func validateBackupPostgresPublicationEvidence(
	values []*etcdstore.KeyValue,
	source backupruntime.BackupRunSourceAttemptRecord,
	snapshot backupruntime.BackupPostgresSourceSnapshot,
) error {
	if len(values) != 5 || values[0] == nil || values[1] == nil || values[2] == nil ||
		values[3] == nil || values[4] == nil || values[0].ModRevision != source.TargetRevision ||
		values[1].ModRevision != snapshot.AttachFactsRevision ||
		values[2].ModRevision != snapshot.BackingProjectRevision ||
		values[3].ModRevision != snapshot.BackingEnvironmentRevision ||
		values[4].ModRevision != snapshot.BackingServiceRevision {
		return errs.New(errs.KindStateConflict, "postgres backup publication evidence changed")
	}
	attach, attachErr := attachrecord.DecodeAttachRecord(values[0].Value)
	facts, factsErr := attachrecord.DecodeAttachEncryptedFacts(values[1].Value)
	defer clear(facts.Ciphertext)
	project, projectErr := hierarchyrecord.DecodeProject(values[2].Value)
	environment, environmentErr := hierarchyrecord.DecodeEnvironment(values[3].Value)
	if attachErr != nil || factsErr != nil || projectErr != nil || environmentErr != nil {
		return backupruntime.CorruptBackupRuntimeRecord()
	}
	if attach.ID != source.TargetID || attach.EnvironmentID != snapshot.ConsumerEnvironmentID ||
		string(attach.Status) != "ready" || attach.BackingProjectID != snapshot.BackingProjectID ||
		attach.BackingEnvironmentID != snapshot.BackingEnvironmentID ||
		attach.BackingServiceID != snapshot.BackingServiceID || facts.AttachID != source.TargetID ||
		project.ID != snapshot.BackingProjectID || project.Kind != hierarchyrecord.ProjectKindBacking ||
		environment.ID != snapshot.BackingEnvironmentID || environment.ProjectID != project.ID {
		return errs.New(errs.KindStateConflict, "postgres backup publication evidence changed")
	}
	return nil
}

func validateBackupVolumePublicationEvidence(
	values []*etcdstore.KeyValue,
	source backupruntime.BackupRunSourceAttemptRecord,
	snapshot backupruntime.BackupVolumeSourceSnapshot,
) error {
	const offset = 3
	if len(values) != len(snapshot.Services)+offset || values[0] == nil || values[1] == nil || values[2] == nil ||
		values[0].ModRevision != snapshot.EnvironmentRevision ||
		values[1].ModRevision != source.TargetRevision || values[2].ModRevision != snapshot.ProjectionRoot {
		return errs.New(errs.KindStateConflict, "volume backup publication evidence changed")
	}
	environment, environmentErr := hierarchyrecord.DecodeEnvironment(values[0].Value)
	revisionID, headErr := idempotencyrecord.DecodeTaskReference(values[1].Value)
	seal, sealErr := decodeEnvironmentBlueprintSeal(values[2].Value)
	if environmentErr != nil || headErr != nil || sealErr != nil {
		return backupruntime.CorruptBackupRuntimeRecord()
	}
	if environment.ID != snapshot.EnvironmentID || environment.VolumeDir != snapshot.AuthorizedVolumeDir ||
		revisionID != snapshot.DesiredRevisionID || seal.EnvironmentID != snapshot.EnvironmentID ||
		seal.RevisionID != snapshot.DesiredRevisionID || seal.RenderGeneration != snapshot.RenderGeneration ||
		hex.EncodeToString(seal.DependencyDigest[:]) != snapshot.DependencyDigest ||
		snapshot.VolumeID != source.TargetID || snapshot.DockerVolumeName != "gp_vol_"+source.TargetID {
		return errs.New(errs.KindStateConflict, "volume projection evidence changed")
	}
	for index, expected := range snapshot.Services {
		value := values[index+offset]
		if expected.ServiceRevision == 0 {
			if value != nil || expected.PriorIntent != backupruntime.BackupServiceIntentRunning {
				return errs.New(errs.KindStateConflict, "volume service publication evidence changed")
			}
			continue
		}
		if value == nil || value.ModRevision != expected.ServiceRevision {
			return errs.New(errs.KindStateConflict, "volume service publication evidence changed")
		}
		service, decodeErr := servicerecord.DecodeServiceRuntimeRecord(value.Value)
		if decodeErr != nil {
			return backupruntime.CorruptBackupRuntimeRecord()
		}
		if service.ServiceID != expected.ServiceID ||
			service.EnvironmentID != snapshot.EnvironmentID ||
			string(service.Runtime.RuntimeIntent) != string(expected.PriorIntent) {
			return errs.New(errs.KindStateConflict, "volume service publication evidence changed")
		}
	}
	return nil
}

func equalBackupMountPaths(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
