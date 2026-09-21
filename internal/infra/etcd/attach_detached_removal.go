package etcd

import (
	"context"
	"github.com/AlanD20/groundplane/internal/core"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *AttachRepository) DeleteDetachedAttach(
	ctx context.Context,
	current etcdstore.Versioned[attachrecord.Record],
) (int64, error) {
	if err := validateAttachVersion(current); err != nil {
		return 0, err
	}
	if current.Record.Status != core.AttachDetached {
		return 0, errs.New(errs.KindStateConflict, "Attach must be detached before record removal")
	}
	revision := current.ReadRevision
	if revision == 0 {
		revision = current.Revision
	}
	conditions, mutations, values, err := prepareAttachRemoval(ctx, repository.store, current, revision)
	if err != nil {
		return 0, err
	}
	defer func() {
		for _, value := range values {
			clear(value)
		}
	}()
	result, err := repository.store.Transact(ctx, conditions, mutations)
	if err != nil {
		return 0, err
	}
	if !result.Succeeded {
		return 0, errs.New(errs.KindStateConflict, "Attach references changed concurrently")
	}
	return result.Revision, nil
}

func prepareAttachRemoval(
	ctx context.Context,
	store attachRemovalStore,
	current etcdstore.Versioned[attachrecord.Record],
	revision int64,
) ([]etcdstore.Condition, []etcdstore.Mutation, [][]byte, error) {
	if err := validateAttachVersion(current); err != nil {
		return nil, nil, nil, err
	}
	if revision <= 0 {
		return nil, nil, nil, errs.New(errs.KindValidationFailed, "Attach removal revision must be positive")
	}
	exclusionCondition, err := requireAttachBackupSourceExclusionAbsent(
		ctx, store, current.Record.ID, revision,
	)
	if err != nil {
		return nil, nil, nil, err
	}
	dependents, err := store.Range(ctx, etcdstore.RangeRequest{
		Prefix: attachrecord.AttachGrantedByPrefix(current.Record.ID), Limit: 1, Revision: revision,
	})
	if err != nil {
		return nil, nil, nil, err
	}
	if dependents == nil || dependents.ReadRevision != revision {
		return nil, nil, nil, errs.New(errs.KindInternal, "Attach grant index read returned an invalid revision")
	}
	if len(dependents.Values) != 0 {
		return nil, nil, nil, errs.New(errs.KindResourceInUse, "Attach is referenced by another Attach grant")
	}
	if dependents != nil {
		defer clearRangeValues(dependents.Values)
	}
	credentialDependents, err := store.Range(ctx, etcdstore.RangeRequest{
		Prefix: attachrecord.AttachCredentialByPrefix(current.Record.ID), Limit: 1, Revision: revision,
	})
	if err != nil {
		return nil, nil, nil, err
	}
	if credentialDependents == nil || credentialDependents.ReadRevision != revision {
		return nil, nil, nil, errs.New(errs.KindInternal, "Attach credential index read returned an invalid revision")
	}
	if len(credentialDependents.Values) != 0 {
		return nil, nil, nil, errs.New(errs.KindResourceInUse, "Attach credential is used by another Service")
	}
	defer clearRangeValues(credentialDependents.Values)
	dependentGrants, err := store.Range(ctx, etcdstore.RangeRequest{
		Prefix: attachrecord.AttachDependentGrantPrefix(current.Record.ID),
		Limit:  2, Revision: revision,
	})
	if err != nil {
		return nil, nil, nil, err
	}
	if dependentGrants == nil || dependentGrants.ReadRevision != revision ||
		dependentGrants.More || len(dependentGrants.Values) > 1 {
		return nil, nil, nil, attachrecord.CorruptAttachRecord()
	}
	defer clearRangeValues(dependentGrants.Values)
	if err := attachrecord.ValidateAttachDependentGrantRange(
		dependentGrants, current.Record.ID, current.Record.GrantAttachIDs,
	); err != nil {
		return nil, nil, nil, err
	}
	if (len(current.Record.GrantAttachIDs) == 0 && len(dependentGrants.Values) != 0) ||
		(len(current.Record.GrantAttachIDs) != 0 && len(dependentGrants.Values) != 1) {
		return nil, nil, nil, attachrecord.CorruptAttachRecord()
	}

	conditions := []etcdstore.Condition{
		{Key: attachrecord.AttachKey(current.Record.ID), ModRevision: current.Revision},
		{Key: deletionTombstoneKey("attach", current.Record.ID)},
		exclusionCondition,
		{Key: attachrecord.AttachGrantedByPrefix(current.Record.ID), Prefix: true},
		{Key: attachrecord.AttachCredentialByPrefix(current.Record.ID), Prefix: true},
	}
	mutations := []etcdstore.Mutation{
		{Type: etcdstore.MutationDelete, Key: attachrecord.AttachKey(current.Record.ID)},
		{Type: etcdstore.MutationDelete, Key: attachrecord.AttachNameKey(current.Record.EnvironmentID, current.Record.Name)},
		{Type: etcdstore.MutationDelete, Key: attachrecord.AttachOwnerKey(current.Record.EnvironmentID, current.Record.ID)},
		{Type: etcdstore.MutationDelete, Key: attachrecord.AttachBackingServiceKey(current.Record.BackingServiceID, current.Record.ID)},
		{Type: etcdstore.MutationDelete, Key: attachrecord.AttachBackingProjectKey(current.Record.BackingProjectID, current.Record.ID)},
		{Type: etcdstore.MutationDelete, Key: attachrecord.AttachFactsKey(current.Record.ID)},
	}
	mutations = append(mutations, etcdstore.Mutation{
		Type: etcdstore.MutationDelete, Key: attachrecord.AttachServiceKey(current.Record.ServiceID, current.Record.ID),
	})
	if len(current.Record.GrantAttachIDs) != 0 {
		dependent := dependentGrants.Values[0]
		conditions = append(conditions, etcdstore.Condition{
			Key: attachrecord.AttachDependentGrantKey(current.Record.ID), ModRevision: dependent.ModRevision,
		})
		mutations = append(mutations, etcdstore.Mutation{
			Type: etcdstore.MutationDelete, Key: attachrecord.AttachDependentGrantKey(current.Record.ID),
		})
	}
	values := make([][]byte, 0, len(current.Record.GrantAttachIDs)+1)
	if !current.Record.OwnsCredential() {
		keys := []string{
			attachrecord.AttachKey(current.Record.CredentialAttachID),
			attachrecord.AttachCredentialByKey(current.Record.CredentialAttachID, current.Record.ID),
		}
		result, readErr := store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: revision})
		if readErr != nil {
			return nil, nil, values, readErr
		}
		if result == nil || result.ReadRevision != revision || len(result.Values) != 2 ||
			result.Values[0] == nil || result.Values[1] == nil ||
			string(result.Values[1].Value) != current.Record.ID {
			return nil, nil, values, attachrecord.CorruptAttachRecord()
		}
		owner, decodeErr := attachrecord.DecodeAttachRecord(result.Values[0].Value)
		if decodeErr != nil || owner.ID != current.Record.CredentialAttachID || !owner.OwnsCredential() {
			return nil, nil, values, attachrecord.CorruptAttachRecord()
		}
		ownerValue, encodeErr := attachrecord.EncodeAttachRecord(owner)
		if encodeErr != nil {
			return nil, nil, values, encodeErr
		}
		values = append(values, ownerValue)
		conditions = append(conditions,
			etcdstore.Condition{Key: attachrecord.AttachKey(owner.ID), ModRevision: result.Values[0].ModRevision},
			etcdstore.Condition{Key: keys[1], ModRevision: result.Values[1].ModRevision},
		)
		mutations = append(mutations,
			etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: keys[1]},
			etcdstore.Mutation{Type: etcdstore.MutationPut, Key: attachrecord.AttachKey(owner.ID), Value: ownerValue},
		)
		etcdstore.ClearValues(result.Values)
	}
	if len(current.Record.GrantAttachIDs) == 0 {
		return conditions, mutations, values, nil
	}
	keys := make([]string, 0, len(current.Record.GrantAttachIDs)*2)
	for _, grantID := range current.Record.GrantAttachIDs {
		keys = append(keys, attachrecord.AttachKey(grantID), attachrecord.AttachGrantedByKey(grantID, current.Record.ID))
	}
	result, err := store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return nil, nil, nil, err
	}
	if result == nil || result.ReadRevision != revision || len(result.Values) != len(keys) {
		return nil, nil, nil, attachrecord.CorruptAttachRecord()
	}
	for index, grantID := range current.Record.GrantAttachIDs {
		target := result.Values[index*2]
		reverse := result.Values[index*2+1]
		if target == nil || reverse == nil || string(reverse.Value) != current.Record.ID {
			return nil, nil, values, attachrecord.CorruptAttachRecord()
		}
		targetRecord, decodeErr := attachrecord.DecodeAttachRecord(target.Value)
		if decodeErr != nil || targetRecord.ID != grantID {
			return nil, nil, values, attachrecord.CorruptAttachRecord()
		}
		targetValue, encodeErr := attachrecord.EncodeAttachRecord(targetRecord)
		if encodeErr != nil {
			return nil, nil, values, encodeErr
		}
		values = append(values, targetValue)
		conditions = append(conditions,
			etcdstore.Condition{Key: attachrecord.AttachKey(grantID), ModRevision: target.ModRevision},
			etcdstore.Condition{Key: attachrecord.AttachGrantedByKey(grantID, current.Record.ID), ModRevision: reverse.ModRevision},
		)
		mutations = append(mutations,
			etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: attachrecord.AttachGrantedByKey(grantID, current.Record.ID)},
			etcdstore.Mutation{Type: etcdstore.MutationPut, Key: attachrecord.AttachKey(grantID), Value: targetValue},
		)
	}
	return conditions, mutations, values, nil
}

func requireAttachBackupSourceExclusionAbsent(
	ctx context.Context,
	store interface {
		GetMany(context.Context, etcdstore.GetManyRequest) (*etcdstore.GetManyResult, error)
	},
	attachID string,
	revision int64,
) (etcdstore.Condition, error) {
	key, err := backupruntime.BackupSourceTargetExclusionKey(backupruntime.BackupSourceTargetAttach, attachID)
	if err != nil {
		return etcdstore.Condition{}, err
	}
	result, err := store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{key}, Revision: revision})
	if err != nil {
		return etcdstore.Condition{}, err
	}
	if result == nil || result.ReadRevision != revision || len(result.Values) != 1 {
		return etcdstore.Condition{}, errs.New(errs.KindInternal, "attach backup source exclusion read is incomplete")
	}
	if result.Values[0] != nil {
		if evidenceErr := classifyAttachBackupSourceExclusionEvidence(result.Values[0], attachID); evidenceErr != nil {
			return etcdstore.Condition{}, evidenceErr
		}
	}
	return etcdstore.Condition{Key: key}, nil
}

func classifyAttachBackupSourceExclusionEvidence(evidence *etcdstore.KeyValue, attachID string) error {
	if evidence == nil {
		return nil
	}
	expectedKey, err := backupruntime.BackupSourceTargetExclusionKey(backupruntime.BackupSourceTargetAttach, attachID)
	if err != nil || evidence.Key != expectedKey {
		return errs.New(errs.KindInternal, "attach backup source exclusion is misbucketed")
	}
	exclusion, decodeErr := backupruntime.DecodeBackupSourceTargetExclusionRecord(evidence.Value)
	if decodeErr != nil || exclusion.TargetKind != backupruntime.BackupSourceTargetAttach || exclusion.TargetID != attachID {
		return errs.New(errs.KindInternal, "attach backup source exclusion is corrupt")
	}
	return errs.New(errs.KindResourceInUse, "attach is an active backup source")
}
