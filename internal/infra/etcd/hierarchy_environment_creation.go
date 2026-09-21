package etcd

import (
	"context"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	hierarchydeletion "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	scriptrecord "github.com/AlanD20/groundplane/internal/infra/etcd/scripts"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *HierarchyRepository) CreateEnvironment(
	ctx context.Context,
	record hierarchyrecord.EnvironmentRecord,
) (etcdstore.Versioned[hierarchyrecord.EnvironmentRecord], error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[hierarchyrecord.EnvironmentRecord]{}, err
	}
	if err := hierarchyrecord.ValidateEnvironment(record); err != nil {
		return etcdstore.Versioned[hierarchyrecord.EnvironmentRecord]{}, err
	}
	owner, err := repository.GetProject(ctx, record.ProjectID)
	if err != nil {
		return etcdstore.Versioned[hierarchyrecord.EnvironmentRecord]{}, err
	}
	if owner.Record.Kind == hierarchyrecord.ProjectKindBacking {
		return etcdstore.Versioned[hierarchyrecord.EnvironmentRecord]{}, errs.New(
			errs.KindValidationFailed,
			"backing project environments are created by the backing-service workflow",
		)
	}
	value, err := hierarchyrecord.EncodeEnvironment(record)
	if err != nil {
		return etcdstore.Versioned[hierarchyrecord.EnvironmentRecord]{}, err
	}
	epochValue, err := backupruntime.EncodeEnvironmentMutationEpochRecord(backupruntime.EnvironmentMutationEpochRecord{
		EnvironmentID: record.ID,
	})
	if err != nil {
		return etcdstore.Versioned[hierarchyrecord.EnvironmentRecord]{}, err
	}
	coordinationValue, err := encodeInitialHierarchyCoordination(hierarchydeletion.HierarchyDeletionTargetEnvironment, record.ID)
	if err != nil {
		return etcdstore.Versioned[hierarchyrecord.EnvironmentRecord]{}, err
	}
	scriptSetValue, err := scriptrecord.EncodeScriptSetGeneration(scriptrecord.SetGenerationRecord{
		EnvironmentID: record.ID, GenerationID: record.ID,
	})
	if err != nil {
		return etcdstore.Versioned[hierarchyrecord.EnvironmentRecord]{}, err
	}
	defer clear(scriptSetValue)
	primary := hierarchyrecord.EnvironmentKey(record.ID)
	label := hierarchyrecord.EnvironmentNameKey(record.ProjectID, record.Name)
	ownerIndex := hierarchyrecord.EnvironmentOwnerKey(record.ProjectID, record.ID)
	epochKey := hierarchyrecord.EnvironmentMutationEpochKey(record.ID)
	coordinationKey := hierarchydeletion.HierarchyCoordinationKey(string(hierarchydeletion.HierarchyDeletionTargetEnvironment), record.ID)
	scriptSetKey := scriptrecord.ScriptSetActiveKey(record.ID)
	result, err := repository.store.Transact(ctx,
		[]etcdstore.Condition{
			{Key: primary},
			{Key: label},
			{Key: ownerIndex},
			{Key: hierarchyrecord.ProjectKey(record.ProjectID), ModRevision: owner.Revision},
			{Key: epochKey},
			{Key: coordinationKey},
			{Key: scriptSetKey},
		},
		[]etcdstore.Mutation{
			{Type: etcdstore.MutationPut, Key: primary, Value: value},
			{Type: etcdstore.MutationPut, Key: label, Value: []byte(record.ID)},
			{Type: etcdstore.MutationPut, Key: ownerIndex, Value: []byte(record.ID)},
			{Type: etcdstore.MutationPut, Key: epochKey, Value: epochValue},
			{Type: etcdstore.MutationPut, Key: coordinationKey, Value: coordinationValue},
			{Type: etcdstore.MutationPut, Key: scriptSetKey, Value: scriptSetValue},
		},
	)
	if err != nil {
		return etcdstore.Versioned[hierarchyrecord.EnvironmentRecord]{}, err
	}
	if !result.Succeeded {
		if len(result.FailureReads) != 7 {
			return etcdstore.Versioned[hierarchyrecord.EnvironmentRecord]{}, errs.New(
				errs.KindInternal,
				"environment creation compare evidence is incomplete",
			)
		}
		if result.FailureReads[4] != nil {
			return etcdstore.Versioned[hierarchyrecord.EnvironmentRecord]{}, errs.New(
				errs.KindInternal,
				"environment creation collided with mutation epoch state",
			)
		}
		if result.FailureReads[5] != nil {
			return etcdstore.Versioned[hierarchyrecord.EnvironmentRecord]{}, errs.New(
				errs.KindInternal,
				"environment creation collided with hierarchy coordination state",
			)
		}
		return etcdstore.Versioned[hierarchyrecord.EnvironmentRecord]{}, repository.diagnoseCreate(ctx, primary, label)
	}
	return etcdstore.Versioned[hierarchyrecord.EnvironmentRecord]{Record: record, Revision: result.Revision, ReadRevision: result.Revision}, nil
}
