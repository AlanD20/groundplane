package etcd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	hierarchydeletion "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *HierarchyDeletionRepository) readDeletionRoot(
	ctx context.Context,
	targetKind hierarchydeletion.HierarchyDeletionTargetKind,
	targetID string,
) (hierarchyDeletionRoot, int64, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return hierarchyDeletionRoot{}, 0, err
	}
	key := hierarchyDeletionPrimaryKey(targetKind, targetID)
	result, err := repository.store.Get(ctx, key)
	if err != nil {
		return hierarchyDeletionRoot{}, 0, err
	}
	if result == nil || result.Entry == nil || result.ReadRevision <= 0 || result.Entry.ModRevision <= 0 {
		return hierarchyDeletionRoot{}, 0, hierarchyDeletionNotFound(targetKind)
	}
	root := hierarchyDeletionRoot{
		targetKey: key, targetRevision: result.Entry.ModRevision,
		primaryFences: []etcdstore.Condition{{Key: key, ModRevision: result.Entry.ModRevision}},
	}
	root.targetValue = append([]byte(nil), result.Entry.Value...)
	switch targetKind {
	case hierarchydeletion.HierarchyDeletionTargetTenant:
		tenant, decodeErr := hierarchyrecord.DecodeTenant(result.Entry.Value)
		if decodeErr != nil || tenant.ID != targetID || tenant.DeletionTaskID != "" {
			return hierarchyDeletionRoot{}, 0, hierarchyDeletionUnavailable(targetKind)
		}
		root.rootSlug = tenant.Slug
		root.workspace = hierarchydeletion.HierarchyDeletionWorkspace{Type: "tenant", TenantID: tenant.ID}
		root.owner, err = taskjournal.TenantTaskOwner(tenant.ID)
		root.coordinationKeys = []string{hierarchydeletion.HierarchyCoordinationKey(string(targetKind), targetID)}
	case hierarchydeletion.HierarchyDeletionTargetProject:
		project, decodeErr := hierarchyrecord.DecodeProject(result.Entry.Value)
		if decodeErr != nil || project.ID != targetID || project.DeletionTaskID != "" {
			return hierarchyDeletionRoot{}, 0, hierarchyDeletionUnavailable(targetKind)
		}
		root.rootSlug = project.Slug
		root.owner, err = taskjournal.ProjectTaskOwner(project)
		if project.Kind == hierarchyrecord.ProjectKindBacking {
			root.workspace = hierarchydeletion.HierarchyDeletionWorkspace{Type: "platform"}
		} else {
			root.workspace = hierarchydeletion.HierarchyDeletionWorkspace{Type: "tenant", TenantID: project.TenantID}
		}
		root.coordinationKeys = []string{hierarchydeletion.HierarchyCoordinationKey(string(targetKind), targetID)}
		if project.TenantID != "" {
			root, err = repository.readProjectParentAtRevision(ctx, result.ReadRevision, project, root)
		}
	case hierarchydeletion.HierarchyDeletionTargetBacking:
		project, decodeErr := hierarchyrecord.DecodeProject(result.Entry.Value)
		if decodeErr != nil || project.ID != targetID || project.Kind != hierarchyrecord.ProjectKindBacking ||
			project.TenantID != "" || project.DeletionTaskID != "" {
			return hierarchyDeletionRoot{}, 0, hierarchyDeletionUnavailable(targetKind)
		}
		root.rootSlug = project.Slug
		root.workspace = hierarchydeletion.HierarchyDeletionWorkspace{Type: "platform"}
		root.owner, err = taskjournal.ProjectTaskOwner(project)
		root.coordinationKeys = []string{hierarchydeletion.HierarchyCoordinationKey(string(hierarchydeletion.HierarchyDeletionTargetProject), targetID)}
	case hierarchydeletion.HierarchyDeletionTargetEnvironment:
		environment, decodeErr := hierarchyrecord.DecodeEnvironment(result.Entry.Value)
		if decodeErr != nil || environment.ID != targetID || environment.DeletionTaskID != "" {
			return hierarchyDeletionRoot{}, 0, hierarchyDeletionUnavailable(targetKind)
		}
		root.rootSlug = environment.Name
		root, err = repository.readEnvironmentParentsAtRevision(ctx, result.ReadRevision, environment, root)
	default:
		return hierarchyDeletionRoot{}, 0, errs.New(
			errs.KindValidationFailed,
			"hierarchy deletion target kind is invalid",
		)
	}
	if err != nil {
		clear(root.targetValue)
		return hierarchyDeletionRoot{}, 0, err
	}
	coordination, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: root.coordinationKeys, Revision: result.ReadRevision,
	})
	if err != nil {
		clear(root.targetValue)
		return hierarchyDeletionRoot{}, 0, err
	}
	if coordination == nil || coordination.ReadRevision != result.ReadRevision ||
		len(coordination.Values) != len(root.coordinationKeys) {
		clear(root.targetValue)
		return hierarchyDeletionRoot{}, 0, hierarchydeletion.CorruptHierarchyDeletion()
	}
	root.coordination = make([]etcdstore.Versioned[hierarchydeletion.HierarchyCoordinationRecord], len(root.coordinationKeys))
	for index, value := range coordination.Values {
		if value == nil || value.Key != root.coordinationKeys[index] || value.ModRevision <= 0 {
			clear(root.targetValue)
			return hierarchyDeletionRoot{}, 0, hierarchydeletion.CorruptHierarchyDeletion()
		}
		record, decodeErr := hierarchydeletion.DecodeHierarchyCoordination(value.Value)
		if decodeErr != nil {
			clear(root.targetValue)
			return hierarchyDeletionRoot{}, 0, decodeErr
		}
		root.coordination[index] = etcdstore.Versioned[hierarchydeletion.HierarchyCoordinationRecord]{
			Record: record, Revision: value.ModRevision, ReadRevision: result.ReadRevision,
		}
	}
	if err := repository.requireHierarchyDeletionDescendantsAvailable(
		ctx,
		result.ReadRevision,
		targetKind,
		targetID,
	); err != nil {
		clear(root.targetValue)
		return hierarchyDeletionRoot{}, 0, err
	}
	if err := repository.requireHierarchyVolumeRemovalAbsent(ctx, result.ReadRevision, targetKind, targetID); err != nil {
		clear(root.targetValue)
		return hierarchyDeletionRoot{}, 0, err
	}
	return root, result.ReadRevision, nil
}

func (repository *HierarchyDeletionRepository) readProjectParentAtRevision(
	ctx context.Context,
	revision int64,
	project hierarchyrecord.ProjectRecord,
	root hierarchyDeletionRoot,
) (hierarchyDeletionRoot, error) {
	parents, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{hierarchyrecord.TenantKey(project.TenantID)}, Revision: revision,
	})
	if err != nil {
		return hierarchyDeletionRoot{}, err
	}
	if parents == nil || parents.ReadRevision != revision || len(parents.Values) != 1 || parents.Values[0] == nil {
		return hierarchyDeletionRoot{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	tenant, err := hierarchyrecord.DecodeTenant(parents.Values[0].Value)
	if err != nil || tenant.ID != project.TenantID || tenant.DeletionTaskID != "" {
		return hierarchyDeletionRoot{}, hierarchyDeletionUnavailable(hierarchydeletion.HierarchyDeletionTargetProject)
	}
	root.primaryFences = append(root.primaryFences, etcdstore.Condition{
		Key: hierarchyrecord.TenantKey(tenant.ID), ModRevision: parents.Values[0].ModRevision,
	})
	root.coordinationKeys = append([]string{
		hierarchydeletion.HierarchyCoordinationKey(string(hierarchydeletion.HierarchyDeletionTargetTenant), tenant.ID),
	}, root.coordinationKeys...)
	return root, nil
}

func (repository *HierarchyDeletionRepository) readEnvironmentParentsAtRevision(
	ctx context.Context,
	revision int64,
	environment hierarchyrecord.EnvironmentRecord,
	root hierarchyDeletionRoot,
) (hierarchyDeletionRoot, error) {
	projectResult, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{hierarchyrecord.ProjectKey(environment.ProjectID)}, Revision: revision,
	})
	if err != nil {
		return hierarchyDeletionRoot{}, err
	}
	if projectResult == nil || projectResult.ReadRevision != revision || len(projectResult.Values) != 1 ||
		projectResult.Values[0] == nil {
		return hierarchyDeletionRoot{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	project, err := hierarchyrecord.DecodeProject(projectResult.Values[0].Value)
	if err != nil || project.ID != environment.ProjectID || project.DeletionTaskID != "" {
		return hierarchyDeletionRoot{}, hierarchyDeletionUnavailable(hierarchydeletion.HierarchyDeletionTargetEnvironment)
	}
	root.primaryFences = append(root.primaryFences, etcdstore.Condition{
		Key: hierarchyrecord.ProjectKey(project.ID), ModRevision: projectResult.Values[0].ModRevision,
	})
	root.owner, err = taskjournal.EnvironmentTaskOwner(project, environment)
	if err != nil {
		return hierarchyDeletionRoot{}, err
	}
	root.coordinationKeys = []string{
		hierarchydeletion.HierarchyCoordinationKey(string(hierarchydeletion.HierarchyDeletionTargetProject), project.ID),
		hierarchydeletion.HierarchyCoordinationKey(string(hierarchydeletion.HierarchyDeletionTargetEnvironment), environment.ID),
	}
	if project.TenantID != "" {
		root, err = repository.readProjectParentAtRevision(ctx, revision, project, root)
		if err != nil {
			return hierarchyDeletionRoot{}, err
		}
		root.workspace = hierarchydeletion.HierarchyDeletionWorkspace{Type: "tenant", TenantID: project.TenantID}
	} else {
		root.workspace = hierarchydeletion.HierarchyDeletionWorkspace{Type: "platform"}
	}
	return root, nil
}

func prepareHierarchyDeletionPublication(
	begin HierarchyDeletionBegin,
	root hierarchyDeletionRoot,
	snapshotRevision int64,
) (HierarchyDeletionOperation, []etcdstore.Condition, []etcdstore.Mutation, TaskInitiation, error) {
	if begin.OperationKind == hierarchydeletion.HierarchyDeletionOperationProject && root.workspace.Type == "platform" ||
		begin.OperationKind == hierarchydeletion.HierarchyDeletionOperationBacking && root.workspace.Type != "platform" {
		return HierarchyDeletionOperation{}, nil, nil, TaskInitiation{}, errs.New(
			errs.KindValidationFailed,
			"hierarchy deletion Project kind must use its exclusive permanent-delete authority",
		)
	}
	targetCoordinationIndex := -1
	for index, record := range root.coordination {
		kindMatches := record.Record.TargetKind == begin.TargetKind ||
			(begin.TargetKind == hierarchydeletion.HierarchyDeletionTargetBacking && record.Record.TargetKind == hierarchydeletion.HierarchyDeletionTargetProject)
		if kindMatches && record.Record.TargetID == begin.TargetID {
			targetCoordinationIndex = index
			break
		}
	}
	if targetCoordinationIndex < 0 {
		return HierarchyDeletionOperation{}, nil, nil, TaskInitiation{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	deletionEpoch := root.coordination[targetCoordinationIndex].Record.MutationEpoch + 1
	tombstoneKey := hierarchydeletion.HierarchyDeletionTombstoneKey(string(begin.TargetKind), begin.TargetID)
	lockKey := hierarchydeletion.HierarchyDeletionLockKey(string(begin.TargetKind), begin.TargetID)
	replayKey, err := hierarchydeletion.HierarchyDeletionReplayTargetKey(begin.OperationID)
	if err != nil {
		return HierarchyDeletionOperation{}, nil, nil, TaskInitiation{}, err
	}
	fenceKey, err := hierarchydeletion.HierarchyDeletionCleanupFenceKey(begin.OperationID)
	if err != nil {
		return HierarchyDeletionOperation{}, nil, nil, TaskInitiation{}, err
	}
	intentKey, err := hierarchydeletion.HierarchyDeletionIntentKey(begin.OperationID)
	if err != nil {
		return HierarchyDeletionOperation{}, nil, nil, TaskInitiation{}, err
	}
	initialDigest := sha256.Sum256([]byte("gp-deletion-completed-prefix-v1\x00"))
	tombstone := hierarchydeletion.HierarchyDeletionTombstone{
		Schema: 1, TargetKind: begin.TargetKind, TargetID: begin.TargetID,
		TargetRevision: root.targetRevision, OperationKind: begin.OperationKind,
		OperationID: begin.OperationID, TaskOperationID: begin.TaskOperationID,
		DeletionEpoch: deletionEpoch, CurrentTaskID: begin.TaskID, Workspace: root.workspace,
		SnapshotRevision: snapshotRevision, Phase: hierarchydeletion.HierarchyDeletionPlanning,
		Checkpoint: hierarchydeletion.HierarchyDeletionCheckpoint{CompletedPrefixDigest: hex.EncodeToString(initialDigest[:])},
		CreatedAt:  begin.CreatedAt, AttemptDeadline: begin.DeadlineAt,
	}
	intent := hierarchydeletion.HierarchyDeletionIntent{
		Schema: 1, OperationKind: begin.OperationKind, OperationID: begin.OperationID,
		TaskOperationID: begin.TaskOperationID, DeletionEpoch: deletionEpoch,
		TargetKind: begin.TargetKind, TargetID: begin.TargetID, TargetRevision: root.targetRevision,
		Workspace: root.workspace, RootSlug: root.rootSlug, SnapshotRevision: snapshotRevision,
		TimeoutSeconds: int64(hierarchyDeletionAttemptTimeout / time.Second), CreatedAt: begin.CreatedAt,
	}
	lock := hierarchydeletion.HierarchyDeletionLock{
		Schema: 1, TargetKind: begin.TargetKind, TargetID: begin.TargetID,
		ParentOperationID: begin.OperationID, DeletionEpoch: deletionEpoch,
		TombstoneKey: tombstoneKey, CleanupFenceKey: fenceKey, CreatedAt: begin.CreatedAt,
	}
	replay := hierarchydeletion.HierarchyDeletionReplayLocator{
		Schema: 1, ParentOperationID: begin.OperationID, OperationKind: begin.OperationKind,
		TargetKind: begin.TargetKind, TargetID: begin.TargetID, DeletionEpoch: deletionEpoch,
		RootTaskID: begin.TaskID, CurrentTaskID: begin.TaskID,
		ResponseDigest: hierarchydeletion.HierarchyDeletionDigest(begin.Marker.Response.Body), TombstoneKey: tombstoneKey,
	}
	fence := hierarchydeletion.HierarchyDeletionCleanupFence{
		Schema: 1, ParentOperationID: begin.OperationID, DeletionEpoch: deletionEpoch,
		TargetKind: begin.TargetKind, TargetID: begin.TargetID, Generation: 1,
		Phase: hierarchydeletion.HierarchyDeletionPlanning, CurrentTaskID: begin.TaskID,
		Dispatch: hierarchydeletion.HierarchyDeletionDispatchOpen, UpdatedAt: begin.CreatedAt,
	}
	tombstoneValue, err := hierarchydeletion.EncodeHierarchyDeletionRecord(tombstone, hierarchydeletion.HierarchyDeletionLargeRecordBytes)
	if err != nil {
		return HierarchyDeletionOperation{}, nil, nil, TaskInitiation{}, err
	}
	intentValue, err := hierarchydeletion.EncodeHierarchyDeletionRecord(intent, hierarchydeletion.HierarchyDeletionLargeRecordBytes)
	if err != nil {
		clear(tombstoneValue)
		return HierarchyDeletionOperation{}, nil, nil, TaskInitiation{}, err
	}
	lockValue, err := hierarchydeletion.EncodeHierarchyDeletionRecord(lock, hierarchydeletion.HierarchyDeletionSmallRecordBytes)
	if err != nil {
		clear(tombstoneValue)
		clear(intentValue)
		return HierarchyDeletionOperation{}, nil, nil, TaskInitiation{}, err
	}
	replayValue, err := hierarchydeletion.EncodeHierarchyDeletionRecord(replay, hierarchydeletion.HierarchyDeletionSmallRecordBytes)
	if err != nil {
		clear(tombstoneValue)
		clear(intentValue)
		clear(lockValue)
		return HierarchyDeletionOperation{}, nil, nil, TaskInitiation{}, err
	}
	fenceValue, err := hierarchydeletion.EncodeHierarchyDeletionRecord(fence, hierarchydeletion.HierarchyDeletionSmallRecordBytes)
	if err != nil {
		clear(tombstoneValue)
		clear(intentValue)
		clear(lockValue)
		clear(replayValue)
		return HierarchyDeletionOperation{}, nil, nil, TaskInitiation{}, err
	}
	mutatedRootValue, err := hierarchyDeletionRootValue(begin.TargetKind, root.targetValue, begin.TaskID)
	if err != nil {
		clear(tombstoneValue)
		clear(intentValue)
		clear(lockValue)
		clear(replayValue)
		clear(fenceValue)
		return HierarchyDeletionOperation{}, nil, nil, TaskInitiation{}, err
	}
	conditions := append([]etcdstore.Condition(nil), root.primaryFences...)
	conditions = append(conditions,
		etcdstore.Condition{Key: tombstoneKey}, etcdstore.Condition{Key: lockKey}, etcdstore.Condition{Key: replayKey},
		etcdstore.Condition{Key: fenceKey}, etcdstore.Condition{Key: intentKey},
	)
	mutations := []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: root.targetKey, Value: mutatedRootValue},
		{Type: etcdstore.MutationPut, Key: tombstoneKey, Value: tombstoneValue},
		{Type: etcdstore.MutationPut, Key: lockKey, Value: lockValue},
		{Type: etcdstore.MutationPut, Key: replayKey, Value: replayValue},
		{Type: etcdstore.MutationPut, Key: fenceKey, Value: fenceValue},
		{Type: etcdstore.MutationPut, Key: intentKey, Value: intentValue},
	}
	for index, current := range root.coordination {
		next := current.Record
		next.MutationEpoch++
		encoded, encodeErr := hierarchydeletion.EncodeHierarchyCoordination(next)
		if encodeErr != nil {
			etcdstore.ClearMutationValues(mutations)
			return HierarchyDeletionOperation{}, nil, nil, TaskInitiation{}, encodeErr
		}
		key := root.coordinationKeys[index]
		conditions = append(conditions, etcdstore.Condition{Key: key, ModRevision: current.Revision})
		mutations = append(mutations, etcdstore.Mutation{Type: etcdstore.MutationPut, Key: key, Value: encoded})
	}
	initiationFences := append([]etcdstore.Condition(nil), root.primaryFences...)
	for index, current := range root.coordination {
		initiationFences = append(initiationFences, etcdstore.Condition{
			Key: root.coordinationKeys[index], ModRevision: current.Revision,
		})
	}
	initiation, err := newTaskInitiation(root.owner, taskjournal.TaskActorOperator, initiationFences...)
	if err != nil {
		etcdstore.ClearMutationValues(mutations)
		return HierarchyDeletionOperation{}, nil, nil, TaskInitiation{}, err
	}
	return HierarchyDeletionOperation{
		Tombstone: tombstone, Fence: fence, Intent: intent, UpdatedAt: begin.CreatedAt,
	}, conditions, mutations, initiation, nil
}

func hierarchyDeletionRootValue(
	targetKind hierarchydeletion.HierarchyDeletionTargetKind,
	value []byte,
	taskID string,
) ([]byte, error) {
	switch targetKind {
	case hierarchydeletion.HierarchyDeletionTargetTenant:
		record, err := hierarchyrecord.DecodeTenant(value)
		if err != nil || record.DeletionTaskID != "" {
			return nil, hierarchyDeletionUnavailable(targetKind)
		}
		record.DeletionTaskID = taskID
		return hierarchyrecord.EncodeTenant(record)
	case hierarchydeletion.HierarchyDeletionTargetProject:
		record, err := hierarchyrecord.DecodeProject(value)
		if err != nil || record.DeletionTaskID != "" {
			return nil, hierarchyDeletionUnavailable(targetKind)
		}
		record.DeletionTaskID = taskID
		return hierarchyrecord.EncodeProject(record)
	case hierarchydeletion.HierarchyDeletionTargetBacking:
		record, err := hierarchyrecord.DecodeProject(value)
		if err != nil || record.Kind != hierarchyrecord.ProjectKindBacking || record.TenantID != "" || record.DeletionTaskID != "" {
			return nil, hierarchyDeletionUnavailable(targetKind)
		}
		record.DeletionTaskID = taskID
		return hierarchyrecord.EncodeProject(record)
	case hierarchydeletion.HierarchyDeletionTargetEnvironment:
		record, err := hierarchyrecord.DecodeEnvironment(value)
		if err != nil || record.DeletionTaskID != "" {
			return nil, hierarchyDeletionUnavailable(targetKind)
		}
		record.DeletionTaskID = taskID
		return hierarchyrecord.EncodeEnvironment(record)
	default:
		return nil, errs.New(errs.KindValidationFailed, "hierarchy deletion target kind is invalid")
	}
}

func hierarchyDeletionPrimaryKey(kind hierarchydeletion.HierarchyDeletionTargetKind, id string) string {
	switch kind {
	case hierarchydeletion.HierarchyDeletionTargetTenant:
		return hierarchyrecord.TenantKey(id)
	case hierarchydeletion.HierarchyDeletionTargetProject:
		return hierarchyrecord.ProjectKey(id)
	case hierarchydeletion.HierarchyDeletionTargetBacking:
		return hierarchyrecord.ProjectKey(id)
	case hierarchydeletion.HierarchyDeletionTargetEnvironment:
		return hierarchyrecord.EnvironmentKey(id)
	default:
		return ""
	}
}

func hierarchyDeletionNotFound(kind hierarchydeletion.HierarchyDeletionTargetKind) error {
	switch kind {
	case hierarchydeletion.HierarchyDeletionTargetTenant:
		return errs.New(errs.KindTenantNotFound, "Tenant was not found")
	case hierarchydeletion.HierarchyDeletionTargetProject:
		return errs.New(errs.KindProjectNotFound, "Project was not found")
	case hierarchydeletion.HierarchyDeletionTargetBacking:
		return errs.New(errs.KindBackingServiceNotFound, "Backing service was not found")
	case hierarchydeletion.HierarchyDeletionTargetEnvironment:
		return errs.New(errs.KindEnvironmentNotFound, "Environment was not found")
	default:
		return errs.New(errs.KindValidationFailed, "hierarchy deletion target kind is invalid")
	}
}

func hierarchyDeletionUnavailable(kind hierarchydeletion.HierarchyDeletionTargetKind) error {
	return errs.Newf(errs.KindResourceInUse, "%s deletion is already in progress", kind)
}

func hierarchyDeletionStableID(kind hierarchydeletion.HierarchyDeletionTargetKind, value string) bool {
	var expected ids.Kind
	switch kind {
	case hierarchydeletion.HierarchyDeletionTargetTenant:
		expected = ids.KindTenant
	case hierarchydeletion.HierarchyDeletionTargetProject:
		expected = ids.KindProject
	case hierarchydeletion.HierarchyDeletionTargetBacking:
		expected = ids.KindProject
	case hierarchydeletion.HierarchyDeletionTargetEnvironment:
		expected = ids.KindEnvironment
	}
	return expected != "" && ids.Validate(expected, value) == nil
}
