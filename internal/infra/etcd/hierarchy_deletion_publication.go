package etcd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *HierarchyDeletionRepository) readDeletionRoot(
	ctx context.Context,
	targetKind HierarchyDeletionTargetKind,
	targetID string,
) (hierarchyDeletionRoot, int64, error) {
	if err := validateContext(ctx); err != nil {
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
		primaryFences: []Condition{{Key: key, ModRevision: result.Entry.ModRevision}},
	}
	root.targetValue = append([]byte(nil), result.Entry.Value...)
	switch targetKind {
	case HierarchyDeletionTargetTenant:
		tenant, decodeErr := decodeTenant(result.Entry.Value)
		if decodeErr != nil || tenant.ID != targetID || tenant.DeletionTaskID != "" {
			return hierarchyDeletionRoot{}, 0, hierarchyDeletionUnavailable(targetKind)
		}
		root.rootSlug = tenant.Slug
		root.workspace = HierarchyDeletionWorkspace{Type: "tenant", TenantID: tenant.ID}
		root.owner, err = TenantTaskOwner(tenant.ID)
		root.coordinationKeys = []string{HierarchyCoordinationKey(string(targetKind), targetID)}
	case HierarchyDeletionTargetProject:
		project, decodeErr := decodeProject(result.Entry.Value)
		if decodeErr != nil || project.ID != targetID || project.DeletionTaskID != "" {
			return hierarchyDeletionRoot{}, 0, hierarchyDeletionUnavailable(targetKind)
		}
		root.rootSlug = project.Slug
		root.owner, err = ProjectTaskOwner(project)
		if project.Kind == ProjectKindBacking {
			root.workspace = HierarchyDeletionWorkspace{Type: "platform"}
		} else {
			root.workspace = HierarchyDeletionWorkspace{Type: "tenant", TenantID: project.TenantID}
		}
		root.coordinationKeys = []string{HierarchyCoordinationKey(string(targetKind), targetID)}
		if project.TenantID != "" {
			root, err = repository.readProjectParentAtRevision(ctx, result.ReadRevision, project, root)
		}
	case HierarchyDeletionTargetBacking:
		project, decodeErr := decodeProject(result.Entry.Value)
		if decodeErr != nil || project.ID != targetID || project.Kind != ProjectKindBacking ||
			project.TenantID != "" || project.DeletionTaskID != "" {
			return hierarchyDeletionRoot{}, 0, hierarchyDeletionUnavailable(targetKind)
		}
		root.rootSlug = project.Slug
		root.workspace = HierarchyDeletionWorkspace{Type: "platform"}
		root.owner, err = ProjectTaskOwner(project)
		root.coordinationKeys = []string{HierarchyCoordinationKey(string(HierarchyDeletionTargetProject), targetID)}
	case HierarchyDeletionTargetEnvironment:
		environment, decodeErr := decodeEnvironment(result.Entry.Value)
		if decodeErr != nil || environment.ID != targetID || environment.DeletionTaskID != "" {
			return hierarchyDeletionRoot{}, 0, hierarchyDeletionUnavailable(targetKind)
		}
		root.rootSlug = environment.Name
		root, err = repository.readEnvironmentParentsAtRevision(ctx, result.ReadRevision, environment, root)
	default:
		return hierarchyDeletionRoot{}, 0, errs.New(errs.KindValidationFailed, "hierarchy deletion target kind is invalid")
	}
	if err != nil {
		clear(root.targetValue)
		return hierarchyDeletionRoot{}, 0, err
	}
	coordination, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys: root.coordinationKeys, Revision: result.ReadRevision,
	})
	if err != nil {
		clear(root.targetValue)
		return hierarchyDeletionRoot{}, 0, err
	}
	if coordination == nil || coordination.ReadRevision != result.ReadRevision ||
		len(coordination.Values) != len(root.coordinationKeys) {
		clear(root.targetValue)
		return hierarchyDeletionRoot{}, 0, corruptHierarchyDeletion()
	}
	root.coordination = make([]Versioned[HierarchyCoordinationRecord], len(root.coordinationKeys))
	for index, value := range coordination.Values {
		if value == nil || value.Key != root.coordinationKeys[index] || value.ModRevision <= 0 {
			clear(root.targetValue)
			return hierarchyDeletionRoot{}, 0, corruptHierarchyDeletion()
		}
		record, decodeErr := decodeHierarchyCoordination(value.Value)
		if decodeErr != nil {
			clear(root.targetValue)
			return hierarchyDeletionRoot{}, 0, decodeErr
		}
		root.coordination[index] = Versioned[HierarchyCoordinationRecord]{
			Record: record, Revision: value.ModRevision, ReadRevision: result.ReadRevision,
		}
	}
	return root, result.ReadRevision, nil
}

func (repository *HierarchyDeletionRepository) readProjectParentAtRevision(
	ctx context.Context,
	revision int64,
	project ProjectRecord,
	root hierarchyDeletionRoot,
) (hierarchyDeletionRoot, error) {
	parents, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys: []string{tenantKey(project.TenantID)}, Revision: revision,
	})
	if err != nil {
		return hierarchyDeletionRoot{}, err
	}
	if parents == nil || parents.ReadRevision != revision || len(parents.Values) != 1 || parents.Values[0] == nil {
		return hierarchyDeletionRoot{}, corruptHierarchyDeletion()
	}
	tenant, err := decodeTenant(parents.Values[0].Value)
	if err != nil || tenant.ID != project.TenantID || tenant.DeletionTaskID != "" {
		return hierarchyDeletionRoot{}, hierarchyDeletionUnavailable(HierarchyDeletionTargetProject)
	}
	root.primaryFences = append(root.primaryFences, Condition{
		Key: tenantKey(tenant.ID), ModRevision: parents.Values[0].ModRevision,
	})
	root.coordinationKeys = append([]string{
		HierarchyCoordinationKey(string(HierarchyDeletionTargetTenant), tenant.ID),
	}, root.coordinationKeys...)
	return root, nil
}

func (repository *HierarchyDeletionRepository) readEnvironmentParentsAtRevision(
	ctx context.Context,
	revision int64,
	environment EnvironmentRecord,
	root hierarchyDeletionRoot,
) (hierarchyDeletionRoot, error) {
	projectResult, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys: []string{projectKey(environment.ProjectID)}, Revision: revision,
	})
	if err != nil {
		return hierarchyDeletionRoot{}, err
	}
	if projectResult == nil || projectResult.ReadRevision != revision || len(projectResult.Values) != 1 ||
		projectResult.Values[0] == nil {
		return hierarchyDeletionRoot{}, corruptHierarchyDeletion()
	}
	project, err := decodeProject(projectResult.Values[0].Value)
	if err != nil || project.ID != environment.ProjectID || project.DeletionTaskID != "" {
		return hierarchyDeletionRoot{}, hierarchyDeletionUnavailable(HierarchyDeletionTargetEnvironment)
	}
	root.primaryFences = append(root.primaryFences, Condition{
		Key: projectKey(project.ID), ModRevision: projectResult.Values[0].ModRevision,
	})
	root.owner, err = EnvironmentTaskOwner(project, environment)
	if err != nil {
		return hierarchyDeletionRoot{}, err
	}
	root.coordinationKeys = []string{
		HierarchyCoordinationKey(string(HierarchyDeletionTargetProject), project.ID),
		HierarchyCoordinationKey(string(HierarchyDeletionTargetEnvironment), environment.ID),
	}
	if project.TenantID != "" {
		root, err = repository.readProjectParentAtRevision(ctx, revision, project, root)
		if err != nil {
			return hierarchyDeletionRoot{}, err
		}
		root.workspace = HierarchyDeletionWorkspace{Type: "tenant", TenantID: project.TenantID}
	} else {
		root.workspace = HierarchyDeletionWorkspace{Type: "platform"}
	}
	return root, nil
}

func prepareHierarchyDeletionPublication(
	begin HierarchyDeletionBegin,
	root hierarchyDeletionRoot,
	snapshotRevision int64,
) (HierarchyDeletionOperation, []Condition, []Mutation, TaskInitiation, error) {
	if begin.OperationKind == HierarchyDeletionOperationProject && root.workspace.Type == "platform" ||
		begin.OperationKind == HierarchyDeletionOperationBacking && root.workspace.Type != "platform" {
		return HierarchyDeletionOperation{}, nil, nil, TaskInitiation{}, errs.New(
			errs.KindValidationFailed,
			"hierarchy deletion Project kind must use its exclusive permanent-delete authority",
		)
	}
	targetCoordinationIndex := -1
	for index, record := range root.coordination {
		kindMatches := record.Record.TargetKind == begin.TargetKind ||
			(begin.TargetKind == HierarchyDeletionTargetBacking && record.Record.TargetKind == HierarchyDeletionTargetProject)
		if kindMatches && record.Record.TargetID == begin.TargetID {
			targetCoordinationIndex = index
			break
		}
	}
	if targetCoordinationIndex < 0 {
		return HierarchyDeletionOperation{}, nil, nil, TaskInitiation{}, corruptHierarchyDeletion()
	}
	deletionEpoch := root.coordination[targetCoordinationIndex].Record.MutationEpoch + 1
	tombstoneKey := HierarchyDeletionTombstoneKey(string(begin.TargetKind), begin.TargetID)
	lockKey := HierarchyDeletionLockKey(string(begin.TargetKind), begin.TargetID)
	replayKey, err := HierarchyDeletionReplayTargetKey(begin.OperationID)
	if err != nil {
		return HierarchyDeletionOperation{}, nil, nil, TaskInitiation{}, err
	}
	fenceKey, err := HierarchyDeletionCleanupFenceKey(begin.OperationID)
	if err != nil {
		return HierarchyDeletionOperation{}, nil, nil, TaskInitiation{}, err
	}
	intentKey, err := HierarchyDeletionIntentKey(begin.OperationID)
	if err != nil {
		return HierarchyDeletionOperation{}, nil, nil, TaskInitiation{}, err
	}
	initialDigest := sha256.Sum256([]byte("gp-deletion-completed-prefix-v1\x00"))
	tombstone := HierarchyDeletionTombstone{
		Schema: 1, TargetKind: begin.TargetKind, TargetID: begin.TargetID,
		TargetRevision: root.targetRevision, OperationKind: begin.OperationKind,
		OperationID: begin.OperationID, TaskOperationID: begin.TaskOperationID,
		DeletionEpoch: deletionEpoch, CurrentTaskID: begin.TaskID, Workspace: root.workspace,
		SnapshotRevision: snapshotRevision, Phase: HierarchyDeletionPlanning,
		Checkpoint: HierarchyDeletionCheckpoint{CompletedPrefixDigest: hex.EncodeToString(initialDigest[:])},
		CreatedAt:  begin.CreatedAt, AttemptDeadline: begin.DeadlineAt,
	}
	intent := HierarchyDeletionIntent{
		Schema: 1, OperationKind: begin.OperationKind, OperationID: begin.OperationID,
		TaskOperationID: begin.TaskOperationID, DeletionEpoch: deletionEpoch,
		TargetKind: begin.TargetKind, TargetID: begin.TargetID, TargetRevision: root.targetRevision,
		Workspace: root.workspace, RootSlug: root.rootSlug, SnapshotRevision: snapshotRevision,
		TimeoutSeconds: int64(hierarchyDeletionAttemptTimeout / time.Second), CreatedAt: begin.CreatedAt,
	}
	lock := HierarchyDeletionLock{
		Schema: 1, TargetKind: begin.TargetKind, TargetID: begin.TargetID,
		ParentOperationID: begin.OperationID, DeletionEpoch: deletionEpoch,
		TombstoneKey: tombstoneKey, CleanupFenceKey: fenceKey, CreatedAt: begin.CreatedAt,
	}
	replay := HierarchyDeletionReplayLocator{
		Schema: 1, ParentOperationID: begin.OperationID, OperationKind: begin.OperationKind,
		TargetKind: begin.TargetKind, TargetID: begin.TargetID, DeletionEpoch: deletionEpoch,
		RootTaskID: begin.TaskID, CurrentTaskID: begin.TaskID,
		ResponseDigest: hierarchyDeletionDigest(begin.Marker.Response.Body), TombstoneKey: tombstoneKey,
	}
	fence := HierarchyDeletionCleanupFence{
		Schema: 1, ParentOperationID: begin.OperationID, DeletionEpoch: deletionEpoch,
		TargetKind: begin.TargetKind, TargetID: begin.TargetID, Generation: 1,
		Phase: HierarchyDeletionPlanning, CurrentTaskID: begin.TaskID,
		Dispatch: HierarchyDeletionDispatchOpen, UpdatedAt: begin.CreatedAt,
	}
	tombstoneValue, err := encodeHierarchyDeletionRecord(tombstone, hierarchyDeletionLargeRecordBytes)
	if err != nil {
		return HierarchyDeletionOperation{}, nil, nil, TaskInitiation{}, err
	}
	intentValue, err := encodeHierarchyDeletionRecord(intent, hierarchyDeletionLargeRecordBytes)
	if err != nil {
		clear(tombstoneValue)
		return HierarchyDeletionOperation{}, nil, nil, TaskInitiation{}, err
	}
	lockValue, err := encodeHierarchyDeletionRecord(lock, hierarchyDeletionSmallRecordBytes)
	if err != nil {
		clear(tombstoneValue)
		clear(intentValue)
		return HierarchyDeletionOperation{}, nil, nil, TaskInitiation{}, err
	}
	replayValue, err := encodeHierarchyDeletionRecord(replay, hierarchyDeletionSmallRecordBytes)
	if err != nil {
		clear(tombstoneValue)
		clear(intentValue)
		clear(lockValue)
		return HierarchyDeletionOperation{}, nil, nil, TaskInitiation{}, err
	}
	fenceValue, err := encodeHierarchyDeletionRecord(fence, hierarchyDeletionSmallRecordBytes)
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
	conditions := append([]Condition(nil), root.primaryFences...)
	conditions = append(conditions,
		Condition{Key: tombstoneKey}, Condition{Key: lockKey}, Condition{Key: replayKey},
		Condition{Key: fenceKey}, Condition{Key: intentKey},
	)
	mutations := []Mutation{
		{Type: MutationPut, Key: root.targetKey, Value: mutatedRootValue},
		{Type: MutationPut, Key: tombstoneKey, Value: tombstoneValue},
		{Type: MutationPut, Key: lockKey, Value: lockValue},
		{Type: MutationPut, Key: replayKey, Value: replayValue},
		{Type: MutationPut, Key: fenceKey, Value: fenceValue},
		{Type: MutationPut, Key: intentKey, Value: intentValue},
	}
	for index, current := range root.coordination {
		next := current.Record
		next.MutationEpoch++
		encoded, encodeErr := encodeHierarchyCoordination(next)
		if encodeErr != nil {
			clearMutationValues(mutations)
			return HierarchyDeletionOperation{}, nil, nil, TaskInitiation{}, encodeErr
		}
		key := root.coordinationKeys[index]
		conditions = append(conditions, Condition{Key: key, ModRevision: current.Revision})
		mutations = append(mutations, Mutation{Type: MutationPut, Key: key, Value: encoded})
	}
	initiationFences := append([]Condition(nil), root.primaryFences...)
	for index, current := range root.coordination {
		initiationFences = append(initiationFences, Condition{
			Key: root.coordinationKeys[index], ModRevision: current.Revision,
		})
	}
	initiation, err := newTaskInitiation(root.owner, TaskActorOperator, initiationFences...)
	if err != nil {
		clearMutationValues(mutations)
		return HierarchyDeletionOperation{}, nil, nil, TaskInitiation{}, err
	}
	return HierarchyDeletionOperation{
		Tombstone: tombstone, Fence: fence, Intent: intent, UpdatedAt: begin.CreatedAt,
	}, conditions, mutations, initiation, nil
}

func hierarchyDeletionRootValue(
	targetKind HierarchyDeletionTargetKind,
	value []byte,
	taskID string,
) ([]byte, error) {
	switch targetKind {
	case HierarchyDeletionTargetTenant:
		record, err := decodeTenant(value)
		if err != nil || record.DeletionTaskID != "" {
			return nil, hierarchyDeletionUnavailable(targetKind)
		}
		record.DeletionTaskID = taskID
		return encodeTenant(record)
	case HierarchyDeletionTargetProject:
		record, err := decodeProject(value)
		if err != nil || record.DeletionTaskID != "" {
			return nil, hierarchyDeletionUnavailable(targetKind)
		}
		record.DeletionTaskID = taskID
		return encodeProject(record)
	case HierarchyDeletionTargetBacking:
		record, err := decodeProject(value)
		if err != nil || record.Kind != ProjectKindBacking || record.TenantID != "" || record.DeletionTaskID != "" {
			return nil, hierarchyDeletionUnavailable(targetKind)
		}
		record.DeletionTaskID = taskID
		return encodeProject(record)
	case HierarchyDeletionTargetEnvironment:
		record, err := decodeEnvironment(value)
		if err != nil || record.DeletionTaskID != "" {
			return nil, hierarchyDeletionUnavailable(targetKind)
		}
		record.DeletionTaskID = taskID
		return encodeEnvironment(record)
	default:
		return nil, errs.New(errs.KindValidationFailed, "hierarchy deletion target kind is invalid")
	}
}

func hierarchyDeletionPrimaryKey(kind HierarchyDeletionTargetKind, id string) string {
	switch kind {
	case HierarchyDeletionTargetTenant:
		return tenantKey(id)
	case HierarchyDeletionTargetProject:
		return projectKey(id)
	case HierarchyDeletionTargetBacking:
		return projectKey(id)
	case HierarchyDeletionTargetEnvironment:
		return environmentKey(id)
	default:
		return ""
	}
}

func hierarchyDeletionNotFound(kind HierarchyDeletionTargetKind) error {
	switch kind {
	case HierarchyDeletionTargetTenant:
		return errs.New(errs.KindTenantNotFound, "Tenant was not found")
	case HierarchyDeletionTargetProject:
		return errs.New(errs.KindProjectNotFound, "Project was not found")
	case HierarchyDeletionTargetBacking:
		return errs.New(errs.KindBackingServiceNotFound, "Backing service was not found")
	case HierarchyDeletionTargetEnvironment:
		return errs.New(errs.KindEnvironmentNotFound, "Environment was not found")
	default:
		return errs.New(errs.KindValidationFailed, "hierarchy deletion target kind is invalid")
	}
}

func hierarchyDeletionUnavailable(kind HierarchyDeletionTargetKind) error {
	return errs.Newf(errs.KindResourceInUse, "%s deletion is already in progress", kind)
}

func hierarchyDeletionStableID(kind HierarchyDeletionTargetKind, value string) bool {
	var expected ids.Kind
	switch kind {
	case HierarchyDeletionTargetTenant:
		expected = ids.KindTenant
	case HierarchyDeletionTargetProject:
		expected = ids.KindProject
	case HierarchyDeletionTargetBacking:
		expected = ids.KindProject
	case HierarchyDeletionTargetEnvironment:
		expected = ids.KindEnvironment
	}
	return expected != "" && ids.Validate(expected, value) == nil
}
