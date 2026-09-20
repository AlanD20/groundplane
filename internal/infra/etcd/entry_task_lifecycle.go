package etcd

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"maps"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *TaskRepository) prepareRemovalTaskRetry(
	ctx context.Context, source, retry TaskRecord, revision int64,
) (routeTaskChange, error) {
	change, err := repository.prepareRouteTaskRetry(ctx, source, retry, revision)
	if err != nil || change.applies {
		return change, err
	}
	change, err = repository.prepareEntryTaskRetry(ctx, source, retry, revision)
	if err != nil || change.applies {
		return change, err
	}
	return repository.prepareServiceRemovalTaskRetry(ctx, source, revision)
}

func (repository *TaskRepository) prepareEntryTaskRetry(
	ctx context.Context, source, retry TaskRecord, revision int64,
) (routeTaskChange, error) {
	intentRead, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{entryRemovalIntentKey(source.ID)}, Revision: revision,
	})
	if err != nil {
		return routeTaskChange{}, err
	}
	if intentRead == nil || len(intentRead.Values) != 1 {
		return routeTaskChange{}, errs.New(errs.KindInternal, "entry removal retry read is incomplete")
	}
	intentValue := intentRead.Values[0]
	if intentValue == nil {
		return routeTaskChange{}, nil
	}
	intent, err := decodeEntryRemovalIntent(intentValue.Value)
	if err != nil {
		return routeTaskChange{}, err
	}
	if err := validateEntryRemovalTaskOwner(source, intent); err != nil {
		return routeTaskChange{}, err
	}
	if source.FinishedAt == nil || intent.TerminalAt == nil || intent.Status != source.Status ||
		!intent.TerminalAt.Equal(*source.FinishedAt) || retry.RetryOf != source.ID ||
		retry.Executor != source.Executor || retry.Type != source.Type || retry.Target != source.Target ||
		retry.PlanID != source.PlanID || retry.PlanHash != source.PlanHash ||
		retry.RenderGeneration != source.RenderGeneration || !maps.Equal(retry.Params, source.Params) {
		return routeTaskChange{}, errs.New(errs.KindStateConflict, "entry removal retry changed its pinned Task")
	}
	retryIntent := cloneEntryRemovalIntent(intent)
	retryIntent.TaskID = retry.ID
	retryIntent.Status = TaskStatusPending
	retryIntent.CreatedAt = retry.CreatedAt
	retryIntent.TerminalAt = nil
	if err := validateEntryRemovalIntent(retryIntent); err != nil {
		return routeTaskChange{}, err
	}
	if err := validateEntryRemovalTaskOwner(retry, retryIntent); err != nil {
		return routeTaskChange{}, err
	}
	if intent.Desired != nil {
		return repository.prepareDesiredEntryRemovalRetry(
			ctx,
			source,
			retry,
			retryIntent,
			intentValue.ModRevision,
			revision,
		)
	}
	primary, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{
			entryRecordKey(intent.EntryID),
			deletionTombstoneKey(string(DeletionTargetEntry), intent.EntryID),
		},
		Revision: revision,
	})
	if err != nil {
		return routeTaskChange{}, err
	}
	if primary == nil || len(primary.Values) != 2 || primary.Values[0] == nil || primary.Values[1] != nil ||
		primary.Values[0].ModRevision != intent.EntryRevision {
		return routeTaskChange{}, errs.New(errs.KindStateConflict, "entry is not available for removal retry")
	}
	entry, err := decodeEntryRecord(primary.Values[0].Value)
	if err != nil || entry.Entry.ID != intent.EntryID || entry.EnvironmentID != intent.EnvironmentID {
		return routeTaskChange{}, corruptEntryRecord()
	}
	parents, keys, err := repository.readEntryRetryDependencies(ctx, source, entry, intent, revision)
	if err != nil {
		return routeTaskChange{}, err
	}
	scriptConditions, err := prepareEntryScriptAbsence(ctx, repository.store, intent.EntryID, revision)
	if err != nil {
		return routeTaskChange{}, err
	}
	change := routeTaskChange{
		applies: true,
		conditions: []etcdstore.Condition{
			{Key: entryRemovalIntentKey(source.ID), ModRevision: intentValue.ModRevision},
			{Key: entryRemovalIntentKey(retry.ID)},
			{Key: entryRecordKey(intent.EntryID), ModRevision: primary.Values[0].ModRevision},
			{Key: deletionTombstoneKey(string(DeletionTargetEntry), intent.EntryID)},
		},
	}
	for index, key := range keys {
		condition := etcdstore.Condition{Key: key}
		if parents.Values[index] != nil {
			condition.ModRevision = parents.Values[index].ModRevision
		}
		change.conditions = append(change.conditions, condition)
	}
	change.conditions = append(change.conditions, scriptConditions...)
	tombstone := DeletionTombstoneRecord{
		TargetKind: DeletionTargetEntry, TargetID: intent.EntryID, TargetRevision: intent.EntryRevision,
		TaskID: retry.ID, Phase: entryRemovalTombstonePhase(retryIntent),
		CreatedAt: retry.CreatedAt, UpdatedAt: retry.CreatedAt,
	}
	tombstoneValue, err := encodeDeletionTombstone(tombstone)
	if err != nil {
		return routeTaskChange{}, err
	}
	intentBytes, err := encodeEntryRemovalIntent(retryIntent)
	if err != nil {
		clear(tombstoneValue)
		return routeTaskChange{}, err
	}
	change.values = append(change.values, tombstoneValue, intentBytes)
	change.mutations = append(change.mutations,
		etcdstore.Mutation{
			Type: etcdstore.MutationPut, Key: deletionTombstoneKey(string(DeletionTargetEntry), intent.EntryID),
			Value: tombstoneValue,
		},
		etcdstore.Mutation{Type: etcdstore.MutationPut, Key: entryRemovalIntentKey(retry.ID), Value: intentBytes},
	)
	if intent.CurrentProjection != nil {
		change.mutations = append(change.mutations, etcdstore.Mutation{
			Type: etcdstore.MutationPut, Key: componentTaskActiveEnvironmentKey(intent.EnvironmentID), Value: []byte(retry.ID),
		})
	}
	return change, nil
}
func (repository *TaskRepository) readEntryRetryDependencies(
	ctx context.Context, source TaskRecord, entry EntryRecord, intent EntryRemovalIntent, revision int64,
) (*etcdstore.GetManyResult, []string, error) {
	baseKeys := []string{
		entryOwnerKey(entry.EnvironmentID, entry.Entry.ID),
		environmentKey(entry.EnvironmentID),
		deletionTombstoneKey(string(DeletionTargetEnvironment), entry.EnvironmentID),
	}
	base, err := getManyBatchedAtRevision(ctx, repository.store, baseKeys, revision)
	if err != nil {
		return nil, nil, err
	}
	if base.Values[0] == nil || base.Values[1] == nil || base.Values[2] != nil ||
		string(base.Values[0].Value) != entry.Entry.ID {
		return nil, nil, errs.New(errs.KindResourceInUse, "entry retry hierarchy is unavailable")
	}
	environment, err := decodeEnvironment(base.Values[1].Value)
	if err != nil || environment.ID != entry.EnvironmentID {
		return nil, nil, corruptRecord()
	}
	extraKeys := []string{
		projectKey(environment.ProjectID),
		deletionTombstoneKey(string(DeletionTargetProject), environment.ProjectID),
	}
	projectRead, err := getManyBatchedAtRevision(ctx, repository.store, extraKeys, revision)
	if err != nil {
		return nil, nil, err
	}
	if projectRead.Values[0] == nil || projectRead.Values[1] != nil {
		return nil, nil, errs.New(errs.KindResourceInUse, "entry retry Project is unavailable")
	}
	project, err := decodeProject(projectRead.Values[0].Value)
	if err != nil || project.ID != environment.ProjectID {
		return nil, nil, corruptRecord()
	}
	keys := append(baseKeys, extraKeys...)
	values := append(base.Values, projectRead.Values...)
	if project.TenantID != "" {
		tenantKey := deletionTombstoneKey(string(DeletionTargetTenant), project.TenantID)
		tenantRead, readErr := getManyBatchedAtRevision(ctx, repository.store, []string{tenantKey}, revision)
		if readErr != nil {
			return nil, nil, readErr
		}
		if tenantRead.Values[0] != nil {
			return nil, nil, errs.New(errs.KindResourceInUse, "entry retry Tenant is unavailable")
		}
		keys = append(keys, tenantKey)
		values = append(values, tenantRead.Values[0])
	}
	if intent.CurrentProjection != nil {
		projectionKey := environmentComposeProjectionKey(intent.EnvironmentID)
		activeKey := componentTaskActiveEnvironmentKey(intent.EnvironmentID)
		projectionRead, readErr := getManyBatchedAtRevision(
			ctx, repository.store, []string{projectionKey, activeKey}, revision)
		if readErr != nil {
			return nil, nil, readErr
		}
		if projectionRead.Values[0] == nil || projectionRead.Values[1] != nil ||
			projectionRead.Values[0].ModRevision != intent.CurrentProjectionRevision {
			return nil, nil, errs.New(errs.KindStateConflict, "entry retry applied projection changed")
		}
		projection, decodeErr := decodeEnvironmentComposeProjection(projectionRead.Values[0].Value)
		if decodeErr != nil || !sameEntryRemovalProjection(projection, *intent.CurrentProjection) {
			return nil, nil, errs.New(errs.KindStateConflict, "entry retry applied projection changed")
		}
		keys = append(keys, projectionKey, activeKey)
		values = append(values, projectionRead.Values...)
	}
	return &etcdstore.GetManyResult{Values: values, ReadRevision: revision}, keys, nil
}

func (repository *TaskRepository) prepareRemovalTaskAcknowledgement(
	ctx context.Context, task TaskRecord, terminalStatus TaskStatus, terminalAt time.Time, revision int64,
) (routeTaskChange, error) {
	change, err := repository.prepareRouteTaskAcknowledgement(ctx, task, terminalStatus, terminalAt, revision)
	if err != nil || change.applies {
		return change, err
	}
	change, err = repository.prepareEntryTaskAcknowledgement(ctx, task, terminalStatus, terminalAt, revision)
	if err != nil || change.applies {
		return change, err
	}
	return repository.prepareServiceRemovalTaskAcknowledgement(ctx, task, terminalStatus, terminalAt, revision)
}

func (repository *TaskRepository) prepareEntryTaskAcknowledgement(
	ctx context.Context, task TaskRecord, terminalStatus TaskStatus, terminalAt time.Time, revision int64,
) (routeTaskChange, error) {
	intentRead, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{entryRemovalIntentKey(task.ID)}, Revision: revision,
	})
	if err != nil {
		return routeTaskChange{}, err
	}
	if intentRead == nil || len(intentRead.Values) != 1 {
		return routeTaskChange{}, errs.New(errs.KindInternal, "entry removal intent read is incomplete")
	}
	intentValue := intentRead.Values[0]
	if intentValue == nil {
		return routeTaskChange{}, nil
	}
	intent, err := decodeEntryRemovalIntent(intentValue.Value)
	if err != nil {
		return routeTaskChange{}, err
	}
	if err := validateEntryRemovalTaskOwner(task, intent); err != nil {
		return routeTaskChange{}, err
	}
	if intent.Status != TaskStatusPending {
		return routeTaskChange{}, errs.New(errs.KindStateConflict, "entry removal intent is not pending")
	}
	if intent.Desired != nil {
		return repository.prepareDesiredEntryRemovalAcknowledgement(
			ctx,
			task,
			intent,
			intentValue.ModRevision,
			terminalStatus,
			terminalAt,
			revision,
		)
	}
	state, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{
			entryRecordKey(intent.EntryID),
			deletionTombstoneKey(string(DeletionTargetEntry), intent.EntryID),
		},
		Revision: revision,
	})
	if err != nil {
		return routeTaskChange{}, err
	}
	if state == nil || len(state.Values) != 2 || state.Values[0] == nil || state.Values[1] == nil ||
		state.Values[0].ModRevision != intent.EntryRevision {
		return routeTaskChange{}, errs.New(errs.KindStateConflict, "entry removal state is incomplete")
	}
	entry, err := decodeEntryRecord(state.Values[0].Value)
	if err != nil || entry.Entry.ID != intent.EntryID || entry.EnvironmentID != intent.EnvironmentID {
		return routeTaskChange{}, corruptEntryRecord()
	}
	tombstone, err := decodeDeletionTombstone(state.Values[1].Value)
	if err != nil || tombstone.TargetKind != DeletionTargetEntry || tombstone.TargetID != intent.EntryID ||
		tombstone.TargetRevision != intent.EntryRevision || tombstone.TaskID != task.ID ||
		tombstone.Phase != entryRemovalTombstonePhase(intent) {
		return routeTaskChange{}, errs.New(errs.KindStateConflict, "entry deletion tombstone changed")
	}
	companionKeys := []string{entryOwnerKey(entry.EnvironmentID, entry.Entry.ID)}
	if intent.CurrentProjection != nil {
		companionKeys = append(companionKeys,
			environmentComposeProjectionKey(intent.EnvironmentID),
			componentTaskActiveEnvironmentKey(intent.EnvironmentID),
		)
	}
	companions, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: companionKeys, Revision: revision})
	if err != nil {
		return routeTaskChange{}, err
	}
	if companions == nil || len(companions.Values) != len(companionKeys) || companions.Values[0] == nil ||
		string(companions.Values[0].Value) != intent.EntryID {
		return routeTaskChange{}, errs.New(errs.KindInternal, "entry deletion owner index is inconsistent")
	}
	if intent.CurrentProjection != nil {
		if companions.Values[1] == nil || companions.Values[2] == nil ||
			companions.Values[1].ModRevision != intent.CurrentProjectionRevision ||
			string(companions.Values[2].Value) != task.ID {
			return routeTaskChange{}, errs.New(errs.KindStateConflict, "entry removal projection ownership changed")
		}
		projection, decodeErr := decodeEnvironmentComposeProjection(companions.Values[1].Value)
		if decodeErr != nil || !sameEntryRemovalProjection(projection, *intent.CurrentProjection) {
			return routeTaskChange{}, errs.New(errs.KindStateConflict, "entry removal applied projection changed")
		}
	}
	terminalIntent, err := terminalEntryRemovalIntent(intent, terminalStatus, terminalAt)
	if err != nil {
		return routeTaskChange{}, err
	}
	intentBytes, err := encodeEntryRemovalIntent(terminalIntent)
	if err != nil {
		return routeTaskChange{}, err
	}
	change := routeTaskChange{
		applies: true,
		conditions: []etcdstore.Condition{
			{Key: entryRemovalIntentKey(task.ID), ModRevision: intentValue.ModRevision},
			{Key: entryRecordKey(intent.EntryID), ModRevision: state.Values[0].ModRevision},
			{
				Key:         deletionTombstoneKey(string(DeletionTargetEntry), intent.EntryID),
				ModRevision: state.Values[1].ModRevision,
			},
			{Key: companionKeys[0], ModRevision: companions.Values[0].ModRevision},
		},
		mutations: []etcdstore.Mutation{
			{Type: etcdstore.MutationPut, Key: entryRemovalIntentKey(task.ID), Value: intentBytes},
			{Type: etcdstore.MutationDelete, Key: deletionTombstoneKey(string(DeletionTargetEntry), intent.EntryID)},
		},
		values: [][]byte{intentBytes},
	}
	if intent.CurrentProjection != nil {
		change.conditions = append(change.conditions,
			etcdstore.Condition{Key: companionKeys[1], ModRevision: companions.Values[1].ModRevision},
			etcdstore.Condition{Key: companionKeys[2], ModRevision: companions.Values[2].ModRevision},
		)
		change.mutations = append(change.mutations, etcdstore.Mutation{
			Type: etcdstore.MutationDelete, Key: componentTaskActiveEnvironmentKey(intent.EnvironmentID),
		})
	}
	if terminalStatus == TaskStatusCompleted {
		scriptConditions, err := prepareEntryScriptAbsence(ctx, repository.store, intent.EntryID, revision)
		if err != nil {
			clearRouteTaskChange(change)
			return routeTaskChange{}, err
		}
		change.conditions = append(change.conditions, scriptConditions...)
		change.mutations = append(change.mutations,
			etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: entryOwnerKey(entry.EnvironmentID, entry.Entry.ID)},
			etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: entryRecordKey(intent.EntryID)},
			etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: entryPlainValueGenerationPrefix + intent.EntryID + "/", Prefix: true},
			etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: entrySecretValueGenerationPrefix + intent.EntryID + "/", Prefix: true},
		)
		if intent.CandidateProjection != nil {
			projectionValue, encodeErr := encodeEnvironmentComposeProjection(*intent.CandidateProjection)
			if encodeErr != nil {
				clearRouteTaskChange(change)
				return routeTaskChange{}, encodeErr
			}
			change.values = append(change.values, projectionValue)
			change.mutations = append(change.mutations, etcdstore.Mutation{
				Type: etcdstore.MutationPut, Key: environmentComposeProjectionKey(intent.EnvironmentID), Value: projectionValue,
			})
		}
	}
	return change, nil
}
func (repository *TaskRepository) validateRemovalTaskAcknowledgementReplay(
	ctx context.Context, task TaskRecord, terminalStatus TaskStatus, revision int64,
) error {
	if err := repository.validateRouteTaskAcknowledgementReplay(ctx, task, terminalStatus, revision); err != nil {
		return err
	}
	if err := repository.validateEntryTaskAcknowledgementReplay(ctx, task, terminalStatus, revision); err != nil {
		return err
	}
	return repository.validateServiceRemovalTaskAcknowledgementReplay(ctx, task, terminalStatus, revision)
}

func (repository *TaskRepository) validateEntryTaskAcknowledgementReplay(
	ctx context.Context, task TaskRecord, terminalStatus TaskStatus, revision int64,
) error {
	intentRead, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{entryRemovalIntentKey(task.ID)}, Revision: revision,
	})
	if err != nil {
		return err
	}
	if intentRead == nil || len(intentRead.Values) != 1 {
		return errs.New(errs.KindInternal, "entry removal replay read is incomplete")
	}
	if intentRead.Values[0] == nil {
		return nil
	}
	intent, err := decodeEntryRemovalIntent(intentRead.Values[0].Value)
	if err != nil {
		return err
	}
	if err := validateEntryRemovalTaskOwner(task, intent); err != nil {
		return err
	}
	if intent.Status != terminalStatus || intent.TerminalAt == nil || task.FinishedAt == nil ||
		!intent.TerminalAt.Equal(*task.FinishedAt) {
		return errs.New(errs.KindStateConflict, "entry removal intent does not match terminal Task")
	}
	if intent.Desired != nil {
		// The immutable terminal intent and Task were committed together. Later
		// edits or removal retries cannot change this acknowledgement's result.
		return nil
	}
	state, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{
			entryRecordKey(intent.EntryID),
			deletionTombstoneKey(string(DeletionTargetEntry), intent.EntryID),
			componentTaskActiveEnvironmentKey(intent.EnvironmentID),
		},
		Revision: revision,
	})
	if err != nil {
		return err
	}
	if state == nil || len(state.Values) != 3 || state.Values[1] != nil {
		return errs.New(errs.KindStateConflict, "entry removal terminal fence is inconsistent")
	}
	if terminalStatus == TaskStatusCompleted && state.Values[0] != nil {
		return errs.New(errs.KindStateConflict, "completed Entry removal retained its target")
	}
	if terminalStatus != TaskStatusCompleted && state.Values[0] == nil {
		return errs.New(errs.KindStateConflict, "failed Entry removal lost its target")
	}
	if state.Values[2] != nil {
		activeTaskID := string(state.Values[2].Value)
		if activeTaskID == task.ID || ids.Validate(ids.KindTask, activeTaskID) != nil {
			return errs.New(errs.KindStateConflict, "terminal Entry removal remains active")
		}
	}
	return nil
}

func validateEntryRemovalTaskOwner(task TaskRecord, intent EntryRemovalIntent) error {
	expectedExecutor := TaskExecutorController
	validParams := len(task.Params) == 2 &&
		task.Params[TaskResourceKindParam] == TaskResourceEntry &&
		task.Params[TaskEntryEnvironmentParam] == intent.EnvironmentID
	if intent.Desired != nil && intent.CurrentProjection == nil {
		validParams = len(task.Params) == 3 && task.Params[TaskResourceKindParam] == TaskResourceEntry &&
			task.Params[TaskEntryEnvironmentParam] == intent.EnvironmentID
	}
	if intent.CurrentProjection != nil {
		expectedExecutor = TaskExecutorAgent
		validParams = intent.CandidateProjection != nil && len(task.Params) == 8 &&
			task.Params[TaskEntryEnvironmentParam] == intent.EnvironmentID &&
			task.Params[TaskMaterializationEnvironmentParam] == intent.EnvironmentID &&
			task.Params[EnvironmentDesiredRevisionParam] == intent.CandidateProjection.RevisionID &&
			validateStableID(ids.KindConfig, task.Params[TaskComposeArtifactParam]) == nil &&
			task.Params[TaskEntryProjectSlugParam] != "" && task.Params[TaskEntryEnvironmentNameParam] != "" &&
			task.Params[TaskEntryAuthorizedVolumeDirParam] != "" &&
			(task.Owner.WorkspaceType == TaskWorkspacePlatform && task.Params[TaskEntryTenantSlugParam] == "" ||
				task.Owner.WorkspaceType == TaskWorkspaceTenant && task.Params[TaskEntryTenantSlugParam] != "")
	}
	if intent.Desired != nil && (task.Params[EnvironmentDesiredRevisionParam] != intent.Desired.RevisionID ||
		uint64(task.RenderGeneration) != intent.Desired.RenderGeneration) {
		validParams = false
	}
	if task.ID != intent.TaskID || task.Executor != expectedExecutor || task.Type != TaskRemove ||
		task.Target != intent.EntryID || !task.CreatedAt.Equal(intent.CreatedAt) || !validParams {
		return errs.New(errs.KindStateConflict, "entry removal intent does not belong to its Task")
	}
	return nil
}

func entryRemovalTombstonePhase(intent EntryRemovalIntent) DeletionPhase {
	if intent.CurrentProjection != nil {
		return DeletionPhaseHostEffects
	}
	return DeletionPhaseFinalizing
}
