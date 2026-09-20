package etcd

import (
	"context"
	"errors"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	recordcodec "github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type scriptTaskChange struct {
	applies    bool
	conditions []etcdstore.Condition
	mutations  []etcdstore.Mutation
	values     [][]byte
}

func (repository *TaskRepository) prepareScriptTaskRetry(
	ctx context.Context,
	source TaskRecord,
	retry TaskRecord,
	revision int64,
) (scriptTaskChange, error) {
	if source.Type == TaskScript {
		return repository.prepareManualScriptRetry(ctx, source, retry, revision)
	}
	applies, err := taskOwnsScriptRemoval(source)
	if err != nil || !applies {
		return scriptTaskChange{}, err
	}
	if retry.Type != source.Type || retry.Target != source.Target ||
		retry.Params[TaskResourceKindParam] != TaskResourceScript {
		return scriptTaskChange{}, errs.New(errs.KindInternal, "Script retry changed its durable target")
	}
	storage, err := readActiveScriptStorage(ctx, repository.store, source.Target, revision)
	if err != nil {
		return scriptTaskChange{}, err
	}
	stored, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{deletionTombstoneKey(string(DeletionTargetScript), source.Target)}, Revision: revision,
	})
	if err != nil {
		return scriptTaskChange{}, err
	}
	if stored == nil || len(stored.Values) != 1 || stored.Values[0] != nil {
		return scriptTaskChange{}, errs.New(errs.KindStateConflict, "Script is not available for deletion retry")
	}
	record := storage.Script.Record
	service, err := findServiceAtRevision(ctx, repository.store, record.ServiceID, revision)
	if err != nil || service.Record.EnvironmentID != record.EnvironmentID {
		return scriptTaskChange{}, errs.New(errs.KindResourceInUse, "Script target Service is unavailable")
	}
	dependencies, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{
			scriptSetOwnerKey(record.EnvironmentID, record.ScriptSetGeneration, record.Desired.ID),
			scriptSetSlugKey(record.EnvironmentID, record.ScriptSetGeneration, record.Desired.Slug),
			hierarchyrecord.EnvironmentKey(record.EnvironmentID),
			service.Record.desiredFenceKey,
		},
		Revision: revision,
	})
	if err != nil {
		return scriptTaskChange{}, err
	}
	if dependencies == nil || len(dependencies.Values) != 4 || dependencies.Values[0] == nil ||
		dependencies.Values[1] == nil || dependencies.Values[2] == nil || dependencies.Values[3] == nil ||
		string(dependencies.Values[0].Value) != record.Desired.ID ||
		string(dependencies.Values[1].Value) != record.Desired.ID {
		return scriptTaskChange{}, errs.New(errs.KindInternal, "Script retry dependencies are corrupt")
	}
	environment, err := hierarchyrecord.DecodeEnvironment(dependencies.Values[2].Value)
	if err != nil || environment.ID != record.EnvironmentID {
		return scriptTaskChange{}, recordcodec.CorruptRecord()
	}
	if dependencies.Values[3].ModRevision != service.Revision {
		return scriptTaskChange{}, recordcodec.CorruptRecord()
	}
	parents, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{
			hierarchyrecord.ProjectKey(environment.ProjectID),
			deletionTombstoneKey(string(DeletionTargetEnvironment), environment.ID),
			deletionTombstoneKey(string(DeletionTargetProject), environment.ProjectID),
			deletionTombstoneKey("service", service.Record.Desired.ID),
		},
		Revision: revision,
	})
	if err != nil {
		return scriptTaskChange{}, err
	}
	if parents == nil || len(parents.Values) != 4 || parents.Values[0] == nil ||
		parents.Values[1] != nil || parents.Values[2] != nil || parents.Values[3] != nil {
		return scriptTaskChange{}, errs.New(errs.KindResourceInUse, "Script owner is unavailable")
	}
	project, err := hierarchyrecord.DecodeProject(parents.Values[0].Value)
	if err != nil || project.ID != environment.ProjectID {
		return scriptTaskChange{}, recordcodec.CorruptRecord()
	}
	change := scriptTaskChange{
		applies: true,
		conditions: []etcdstore.Condition{
			{
				Key:         scriptSetScriptKey(record.EnvironmentID, record.ScriptSetGeneration, record.Desired.ID),
				ModRevision: storage.Script.Revision,
			},
			{
				Key:         scriptSetOwnerKey(record.EnvironmentID, record.ScriptSetGeneration, record.Desired.ID),
				ModRevision: dependencies.Values[0].ModRevision,
			},
			{
				Key:         scriptSetSlugKey(record.EnvironmentID, record.ScriptSetGeneration, record.Desired.Slug),
				ModRevision: dependencies.Values[1].ModRevision,
			},
			{Key: deletionTombstoneKey(string(DeletionTargetScript), record.Desired.ID)},
			{Key: hierarchyrecord.EnvironmentKey(environment.ID), ModRevision: dependencies.Values[2].ModRevision},
			serviceDesiredCondition(service),
			{Key: hierarchyrecord.ProjectKey(project.ID), ModRevision: parents.Values[0].ModRevision},
			{Key: deletionTombstoneKey(string(DeletionTargetEnvironment), environment.ID)},
			{Key: deletionTombstoneKey(string(DeletionTargetProject), project.ID)},
			{Key: deletionTombstoneKey("service", service.Record.Desired.ID)},
			{Key: scriptSetActiveKey(record.EnvironmentID), ModRevision: storage.Active.Revision},
		},
	}
	if project.TenantID != "" {
		tenantFence, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
			Keys: []string{deletionTombstoneKey(string(DeletionTargetTenant), project.TenantID)}, Revision: revision,
		})
		if err != nil {
			return scriptTaskChange{}, err
		}
		if tenantFence == nil || len(tenantFence.Values) != 1 || tenantFence.Values[0] != nil {
			return scriptTaskChange{}, errs.New(errs.KindResourceInUse, "Script owner is unavailable")
		}
		change.conditions = append(change.conditions, etcdstore.Condition{
			Key: deletionTombstoneKey(string(DeletionTargetTenant), project.TenantID),
		})
	}
	tombstone := DeletionTombstoneRecord{
		TargetKind: DeletionTargetScript, TargetID: record.Desired.ID,
		TargetRevision: storage.Script.Revision, TaskID: retry.ID,
		Phase: DeletionPhaseFinalizing, CreatedAt: retry.CreatedAt, UpdatedAt: retry.CreatedAt,
	}
	value, err := encodeDeletionTombstone(tombstone)
	if err != nil {
		return scriptTaskChange{}, err
	}
	change.values = append(change.values, value)
	activeValue, err := encodeScriptSetGeneration(storage.Active.Record)
	if err != nil {
		return scriptTaskChange{}, err
	}
	change.values = append(change.values, activeValue)
	change.mutations = append(change.mutations, etcdstore.Mutation{
		Type: etcdstore.MutationPut, Key: deletionTombstoneKey(string(DeletionTargetScript), record.Desired.ID), Value: value,
	}, etcdstore.Mutation{Type: etcdstore.MutationPut, Key: scriptSetActiveKey(record.EnvironmentID), Value: activeValue})
	return change, nil
}

func (repository *TaskRepository) prepareScriptTaskAcknowledgement(
	ctx context.Context,
	task TaskRecord,
	terminalStatus TaskStatus,
	revision int64,
) (scriptTaskChange, error) {
	applies, err := taskOwnsScriptRemoval(task)
	if err != nil || !applies {
		return scriptTaskChange{}, err
	}
	storage, err := readActiveScriptStorage(ctx, repository.store, task.Target, revision)
	if err != nil {
		return scriptTaskChange{}, err
	}
	stored, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{deletionTombstoneKey(string(DeletionTargetScript), task.Target)}, Revision: revision,
	})
	if err != nil {
		return scriptTaskChange{}, err
	}
	if stored == nil || len(stored.Values) != 1 || stored.Values[0] == nil {
		return scriptTaskChange{}, errs.New(errs.KindInternal, "Script deletion state is inconsistent")
	}
	record := storage.Script.Record
	tombstone, err := decodeDeletionTombstone(stored.Values[0].Value)
	if err != nil || tombstone.TargetKind != DeletionTargetScript || tombstone.TargetID != task.Target ||
		tombstone.TargetRevision != storage.Script.Revision || tombstone.TaskID != task.ID ||
		tombstone.Phase != DeletionPhaseFinalizing {
		return scriptTaskChange{}, errs.New(errs.KindStateConflict, "Script deletion tombstone does not match its Task")
	}
	indexes, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{
			scriptSetOwnerKey(record.EnvironmentID, record.ScriptSetGeneration, record.Desired.ID),
			scriptSetSlugKey(record.EnvironmentID, record.ScriptSetGeneration, record.Desired.Slug),
		},
		Revision: revision,
	})
	if err != nil {
		return scriptTaskChange{}, err
	}
	if indexes == nil || len(indexes.Values) != 2 || indexes.Values[0] == nil || indexes.Values[1] == nil ||
		string(indexes.Values[0].Value) != record.Desired.ID || string(indexes.Values[1].Value) != record.Desired.ID {
		return scriptTaskChange{}, errs.New(errs.KindInternal, "Script deletion indexes are corrupt")
	}
	change := scriptTaskChange{
		applies: true,
		conditions: []etcdstore.Condition{
			{
				Key:         scriptSetScriptKey(record.EnvironmentID, record.ScriptSetGeneration, task.Target),
				ModRevision: storage.Script.Revision,
			},
			{
				Key:         deletionTombstoneKey(string(DeletionTargetScript), task.Target),
				ModRevision: stored.Values[0].ModRevision,
			},
			{
				Key:         scriptSetOwnerKey(record.EnvironmentID, record.ScriptSetGeneration, task.Target),
				ModRevision: indexes.Values[0].ModRevision,
			},
			{
				Key:         scriptSetSlugKey(record.EnvironmentID, record.ScriptSetGeneration, record.Desired.Slug),
				ModRevision: indexes.Values[1].ModRevision,
			},
			{Key: scriptSetActiveKey(record.EnvironmentID), ModRevision: storage.Active.Revision},
			{Key: scriptLocatorKey(task.Target), ModRevision: storage.Locator.Revision},
			{
				Key:         scriptEnvironmentLocatorKey(record.EnvironmentID, task.Target),
				ModRevision: storage.EnvironmentLocator.Revision,
			},
		},
		mutations: []etcdstore.Mutation{{
			Type: etcdstore.MutationDelete, Key: deletionTombstoneKey(string(DeletionTargetScript), task.Target),
		}},
	}
	if terminalStatus == TaskStatusCompleted {
		change.mutations = append(
			change.mutations,
			etcdstore.Mutation{
				Type: etcdstore.MutationDelete,
				Key:  scriptSetOwnerKey(record.EnvironmentID, record.ScriptSetGeneration, task.Target),
			},
			etcdstore.Mutation{
				Type: etcdstore.MutationDelete,
				Key:  scriptSetSlugKey(record.EnvironmentID, record.ScriptSetGeneration, record.Desired.Slug),
			},
			etcdstore.Mutation{
				Type:   etcdstore.MutationDelete,
				Key:    scriptSetBodyGenerationPrefix(record.EnvironmentID, record.ScriptSetGeneration, task.Target),
				Prefix: true,
			},
			etcdstore.Mutation{
				Type: etcdstore.MutationDelete,
				Key:  scriptSetScriptKey(record.EnvironmentID, record.ScriptSetGeneration, task.Target),
			},
			etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: scriptLocatorKey(task.Target)},
			etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: scriptEnvironmentLocatorKey(record.EnvironmentID, task.Target)},
		)
	}
	activeValue, err := encodeScriptSetGeneration(storage.Active.Record)
	if err != nil {
		return scriptTaskChange{}, err
	}
	change.values = append(change.values, activeValue)
	change.mutations = append(
		change.mutations,
		etcdstore.Mutation{Type: etcdstore.MutationPut, Key: scriptSetActiveKey(record.EnvironmentID), Value: activeValue},
	)
	return change, nil
}

func (repository *TaskRepository) validateScriptTaskAcknowledgementReplay(
	ctx context.Context,
	task TaskRecord,
	terminalStatus TaskStatus,
	revision int64,
) error {
	applies, err := taskOwnsScriptRemoval(task)
	if err != nil || !applies {
		return err
	}
	stored, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{deletionTombstoneKey(string(DeletionTargetScript), task.Target)}, Revision: revision,
	})
	if err != nil {
		return err
	}
	if stored == nil || len(stored.Values) != 1 || stored.Values[0] != nil {
		return errs.New(errs.KindStateConflict, "Script deletion terminal state does not match its Task")
	}
	storage, scriptErr := readActiveScriptStorage(ctx, repository.store, task.Target, revision)
	if terminalStatus == TaskStatusCompleted {
		if scriptErr == nil || !errors.Is(scriptErr, errs.New(errs.KindScriptNotFound, "")) {
			return errs.New(errs.KindStateConflict, "completed Script deletion retained its target")
		}
		return nil
	}
	if scriptErr != nil {
		return errs.New(errs.KindStateConflict, "failed Script deletion lost its target")
	}
	if storage.Script.Record.Desired.ID != task.Target {
		return errs.New(errs.KindStateConflict, "failed Script deletion retained another target")
	}
	return nil
}

func taskOwnsScriptRemoval(task TaskRecord) (bool, error) {
	if task.Executor != TaskExecutorController || task.Params[TaskResourceKindParam] != TaskResourceScript {
		return false, nil
	}
	if task.Type != TaskRemove || len(task.Params) != 1 || ids.Validate(ids.KindScript, task.Target) != nil {
		return false, errs.New(errs.KindInternal, "Script deletion Task has invalid durable input")
	}
	return true, nil
}

func clearScriptTaskChange(change scriptTaskChange) {
	for _, value := range change.values {
		clear(value)
	}
}
