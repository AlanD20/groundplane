package etcd

import (
	"context"
	"errors"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	recordcodec "github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	scriptrecord "github.com/AlanD20/groundplane/internal/infra/etcd/scripts"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"

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
	if source.Type == taskjournal.TaskScript {
		return repository.prepareManualScriptRetry(ctx, source, retry, revision)
	}
	applies, err := taskOwnsScriptRemoval(source)
	if err != nil || !applies {
		return scriptTaskChange{}, err
	}
	if retry.Type != source.Type || retry.Target != source.Target ||
		retry.Params[taskjournal.TaskResourceKindParam] != taskjournal.TaskResourceScript {
		return scriptTaskChange{}, errs.New(errs.KindInternal, "Script retry changed its durable target")
	}
	storage, err := readActiveScriptStorage(ctx, repository.store, source.Target, revision)
	if err != nil {
		return scriptTaskChange{}, err
	}
	stored, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{deletionTombstoneKey(string(deletionrecord.DeletionTargetScript), source.Target)}, Revision: revision,
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
			scriptrecord.ScriptSetOwnerKey(record.EnvironmentID, record.ScriptSetGeneration, record.Desired.ID),
			scriptrecord.ScriptSetSlugKey(record.EnvironmentID, record.ScriptSetGeneration, record.Desired.Slug),
			hierarchyrecord.EnvironmentKey(record.EnvironmentID),
			servicerecord.ServiceDesiredCondition(service).Key,
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
			deletionTombstoneKey(string(deletionrecord.DeletionTargetEnvironment), environment.ID),
			deletionTombstoneKey(string(deletionrecord.DeletionTargetProject), environment.ProjectID),
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
				Key:         scriptrecord.ScriptSetScriptKey(record.EnvironmentID, record.ScriptSetGeneration, record.Desired.ID),
				ModRevision: storage.Script.Revision,
			},
			{
				Key:         scriptrecord.ScriptSetOwnerKey(record.EnvironmentID, record.ScriptSetGeneration, record.Desired.ID),
				ModRevision: dependencies.Values[0].ModRevision,
			},
			{
				Key:         scriptrecord.ScriptSetSlugKey(record.EnvironmentID, record.ScriptSetGeneration, record.Desired.Slug),
				ModRevision: dependencies.Values[1].ModRevision,
			},
			{Key: deletionTombstoneKey(string(deletionrecord.DeletionTargetScript), record.Desired.ID)},
			{Key: hierarchyrecord.EnvironmentKey(environment.ID), ModRevision: dependencies.Values[2].ModRevision},
			servicerecord.ServiceDesiredCondition(service),
			{Key: hierarchyrecord.ProjectKey(project.ID), ModRevision: parents.Values[0].ModRevision},
			{Key: deletionTombstoneKey(string(deletionrecord.DeletionTargetEnvironment), environment.ID)},
			{Key: deletionTombstoneKey(string(deletionrecord.DeletionTargetProject), project.ID)},
			{Key: deletionTombstoneKey("service", service.Record.Desired.ID)},
			{Key: scriptrecord.ScriptSetActiveKey(record.EnvironmentID), ModRevision: storage.Active.Revision},
		},
	}
	if project.TenantID != "" {
		tenantFence, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
			Keys: []string{deletionTombstoneKey(string(deletionrecord.DeletionTargetTenant), project.TenantID)}, Revision: revision,
		})
		if err != nil {
			return scriptTaskChange{}, err
		}
		if tenantFence == nil || len(tenantFence.Values) != 1 || tenantFence.Values[0] != nil {
			return scriptTaskChange{}, errs.New(errs.KindResourceInUse, "Script owner is unavailable")
		}
		change.conditions = append(change.conditions, etcdstore.Condition{
			Key: deletionTombstoneKey(string(deletionrecord.DeletionTargetTenant), project.TenantID),
		})
	}
	tombstone := deletionrecord.DeletionTombstoneRecord{
		TargetKind: deletionrecord.DeletionTargetScript, TargetID: record.Desired.ID,
		TargetRevision: storage.Script.Revision, TaskID: retry.ID,
		Phase: deletionrecord.DeletionPhaseFinalizing, CreatedAt: retry.CreatedAt, UpdatedAt: retry.CreatedAt,
	}
	value, err := deletionrecord.EncodeDeletionTombstone(tombstone)
	if err != nil {
		return scriptTaskChange{}, err
	}
	change.values = append(change.values, value)
	activeValue, err := scriptrecord.EncodeScriptSetGeneration(storage.Active.Record)
	if err != nil {
		return scriptTaskChange{}, err
	}
	change.values = append(change.values, activeValue)
	change.mutations = append(change.mutations, etcdstore.Mutation{
		Type: etcdstore.MutationPut, Key: deletionTombstoneKey(string(deletionrecord.DeletionTargetScript), record.Desired.ID), Value: value,
	}, etcdstore.Mutation{Type: etcdstore.MutationPut, Key: scriptrecord.ScriptSetActiveKey(record.EnvironmentID), Value: activeValue})
	return change, nil
}

func (repository *TaskRepository) prepareScriptTaskAcknowledgement(
	ctx context.Context,
	task TaskRecord,
	terminalStatus taskjournal.TaskStatus,
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
		Keys: []string{deletionTombstoneKey(string(deletionrecord.DeletionTargetScript), task.Target)}, Revision: revision,
	})
	if err != nil {
		return scriptTaskChange{}, err
	}
	if stored == nil || len(stored.Values) != 1 || stored.Values[0] == nil {
		return scriptTaskChange{}, errs.New(errs.KindInternal, "Script deletion state is inconsistent")
	}
	record := storage.Script.Record
	tombstone, err := deletionrecord.DecodeDeletionTombstone(stored.Values[0].Value)
	if err != nil || tombstone.TargetKind != deletionrecord.DeletionTargetScript || tombstone.TargetID != task.Target ||
		tombstone.TargetRevision != storage.Script.Revision || tombstone.TaskID != task.ID ||
		tombstone.Phase != deletionrecord.DeletionPhaseFinalizing {
		return scriptTaskChange{}, errs.New(errs.KindStateConflict, "Script deletion tombstone does not match its Task")
	}
	indexes, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{
			scriptrecord.ScriptSetOwnerKey(record.EnvironmentID, record.ScriptSetGeneration, record.Desired.ID),
			scriptrecord.ScriptSetSlugKey(record.EnvironmentID, record.ScriptSetGeneration, record.Desired.Slug),
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
				Key:         scriptrecord.ScriptSetScriptKey(record.EnvironmentID, record.ScriptSetGeneration, task.Target),
				ModRevision: storage.Script.Revision,
			},
			{
				Key:         deletionTombstoneKey(string(deletionrecord.DeletionTargetScript), task.Target),
				ModRevision: stored.Values[0].ModRevision,
			},
			{
				Key:         scriptrecord.ScriptSetOwnerKey(record.EnvironmentID, record.ScriptSetGeneration, task.Target),
				ModRevision: indexes.Values[0].ModRevision,
			},
			{
				Key:         scriptrecord.ScriptSetSlugKey(record.EnvironmentID, record.ScriptSetGeneration, record.Desired.Slug),
				ModRevision: indexes.Values[1].ModRevision,
			},
			{Key: scriptrecord.ScriptSetActiveKey(record.EnvironmentID), ModRevision: storage.Active.Revision},
			{Key: scriptrecord.ScriptLocatorKey(task.Target), ModRevision: storage.Locator.Revision},
			{
				Key:         scriptrecord.ScriptEnvironmentLocatorKey(record.EnvironmentID, task.Target),
				ModRevision: storage.EnvironmentLocator.Revision,
			},
		},
		mutations: []etcdstore.Mutation{{
			Type: etcdstore.MutationDelete, Key: deletionTombstoneKey(string(deletionrecord.DeletionTargetScript), task.Target),
		}},
	}
	if terminalStatus == taskjournal.TaskStatusCompleted {
		change.mutations = append(
			change.mutations,
			etcdstore.Mutation{
				Type: etcdstore.MutationDelete,
				Key:  scriptrecord.ScriptSetOwnerKey(record.EnvironmentID, record.ScriptSetGeneration, task.Target),
			},
			etcdstore.Mutation{
				Type: etcdstore.MutationDelete,
				Key:  scriptrecord.ScriptSetSlugKey(record.EnvironmentID, record.ScriptSetGeneration, record.Desired.Slug),
			},
			etcdstore.Mutation{
				Type:   etcdstore.MutationDelete,
				Key:    scriptrecord.ScriptSetBodyGenerationPrefix(record.EnvironmentID, record.ScriptSetGeneration, task.Target),
				Prefix: true,
			},
			etcdstore.Mutation{
				Type: etcdstore.MutationDelete,
				Key:  scriptrecord.ScriptSetScriptKey(record.EnvironmentID, record.ScriptSetGeneration, task.Target),
			},
			etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: scriptrecord.ScriptLocatorKey(task.Target)},
			etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: scriptrecord.ScriptEnvironmentLocatorKey(record.EnvironmentID, task.Target)},
		)
	}
	activeValue, err := scriptrecord.EncodeScriptSetGeneration(storage.Active.Record)
	if err != nil {
		return scriptTaskChange{}, err
	}
	change.values = append(change.values, activeValue)
	change.mutations = append(
		change.mutations,
		etcdstore.Mutation{Type: etcdstore.MutationPut, Key: scriptrecord.ScriptSetActiveKey(record.EnvironmentID), Value: activeValue},
	)
	return change, nil
}

func (repository *TaskRepository) validateScriptTaskAcknowledgementReplay(
	ctx context.Context,
	task TaskRecord,
	terminalStatus taskjournal.TaskStatus,
	revision int64,
) error {
	applies, err := taskOwnsScriptRemoval(task)
	if err != nil || !applies {
		return err
	}
	stored, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{deletionTombstoneKey(string(deletionrecord.DeletionTargetScript), task.Target)}, Revision: revision,
	})
	if err != nil {
		return err
	}
	if stored == nil || len(stored.Values) != 1 || stored.Values[0] != nil {
		return errs.New(errs.KindStateConflict, "Script deletion terminal state does not match its Task")
	}
	storage, scriptErr := readActiveScriptStorage(ctx, repository.store, task.Target, revision)
	if terminalStatus == taskjournal.TaskStatusCompleted {
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
	if task.Executor != taskjournal.TaskExecutorController || task.Params[taskjournal.TaskResourceKindParam] != taskjournal.TaskResourceScript {
		return false, nil
	}
	if task.Type != taskjournal.TaskRemove || len(task.Params) != 1 || ids.Validate(ids.KindScript, task.Target) != nil {
		return false, errs.New(errs.KindInternal, "Script deletion Task has invalid durable input")
	}
	return true, nil
}

func clearScriptTaskChange(change scriptTaskChange) {
	for _, value := range change.values {
		clear(value)
	}
}
