package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type scriptTaskChange struct {
	applies    bool
	conditions []Condition
	mutations  []Mutation
	values     [][]byte
}

func (repository *TaskRepository) prepareScriptTaskRetry(
	ctx context.Context,
	source TaskRecord,
	retry TaskRecord,
	revision int64,
) (scriptTaskChange, error) {
	applies, err := taskOwnsScriptRemoval(source)
	if err != nil || !applies {
		return scriptTaskChange{}, err
	}
	if retry.Type != source.Type || retry.Target != source.Target ||
		retry.Params[TaskResourceKindParam] != TaskResourceScript {
		return scriptTaskChange{}, errs.New(errs.KindInternal, "Script retry changed its durable target")
	}
	stored, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys: []string{
			scriptKey(source.Target),
			deletionTombstoneKey(string(DeletionTargetScript), source.Target),
		},
		Revision: revision,
	})
	if err != nil {
		return scriptTaskChange{}, err
	}
	if stored == nil || len(stored.Values) != 2 || stored.Values[0] == nil || stored.Values[1] != nil {
		return scriptTaskChange{}, errs.New(errs.KindStateConflict, "Script is not available for deletion retry")
	}
	record, err := decodeScriptRecord(stored.Values[0].Value)
	if err != nil || record.Desired.ID != source.Target {
		return scriptTaskChange{}, corruptRecord()
	}
	dependencies, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys: []string{
			scriptOwnerKey(record.EnvironmentID, record.Desired.ID),
			scriptSlugKey(record.EnvironmentID, record.Desired.Slug),
			environmentKey(record.EnvironmentID),
			serviceKey(record.ServiceID),
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
	environment, err := decodeEnvironment(dependencies.Values[2].Value)
	if err != nil || environment.ID != record.EnvironmentID {
		return scriptTaskChange{}, corruptRecord()
	}
	service, err := decodeServiceRecord(dependencies.Values[3].Value)
	if err != nil || service.Desired.ID != record.ServiceID || service.EnvironmentID != record.EnvironmentID {
		return scriptTaskChange{}, corruptRecord()
	}
	parents, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys: []string{
			projectKey(environment.ProjectID),
			deletionTombstoneKey(string(DeletionTargetEnvironment), environment.ID),
			deletionTombstoneKey(string(DeletionTargetProject), environment.ProjectID),
			deletionTombstoneKey("service", service.Desired.ID),
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
	project, err := decodeProject(parents.Values[0].Value)
	if err != nil || project.ID != environment.ProjectID {
		return scriptTaskChange{}, corruptRecord()
	}
	change := scriptTaskChange{
		applies: true,
		conditions: []Condition{
			{Key: scriptKey(record.Desired.ID), ModRevision: stored.Values[0].ModRevision},
			{
				Key:         scriptOwnerKey(record.EnvironmentID, record.Desired.ID),
				ModRevision: dependencies.Values[0].ModRevision,
			},
			{
				Key:         scriptSlugKey(record.EnvironmentID, record.Desired.Slug),
				ModRevision: dependencies.Values[1].ModRevision,
			},
			{Key: deletionTombstoneKey(string(DeletionTargetScript), record.Desired.ID)},
			{Key: environmentKey(environment.ID), ModRevision: dependencies.Values[2].ModRevision},
			{Key: serviceKey(service.Desired.ID), ModRevision: dependencies.Values[3].ModRevision},
			{Key: projectKey(project.ID), ModRevision: parents.Values[0].ModRevision},
			{Key: deletionTombstoneKey(string(DeletionTargetEnvironment), environment.ID)},
			{Key: deletionTombstoneKey(string(DeletionTargetProject), project.ID)},
			{Key: deletionTombstoneKey("service", service.Desired.ID)},
		},
	}
	if project.TenantID != "" {
		tenantFence, err := repository.store.GetMany(ctx, GetManyRequest{
			Keys: []string{deletionTombstoneKey(string(DeletionTargetTenant), project.TenantID)}, Revision: revision,
		})
		if err != nil {
			return scriptTaskChange{}, err
		}
		if tenantFence == nil || len(tenantFence.Values) != 1 || tenantFence.Values[0] != nil {
			return scriptTaskChange{}, errs.New(errs.KindResourceInUse, "Script owner is unavailable")
		}
		change.conditions = append(change.conditions, Condition{
			Key: deletionTombstoneKey(string(DeletionTargetTenant), project.TenantID),
		})
	}
	tombstone := DeletionTombstoneRecord{
		TargetKind: DeletionTargetScript, TargetID: record.Desired.ID,
		TargetRevision: stored.Values[0].ModRevision, TaskID: retry.ID,
		Phase: DeletionPhaseFinalizing, CreatedAt: retry.CreatedAt, UpdatedAt: retry.CreatedAt,
	}
	value, err := encodeDeletionTombstone(tombstone)
	if err != nil {
		return scriptTaskChange{}, err
	}
	change.values = append(change.values, value)
	change.mutations = append(change.mutations, Mutation{
		Type: MutationPut, Key: deletionTombstoneKey(string(DeletionTargetScript), record.Desired.ID), Value: value,
	})
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
	stored, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys: []string{
			scriptKey(task.Target), deletionTombstoneKey(string(DeletionTargetScript), task.Target),
		},
		Revision: revision,
	})
	if err != nil {
		return scriptTaskChange{}, err
	}
	if stored == nil || len(stored.Values) != 2 || stored.Values[0] == nil || stored.Values[1] == nil {
		return scriptTaskChange{}, errs.New(errs.KindInternal, "Script deletion state is inconsistent")
	}
	record, err := decodeScriptRecord(stored.Values[0].Value)
	if err != nil || record.Desired.ID != task.Target {
		return scriptTaskChange{}, corruptRecord()
	}
	tombstone, err := decodeDeletionTombstone(stored.Values[1].Value)
	if err != nil || tombstone.TargetKind != DeletionTargetScript || tombstone.TargetID != task.Target ||
		tombstone.TargetRevision != stored.Values[0].ModRevision || tombstone.TaskID != task.ID ||
		tombstone.Phase != DeletionPhaseFinalizing {
		return scriptTaskChange{}, errs.New(errs.KindStateConflict, "Script deletion tombstone does not match its Task")
	}
	indexes, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys: []string{
			scriptOwnerKey(record.EnvironmentID, record.Desired.ID),
			scriptSlugKey(record.EnvironmentID, record.Desired.Slug),
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
		conditions: []Condition{
			{Key: scriptKey(task.Target), ModRevision: stored.Values[0].ModRevision},
			{
				Key:         deletionTombstoneKey(string(DeletionTargetScript), task.Target),
				ModRevision: stored.Values[1].ModRevision,
			},
			{Key: scriptOwnerKey(record.EnvironmentID, task.Target), ModRevision: indexes.Values[0].ModRevision},
			{Key: scriptSlugKey(record.EnvironmentID, record.Desired.Slug), ModRevision: indexes.Values[1].ModRevision},
		},
		mutations: []Mutation{{
			Type: MutationDelete, Key: deletionTombstoneKey(string(DeletionTargetScript), task.Target),
		}},
	}
	if terminalStatus == TaskStatusCompleted {
		change.mutations = append(change.mutations,
			Mutation{Type: MutationDelete, Key: scriptOwnerKey(record.EnvironmentID, task.Target)},
			Mutation{Type: MutationDelete, Key: scriptSlugKey(record.EnvironmentID, record.Desired.Slug)},
			Mutation{Type: MutationDelete, Key: scriptBodyGenerationPrefix(task.Target), Prefix: true},
			Mutation{Type: MutationDelete, Key: scriptKey(task.Target)},
		)
	}
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
	stored, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys: []string{
			scriptKey(task.Target), deletionTombstoneKey(string(DeletionTargetScript), task.Target),
		},
		Revision: revision,
	})
	if err != nil {
		return err
	}
	if stored == nil || len(stored.Values) != 2 || stored.Values[1] != nil {
		return errs.New(errs.KindStateConflict, "Script deletion terminal state does not match its Task")
	}
	if terminalStatus == TaskStatusCompleted {
		if stored.Values[0] != nil {
			return errs.New(errs.KindStateConflict, "completed Script deletion retained its target")
		}
		return nil
	}
	if stored.Values[0] == nil {
		return errs.New(errs.KindStateConflict, "failed Script deletion lost its target")
	}
	record, err := decodeScriptRecord(stored.Values[0].Value)
	if err != nil || record.Desired.ID != task.Target {
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
