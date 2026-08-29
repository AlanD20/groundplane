package etcd

import (
	"bytes"
	"context"
	"encoding/json"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type taskMaterializationWriterRecord struct {
	EnvironmentID    string
	TaskID           string
	RenderGeneration int32
}

type taskMaterializationProjectionChange struct {
	applies    bool
	conditions []Condition
	mutations  []Mutation
}

type taskMaterializationWriterJSON struct {
	Schema           int    `json:"schema"`
	EnvironmentID    string `json:"environment_id"`
	TaskID           string `json:"task_id"`
	RenderGeneration int32  `json:"render_generation"`
}

func taskMaterializationEnvironment(record TaskRecord) (string, bool, error) {
	environmentID, declared := record.Params[TaskMaterializationEnvironmentParam]
	if !declared {
		return "", false, nil
	}
	resourceRemoval := record.Type == TaskRemove &&
		(validateStableID(ids.KindRoute, record.Target) == nil || validateStableID(ids.KindEnvEntry, record.Target) == nil)
	volumeMutation := record.Params[TaskResourceKindParam] == TaskResourceVolume &&
		validateStableID(ids.KindVolume, record.Target) == nil &&
		(record.Type == TaskCreate || record.Type == TaskUpdate || record.Type == TaskRemove)
	if record.Executor != TaskExecutorAgent || validateStableID(ids.KindEnvironment, environmentID) != nil ||
		(record.Target != environmentID && !resourceRemoval && !volumeMutation) {
		return "", false, errs.New(errs.KindValidationFailed, "task materialization Environment is invalid")
	}
	return environmentID, true, nil
}

func (repository *TaskRepository) prepareTaskMaterializationProjectionAcknowledgement(
	ctx context.Context,
	record TaskRecord,
	terminalStatus TaskStatus,
	readRevision int64,
) (taskMaterializationProjectionChange, error) {
	if terminalStatus != TaskStatusCompleted {
		return taskMaterializationProjectionChange{}, nil
	}
	revisionID, declared := record.Params[EnvironmentDesiredRevisionParam]
	if !declared {
		return taskMaterializationProjectionChange{}, nil
	}
	environmentID, materializes, err := taskMaterializationEnvironment(record)
	if err != nil {
		return taskMaterializationProjectionChange{}, err
	}
	if !materializes || taskHasSpecializedProjectionAcknowledgement(record) {
		return taskMaterializationProjectionChange{}, nil
	}
	if validateStableID(ids.KindTask, revisionID) != nil || record.RenderGeneration <= 0 {
		return taskMaterializationProjectionChange{}, errs.New(
			errs.KindStateConflict,
			"task desired revision identity is invalid",
		)
	}

	rootKey := environmentBlueprintRootKey(environmentID, revisionID)
	projectionKey := environmentComposeProjectionKey(environmentID)
	state, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys: []string{rootKey, projectionKey}, Revision: readRevision,
	})
	if err != nil {
		return taskMaterializationProjectionChange{}, err
	}
	if state == nil || state.ReadRevision != readRevision || len(state.Values) != 2 || state.Values[0] == nil {
		return taskMaterializationProjectionChange{}, errs.New(
			errs.KindStateConflict,
			"task desired revision is unavailable",
		)
	}
	seal, err := decodeEnvironmentBlueprintSeal(state.Values[0].Value)
	if err != nil || seal.EnvironmentID != environmentID || seal.RevisionID != revisionID {
		return taskMaterializationProjectionChange{}, corruptEnvironmentComposeProjection()
	}
	chunkKeys := make([]string, seal.ProjectionChunks)
	for index := range chunkKeys {
		chunkKeys[index] = environmentBlueprintChunkKeyFor(
			environmentID,
			revisionID,
			EnvironmentBlueprintChunkProjection,
			uint32(index),
		)
	}
	hierarchy := &HierarchyRepository{store: repository.store}
	projectionValue, projectionRevision, err := hierarchy.readEnvironmentBlueprintStreamAtRevision(
		ctx,
		seal,
		"projection",
		chunkKeys,
		readRevision,
	)
	if err != nil {
		return taskMaterializationProjectionChange{}, err
	}
	if projectionRevision != readRevision {
		clear(projectionValue)
		return taskMaterializationProjectionChange{}, errs.New(
			errs.KindInternal,
			"task desired projection read changed revision",
		)
	}
	projection, err := decodeEnvironmentComposeProjection(projectionValue)
	if err != nil || projection.EnvironmentID != environmentID || projection.RevisionID != revisionID ||
		projection.RenderGeneration != uint64(record.RenderGeneration) {
		clear(projectionValue)
		return taskMaterializationProjectionChange{}, errs.New(
			errs.KindStateConflict,
			"task desired projection identity changed",
		)
	}
	projectionRevisionCondition := int64(0)
	if state.Values[1] != nil {
		current, decodeErr := decodeEnvironmentComposeProjection(state.Values[1].Value)
		if decodeErr != nil || current.EnvironmentID != environmentID {
			clear(projectionValue)
			return taskMaterializationProjectionChange{}, corruptEnvironmentComposeProjection()
		}
		if projection.RenderGeneration <= current.RenderGeneration {
			clear(projectionValue)
			return taskMaterializationProjectionChange{}, errs.New(
				errs.KindStateConflict,
				"task desired projection is not newer than the applied projection",
			)
		}
		projectionRevisionCondition = state.Values[1].ModRevision
	}
	return taskMaterializationProjectionChange{
		applies: true,
		conditions: []Condition{
			{Key: rootKey, ModRevision: state.Values[0].ModRevision},
			{Key: projectionKey, ModRevision: projectionRevisionCondition},
		},
		mutations: []Mutation{{Type: MutationPut, Key: projectionKey, Value: projectionValue}},
	}, nil
}

func taskHasSpecializedProjectionAcknowledgement(record TaskRecord) bool {
	return record.Type == TaskRemove &&
		(validateStableID(ids.KindRoute, record.Target) == nil ||
			validateStableID(ids.KindEnvEntry, record.Target) == nil)
}

func clearTaskMaterializationProjectionChange(change taskMaterializationProjectionChange) {
	clearMutationValues(change.mutations)
}

func taskEnvironmentWriter(record TaskRecord) (string, bool, error) {
	materializationEnvironment, materializes, err := taskMaterializationEnvironment(record)
	if err != nil {
		return "", false, err
	}
	mutationEnvironment, mutates := record.Params[TaskMutationEnvironmentParam]
	if materializes && mutates {
		return "", false, errs.New(errs.KindValidationFailed, "task declares multiple Environment writers")
	}
	if materializes {
		return materializationEnvironment, true, nil
	}
	if !mutates {
		return "", false, nil
	}
	if record.Executor != TaskExecutorAgent || validateStableID(ids.KindEnvironment, mutationEnvironment) != nil ||
		(record.Type != TaskAttach && record.Type != TaskDetach) {
		return "", false, errs.New(errs.KindValidationFailed, "task mutation Environment is invalid")
	}
	return mutationEnvironment, true, nil
}

func taskMaterializationWriter(record TaskRecord, environmentID string) taskMaterializationWriterRecord {
	return taskMaterializationWriterRecord{
		EnvironmentID: environmentID, TaskID: record.ID, RenderGeneration: record.RenderGeneration,
	}
}

func encodeTaskMaterializationWriter(record taskMaterializationWriterRecord) ([]byte, error) {
	if err := validateTaskMaterializationWriter(record); err != nil {
		return nil, err
	}
	return json.Marshal(taskMaterializationWriterJSON{
		Schema: 1, EnvironmentID: record.EnvironmentID,
		TaskID: record.TaskID, RenderGeneration: record.RenderGeneration,
	})
}

func decodeTaskMaterializationWriter(value []byte) (taskMaterializationWriterRecord, error) {
	if rejectDuplicateJSONFields(value) != nil {
		return taskMaterializationWriterRecord{}, corruptTaskMaterializationWriter()
	}
	decoder := json.NewDecoder(bytes.NewReader(value))
	decoder.DisallowUnknownFields()
	var data taskMaterializationWriterJSON
	if err := decoder.Decode(&data); err != nil || requireJSONEOF(decoder) != nil || data.Schema != 1 {
		return taskMaterializationWriterRecord{}, corruptTaskMaterializationWriter()
	}
	record := taskMaterializationWriterRecord{
		EnvironmentID: data.EnvironmentID, TaskID: data.TaskID, RenderGeneration: data.RenderGeneration,
	}
	if err := validateTaskMaterializationWriter(record); err != nil {
		return taskMaterializationWriterRecord{}, corruptTaskMaterializationWriter()
	}
	return record, nil
}

func validateTaskMaterializationWriter(record taskMaterializationWriterRecord) error {
	if validateStableID(ids.KindEnvironment, record.EnvironmentID) != nil ||
		validateStableID(ids.KindTask, record.TaskID) != nil || record.RenderGeneration <= 0 {
		return corruptTaskMaterializationWriter()
	}
	return nil
}

func corruptTaskMaterializationWriter() error {
	return errs.New(errs.KindInternal, "task materialization writer record is corrupt")
}
