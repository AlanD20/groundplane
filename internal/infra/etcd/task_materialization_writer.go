package etcd

import (
	"bytes"
	"context"
	"encoding/json"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type taskMaterializationWriterRecord struct {
	EnvironmentID               string
	TaskID                      string
	RenderGeneration            int32
	BlueprintAppliedPredecessor *taskMaterializationAppliedPredecessor
}

type taskMaterializationAppliedPredecessor struct {
	Present          bool   `json:"present"`
	KeyRevision      int64  `json:"key_revision"`
	RevisionID       string `json:"revision_id,omitempty"`
	RenderGeneration uint64 `json:"render_generation"`
}

type taskMaterializationProjectionChange struct {
	applies    bool
	conditions []etcdstore.Condition
	mutations  []etcdstore.Mutation
}

type taskMaterializationWriterJSON struct {
	Schema                      int                                    `json:"schema"`
	EnvironmentID               string                                 `json:"environment_id"`
	TaskID                      string                                 `json:"task_id"`
	RenderGeneration            int32                                  `json:"render_generation"`
	BlueprintAppliedPredecessor *taskMaterializationAppliedPredecessor `json:"blueprint_applied_predecessor"`
}

func (repository *TaskRepository) prepareTaskMaterializationWriter(
	ctx context.Context,
	record TaskRecord,
	environmentID string,
	readRevision int64,
) (taskMaterializationWriterRecord, []etcdstore.Condition, error) {
	entryRuntimeConditions, err := repository.entryRuntimeClaimConditions(ctx, record, readRevision)
	if err != nil {
		return taskMaterializationWriterRecord{}, nil, err
	}
	configurationConditions, err := repository.runtimeConfigurationClaimConditions(
		ctx,
		record,
		readRevision,
	)
	if err != nil {
		return taskMaterializationWriterRecord{}, nil, err
	}
	entryRuntimeConditions = append(entryRuntimeConditions, configurationConditions...)
	if !taskHasBlueprintCandidateAppliedAuthority(record) {
		writer := taskMaterializationWriter(record, environmentID, nil)
		if err := validateTaskMaterializationWriterForTask(writer, record, environmentID); err != nil {
			return taskMaterializationWriterRecord{}, nil, err
		}
		return writer, entryRuntimeConditions, nil
	}
	predecessor, predecessorCondition, err := repository.readTaskMaterializationAppliedPredecessor(
		ctx, environmentID, record.RenderGeneration, readRevision,
	)
	if err != nil {
		return taskMaterializationWriterRecord{}, nil, err
	}
	conditions := append(entryRuntimeConditions, predecessorCondition)
	if record.RetryOf != "" && record.Type == taskjournal.TaskUpdate && record.Params[TaskReleasePublicationParam] != "" {
		authority, authorityCondition, authorityErr := repository.blueprintCandidateAttemptAuthority(
			ctx, record, readRevision,
		)
		if authorityErr != nil {
			return taskMaterializationWriterRecord{}, nil, authorityErr
		}
		if authority.AppliedPredecessor != predecessor {
			return taskMaterializationWriterRecord{}, nil, errs.New(
				errs.KindStateConflict,
				"blueprint retry applied predecessor changed",
			)
		}
		conditions = append(conditions, authorityCondition)
	}
	writer := taskMaterializationWriter(record, environmentID, &predecessor)
	if err := validateTaskMaterializationWriterForTask(writer, record, environmentID); err != nil {
		return taskMaterializationWriterRecord{}, nil, err
	}
	return writer, conditions, nil
}

func (repository *TaskRepository) readTaskMaterializationAppliedPredecessor(
	ctx context.Context,
	environmentID string,
	candidateGeneration int32,
	readRevision int64,
) (taskMaterializationAppliedPredecessor, etcdstore.Condition, error) {
	key := projectionrecord.EnvironmentComposeProjectionStorageKey(environmentID)
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{key}, Revision: readRevision})
	if err != nil {
		return taskMaterializationAppliedPredecessor{}, etcdstore.Condition{}, err
	}
	if read == nil || read.ReadRevision != readRevision || len(read.Values) != 1 {
		return taskMaterializationAppliedPredecessor{}, etcdstore.Condition{}, errs.New(
			errs.KindInternal,
			"task applied predecessor read is incomplete",
		)
	}
	predecessor, err := taskMaterializationAppliedPredecessorFromValue(
		read.Values[0], environmentID, candidateGeneration,
	)
	if err != nil {
		return taskMaterializationAppliedPredecessor{}, etcdstore.Condition{}, err
	}
	return predecessor, etcdstore.Condition{Key: key, ModRevision: keyValueRevision(read.Values[0])}, nil
}

func taskMaterializationAppliedPredecessorFromValue(
	value *etcdstore.KeyValue,
	environmentID string,
	candidateGeneration int32,
) (taskMaterializationAppliedPredecessor, error) {
	if candidateGeneration <= 0 {
		return taskMaterializationAppliedPredecessor{}, errs.New(
			errs.KindStateConflict,
			"task candidate render generation is invalid",
		)
	}
	if value == nil {
		return taskMaterializationAppliedPredecessor{}, nil
	}
	projection, err := projectionrecord.DecodeEnvironmentComposeProjectionStorage(value.Value)
	if err != nil || projection.EnvironmentID != environmentID ||
		recordcodec.ValidateID(ids.KindTask, projection.RevisionID) != nil ||
		projection.RenderGeneration >= uint64(candidateGeneration) {
		return taskMaterializationAppliedPredecessor{}, errs.New(
			errs.KindStateConflict,
			"task applied predecessor identity is invalid",
		)
	}
	return taskMaterializationAppliedPredecessor{
		Present: true, KeyRevision: value.ModRevision,
		RevisionID: projection.RevisionID, RenderGeneration: projection.RenderGeneration,
	}, nil
}

func validateTaskMaterializationWriterForTask(
	writer taskMaterializationWriterRecord,
	record TaskRecord,
	environmentID string,
) error {
	if writer.EnvironmentID != environmentID || writer.TaskID != record.ID ||
		writer.RenderGeneration != record.RenderGeneration {
		return corruptTaskMaterializationWriter()
	}
	blueprintCandidate := taskHasBlueprintCandidateAppliedAuthority(record)
	if blueprintCandidate != (writer.BlueprintAppliedPredecessor != nil) {
		return corruptTaskMaterializationWriter()
	}
	return validateTaskMaterializationWriter(writer)
}

func taskHasBlueprintCandidateAppliedAuthority(record TaskRecord) bool {
	return record.Type == taskjournal.TaskUpdate && record.Params[TaskReleasePublicationParam] != ""
}

func taskMaterializationEnvironment(record TaskRecord) (string, bool, error) {
	environmentID, declared := record.Params[TaskMaterializationEnvironmentParam]
	if !declared {
		return "", false, nil
	}
	resourceRemoval := record.Type == taskjournal.TaskRemove &&
		(recordcodec.ValidateID(ids.KindRoute, record.Target) == nil || recordcodec.ValidateID(ids.KindEnvEntry, record.Target) == nil)
	volumeMutation := record.Params[TaskResourceKindParam] == TaskResourceVolume &&
		recordcodec.ValidateID(ids.KindVolume, record.Target) == nil &&
		(record.Type == taskjournal.TaskCreate || record.Type == taskjournal.TaskUpdate || record.Type == taskjournal.TaskRemove)
	routeMutation := record.Params[TaskResourceKindParam] == TaskResourceRoute &&
		recordcodec.ValidateID(ids.KindRoute, record.Target) == nil &&
		(record.Type == taskjournal.TaskCreate || record.Type == taskjournal.TaskUpdate) &&
		record.Params[TaskRouteEnvironmentParam] == environmentID && record.Owner.EnvironmentID == environmentID
	if record.Executor != taskjournal.TaskExecutorAgent || recordcodec.ValidateID(ids.KindEnvironment, environmentID) != nil ||
		(record.Target != environmentID && !resourceRemoval && !volumeMutation && !routeMutation) {
		return "", false, errs.New(errs.KindValidationFailed, "task materialization Environment is invalid")
	}
	return environmentID, true, nil
}

func (repository *TaskRepository) prepareTaskMaterializationAcknowledgement(
	ctx context.Context,
	terminal TaskRecord,
	assignment TaskAssignmentRecord,
	readRevision int64,
) (taskMaterializationProjectionChange, error) {
	change, err := repository.prepareTaskMaterializationProjectionAcknowledgement(
		ctx, terminal, terminal.Status, readRevision,
	)
	if err != nil {
		return taskMaterializationProjectionChange{}, err
	}
	runtime, err := repository.prepareEntryRuntimeAcknowledgement(ctx, terminal, assignment, readRevision)
	if err != nil {
		clearTaskMaterializationProjectionChange(change)
		return taskMaterializationProjectionChange{}, err
	}
	change.applies = change.applies || runtime.applies
	change.conditions = append(change.conditions, runtime.conditions...)
	change.mutations = append(change.mutations, runtime.mutations...)
	configuration, err := prepareRuntimeConfigurationAcknowledgement(terminal)
	if err != nil {
		clearTaskMaterializationProjectionChange(change)
		return taskMaterializationProjectionChange{}, err
	}
	change.applies = change.applies || configuration.applies
	change.conditions = append(change.conditions, configuration.conditions...)
	change.mutations = append(change.mutations, configuration.mutations...)
	backingHook, err := repository.prepareBackingServiceCreationHookAcknowledgement(
		ctx, terminal, assignment, readRevision,
	)
	if err != nil {
		clearTaskMaterializationProjectionChange(change)
		return taskMaterializationProjectionChange{}, err
	}
	change.applies = change.applies || backingHook.applies
	change.conditions = append(change.conditions, backingHook.conditions...)
	pins, err := repository.prepareRecoverySecretPinTerminal(ctx, terminal, readRevision)
	if err != nil {
		clearTaskMaterializationProjectionChange(change)
		return taskMaterializationProjectionChange{}, err
	}
	change.applies = change.applies || pins.applies
	change.conditions = append(change.conditions, pins.conditions...)
	change.mutations = append(change.mutations, pins.mutations...)
	return change, nil
}

func (repository *TaskRepository) prepareTaskMaterializationProjectionAcknowledgement(
	ctx context.Context,
	record TaskRecord,
	terminalStatus taskjournal.TaskStatus,
	readRevision int64,
) (taskMaterializationProjectionChange, error) {
	if terminalStatus != taskjournal.TaskStatusCompleted {
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
	if recordcodec.ValidateID(ids.KindTask, revisionID) != nil || record.RenderGeneration <= 0 {
		return taskMaterializationProjectionChange{}, errs.New(
			errs.KindStateConflict,
			"task desired revision identity is invalid",
		)
	}

	rootKey := environmentBlueprintRootKey(environmentID, revisionID)
	projectionKey := projectionrecord.EnvironmentComposeProjectionStorageKey(environmentID)
	state, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
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
	// Metadata edits and Volume identity Tasks do not execute desired runtime.
	// Blueprint Apply can execute Components even without native Releases.
	volumeIdentity := record.Params[TaskResourceKindParam] == TaskResourceVolume &&
		(record.Type == taskjournal.TaskCreate || record.Type == taskjournal.TaskUpdate)
	entryMutation := record.Params[TaskResourceKindParam] == TaskResourceEntry && record.Type == taskjournal.TaskUpdate
	configuredApply := record.Params[componentTaskBlueprintProcedureParam] == componentTaskBlueprintProcedureNone
	if volumeIdentity ||
		record.Type == taskjournal.TaskUpdate && !entryMutation && !configuredApply && !taskHasBlueprintCandidateAppliedAuthority(record) &&
			state.Values[1] != nil {
		if state.Values[1] != nil {
			if _, err := projectionrecord.DecodeEnvironmentComposeProjectionStorage(state.Values[1].Value); err != nil {
				return taskMaterializationProjectionChange{}, err
			}
		}
		return taskMaterializationProjectionChange{}, nil
	}
	seal, err := decodeEnvironmentBlueprintSeal(state.Values[0].Value)
	if err != nil || seal.EnvironmentID != environmentID || seal.RevisionID != revisionID {
		return taskMaterializationProjectionChange{}, projectionrecord.CorruptEnvironmentComposeProjection()
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
	projection, err := projectionrecord.DecodeEnvironmentComposeProjectionStorage(projectionValue)
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
		current, decodeErr := projectionrecord.DecodeEnvironmentComposeProjectionStorage(state.Values[1].Value)
		if decodeErr != nil || current.EnvironmentID != environmentID {
			clear(projectionValue)
			return taskMaterializationProjectionChange{}, projectionrecord.CorruptEnvironmentComposeProjection()
		}
		if projection.RenderGeneration <= current.RenderGeneration {
			clear(projectionValue)
			return taskMaterializationProjectionChange{}, errs.New(
				errs.KindStateConflict,
				"task desired projection is not newer than the applied projection",
			)
		}
		projectionRevisionCondition = state.Values[1].ModRevision
		if entryMutation {
			// Entry execution changes materialized generations and their Compose
			// bindings, not unrelated applied workload or resource decisions. The
			// selected Services' acknowledged runtime records own their new exact
			// artifacts; this aggregate projection remains byte-identical.
			current.RevisionID, current.RenderGeneration = projection.RevisionID, projection.RenderGeneration
			current.Entries = projection.Entries
			projection = current
			clear(projectionValue)
			projectionValue, err = projectionrecord.EncodeEnvironmentComposeProjectionStorage(projection)
			if err != nil {
				return taskMaterializationProjectionChange{}, err
			}
		}
	}
	conditions := []etcdstore.Condition{
		{Key: rootKey, ModRevision: state.Values[0].ModRevision},
		{Key: projectionKey, ModRevision: projectionRevisionCondition},
	}
	if taskHasBlueprintCandidateAppliedAuthority(record) {
		artifact, authority, err := repository.blueprintAcknowledgedArtifact(ctx, record, readRevision)
		if err != nil {
			clear(projectionValue)
			return taskMaterializationProjectionChange{}, err
		}
		projection.ComposeArtifact = artifact
		clear(projectionValue)
		projectionValue, err = projectionrecord.EncodeEnvironmentComposeProjectionStorage(projection)
		if err != nil {
			return taskMaterializationProjectionChange{}, err
		}
		conditions = append(conditions, authority)
	}
	return taskMaterializationProjectionChange{
		applies:    true,
		conditions: conditions,
		mutations:  []etcdstore.Mutation{{Type: etcdstore.MutationPut, Key: projectionKey, Value: projectionValue}},
	}, nil
}

func taskHasSpecializedProjectionAcknowledgement(record TaskRecord) bool {
	return record.Type == taskjournal.TaskRemove &&
		(recordcodec.ValidateID(ids.KindRoute, record.Target) == nil ||
			recordcodec.ValidateID(ids.KindEnvEntry, record.Target) == nil)
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
	if record.Executor != taskjournal.TaskExecutorAgent || recordcodec.ValidateID(ids.KindEnvironment, mutationEnvironment) != nil ||
		(record.Type != taskjournal.TaskAttach && record.Type != taskjournal.TaskDetach) {
		return "", false, errs.New(errs.KindValidationFailed, "task mutation Environment is invalid")
	}
	return mutationEnvironment, true, nil
}

func taskMaterializationWriter(
	record TaskRecord,
	environmentID string,
	predecessor *taskMaterializationAppliedPredecessor,
) taskMaterializationWriterRecord {
	var sealedPredecessor *taskMaterializationAppliedPredecessor
	if predecessor != nil {
		sealed := *predecessor
		sealedPredecessor = &sealed
	}
	return taskMaterializationWriterRecord{
		EnvironmentID: environmentID, TaskID: record.ID, RenderGeneration: record.RenderGeneration,
		BlueprintAppliedPredecessor: sealedPredecessor,
	}
}

func encodeTaskMaterializationWriter(record taskMaterializationWriterRecord) ([]byte, error) {
	if err := validateTaskMaterializationWriter(record); err != nil {
		return nil, err
	}
	return json.Marshal(taskMaterializationWriterJSON{
		Schema: 2, EnvironmentID: record.EnvironmentID,
		TaskID: record.TaskID, RenderGeneration: record.RenderGeneration,
		BlueprintAppliedPredecessor: record.BlueprintAppliedPredecessor,
	})
}

func decodeTaskMaterializationWriter(value []byte) (taskMaterializationWriterRecord, error) {
	if recordcodec.RejectDuplicateFields(value) != nil {
		return taskMaterializationWriterRecord{}, corruptTaskMaterializationWriter()
	}
	decoder := json.NewDecoder(bytes.NewReader(value))
	decoder.DisallowUnknownFields()
	var data taskMaterializationWriterJSON
	if err := decoder.Decode(&data); err != nil || recordcodec.RequireEOF(decoder) != nil || data.Schema != 2 {
		return taskMaterializationWriterRecord{}, corruptTaskMaterializationWriter()
	}
	record := taskMaterializationWriterRecord{
		EnvironmentID: data.EnvironmentID, TaskID: data.TaskID, RenderGeneration: data.RenderGeneration,
		BlueprintAppliedPredecessor: data.BlueprintAppliedPredecessor,
	}
	if err := validateTaskMaterializationWriter(record); err != nil {
		return taskMaterializationWriterRecord{}, corruptTaskMaterializationWriter()
	}
	return record, nil
}

func validateTaskMaterializationWriter(record taskMaterializationWriterRecord) error {
	if recordcodec.ValidateID(ids.KindEnvironment, record.EnvironmentID) != nil ||
		recordcodec.ValidateID(ids.KindTask, record.TaskID) != nil || record.RenderGeneration <= 0 {
		return corruptTaskMaterializationWriter()
	}
	if record.BlueprintAppliedPredecessor == nil {
		return nil
	}
	predecessor := *record.BlueprintAppliedPredecessor
	if !predecessor.Present {
		if predecessor.KeyRevision != 0 || predecessor.RevisionID != "" || predecessor.RenderGeneration != 0 {
			return corruptTaskMaterializationWriter()
		}
		return nil
	}
	if !predecessor.Present || predecessor.KeyRevision <= 0 ||
		recordcodec.ValidateID(ids.KindTask, predecessor.RevisionID) != nil ||
		predecessor.RenderGeneration >= uint64(record.RenderGeneration) {
		return corruptTaskMaterializationWriter()
	}
	return nil
}

func corruptTaskMaterializationWriter() error {
	return errs.New(errs.KindInternal, "task materialization writer record is corrupt")
}
