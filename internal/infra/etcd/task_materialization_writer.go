package etcd

import (
	"bytes"
	"encoding/json"

	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	releaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
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

func taskMaterializationAppliedPredecessorFromValue(
	value *etcdstore.KeyValue,
	environmentID string,
	task TaskRecord,
) (taskMaterializationAppliedPredecessor, error) {
	if task.RenderGeneration <= 0 {
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
		!taskAcceptsAppliedGeneration(task, projection.RevisionID, projection.RenderGeneration) {
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

// Independent children share one authored generation. A completed sibling is
// an admissible source only within that exact revision; equal generations from
// another Apply and all newer applied state remain fenced out.
func taskAcceptsAppliedGeneration(task TaskRecord, revisionID string, generation uint64) bool {
	return generation < uint64(task.RenderGeneration) ||
		generation == uint64(task.RenderGeneration) && taskjournal.IsBlueprintChild(task.Params) &&
			revisionID == task.Params[blueprints.EnvironmentDesiredRevisionParam]
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
	if predecessor := writer.BlueprintAppliedPredecessor; predecessor != nil && predecessor.Present &&
		!taskAcceptsAppliedGeneration(record, predecessor.RevisionID, predecessor.RenderGeneration) {
		return corruptTaskMaterializationWriter()
	}
	return validateTaskMaterializationWriter(writer)
}

func taskHasBlueprintCandidateAppliedAuthority(record TaskRecord) bool {
	return record.Type == taskjournal.TaskUpdate && record.Params[releaserender.TaskReleasePublicationParam] != ""
}

func taskMaterializationEnvironment(record TaskRecord) (string, bool, error) {
	environmentID, declared := record.Params[taskjournal.TaskMaterializationEnvironmentParam]
	if !declared {
		return "", false, nil
	}
	resourceRemoval := record.Type == taskjournal.TaskRemove &&
		(recordcodec.ValidateID(ids.KindRoute, record.Target) == nil || recordcodec.ValidateID(ids.KindEnvEntry, record.Target) == nil)
	volumeMutation := record.Params[taskjournal.TaskResourceKindParam] == taskjournal.TaskResourceVolume &&
		recordcodec.ValidateID(ids.KindVolume, record.Target) == nil &&
		(record.Type == taskjournal.TaskCreate || record.Type == taskjournal.TaskUpdate || record.Type == taskjournal.TaskRemove)
	routeMutation := record.Params[taskjournal.TaskResourceKindParam] == taskjournal.TaskResourceRoute &&
		recordcodec.ValidateID(ids.KindRoute, record.Target) == nil &&
		(record.Type == taskjournal.TaskCreate || record.Type == taskjournal.TaskUpdate) &&
		record.Params[taskjournal.TaskRouteEnvironmentParam] == environmentID && record.Owner.EnvironmentID == environmentID
	if record.Executor != taskjournal.TaskExecutorAgent ||
		recordcodec.ValidateID(ids.KindEnvironment, environmentID) != nil ||
		(record.Target != environmentID && !resourceRemoval && !volumeMutation && !routeMutation) {
		return "", false, errs.New(errs.KindValidationFailed, "task materialization Environment is invalid")
	}
	return environmentID, true, nil
}

func clearTaskMaterializationProjectionChange(change taskMaterializationProjectionChange) {
	etcdstore.ClearMutationValues(change.mutations)
}

func taskEnvironmentWriter(record TaskRecord) (string, bool, error) {
	if taskjournal.IsReleaseNetworkChild(record.Params) {
		if err := taskjournal.ValidateReleaseNetworkChild(record.ID, record.Actor, record.Executor, record.Type, record.Params); err != nil {
			return "", false, err
		}
		if record.Target != record.Owner.EnvironmentID ||
			recordcodec.ValidateID(ids.KindEnvironment, record.Target) != nil ||
			len(record.Materializations) != 0 ||
			record.Configuration != nil {
			return "", false, errs.New(errs.KindValidationFailed, "release network writer ownership is invalid")
		}
		return record.Owner.EnvironmentID, true, nil
	}
	materializationEnvironment, materializes, err := taskMaterializationEnvironment(record)
	if err != nil {
		return "", false, err
	}
	mutationEnvironment, mutates := record.Params[taskjournal.TaskMutationEnvironmentParam]
	if materializes && mutates {
		return "", false, errs.New(errs.KindValidationFailed, "task declares multiple Environment writers")
	}
	if materializes {
		return materializationEnvironment, true, nil
	}
	if !mutates {
		return "", false, nil
	}
	if record.Executor != taskjournal.TaskExecutorAgent ||
		recordcodec.ValidateID(ids.KindEnvironment, mutationEnvironment) != nil ||
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
		predecessor.RenderGeneration > uint64(record.RenderGeneration) {
		return corruptTaskMaterializationWriter()
	}
	return nil
}

func corruptTaskMaterializationWriter() error {
	return errs.New(errs.KindInternal, "task materialization writer record is corrupt")
}
