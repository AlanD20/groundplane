package etcd

import (
	"bytes"
	"encoding/json"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type taskMaterializationWriterRecord struct {
	EnvironmentID    string
	TaskID           string
	RenderGeneration int32
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
	if record.Executor != TaskExecutorAgent || validateStableID(ids.KindEnvironment, environmentID) != nil ||
		record.Target != environmentID {
		return "", false, errs.New(errs.KindValidationFailed, "task materialization Environment is invalid")
	}
	return environmentID, true, nil
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
