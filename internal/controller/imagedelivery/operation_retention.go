package imagedelivery

import (
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/imageoperations"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func retainImageOperations(retention *imageRetention, excludingOperation string,
	read func(string, func(keyvalue.KeyValue) error) error,
) error {
	return read(imageoperations.Prefix, func(value keyvalue.KeyValue) error {
		record, err := imageoperations.Decode(value.Value)
		if err != nil {
			return err
		}
		if value.Key != imageoperations.Key(record.Input.TaskID) {
			return errs.New(errs.KindInternal, "image operation key differs from its owner")
		}
		if record.Input.OperationID != excludingOperation && record.Phase != imageoperations.Finished {
			reason := "Image operation " + record.Input.TaskID + " is unfinished"
			retention.retain(record.Input.Target(), reason)
			if record.Input.Fetch != nil {
				retention.retain(record.Input.Fetch.Reference(), reason)
			}
		}
		if record.Input.Fetch != nil {
			retention.fetches = append(retention.fetches, retainedFetch{plan: *record.Input.Fetch,
				taskID: record.Input.TaskID, requestedAt: record.Input.CreatedAt, status: imageOperationStatus(record)})
		}
		return nil
	})
}

func imageOperationStatus(record imageoperations.Record) apiTypes.TaskStatus {
	if record.Phase == imageoperations.Ready {
		return apiTypes.TaskPending
	}
	if record.Phase != imageoperations.Finished {
		return apiTypes.TaskRunning
	}
	switch record.Outcome {
	case workexecution.Succeeded:
		return apiTypes.TaskCompleted
	case workexecution.Cancelled, workexecution.Superseded:
		return apiTypes.TaskAborted
	default:
		return apiTypes.TaskFailed
	}
}
