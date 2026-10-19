package imageoperations

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/imagefetch"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

type Inspection struct {
	Operation         Record
	Progress          imagefetch.Progress
	ProgressAvailable bool
}

// Inspect reads diagnostic progress at the captured operation revision. Missing
// progress is explicit, not fabricated from a step number or phase description.
func (repository *Repository) Inspect(ctx context.Context, taskID string, revision int64) (Inspection, error) {
	loaded, err := repository.LoadAt(ctx, taskID, revision)
	if err != nil {
		return Inspection{}, err
	}
	record := loaded.Record
	result := Inspection{Operation: record}
	if record.AttemptID == "" {
		return result, nil
	}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{
		Keys: []string{
			workqueue.AttemptKey(record.AttemptID),
			ProgressKey(record.AttemptID),
		}, Revision: loaded.ReadRevision})
	if err != nil {
		return Inspection{}, err
	}
	if read == nil {
		return Inspection{}, conflict()
	}
	defer keyvalue.ClearValues(read.Values)
	if read.ReadRevision != loaded.ReadRevision || len(read.Values) != 2 || read.Values[0] == nil {
		return Inspection{}, conflict()
	}
	attempt, err := workqueue.DecodeAttempt(read.Values[0].Value)
	if err != nil {
		return Inspection{}, err
	}
	input, err := record.Input.Reference()
	if err != nil {
		return Inspection{}, err
	}
	if attempt.ID != record.AttemptID || attempt.WorkItemID != record.Input.WorkItemID || attempt.TaskID != taskID ||
		attempt.Input != input {
		return Inspection{}, conflict()
	}
	if read.Values[1] == nil {
		return result, nil
	}
	progress, err := recordcodec.Decode[imagefetch.Progress](read.Values[1].Value, "image_progress")
	if err != nil {
		return Inspection{}, err
	}
	result.Progress, result.ProgressAvailable = progress, true
	return result, nil
}
