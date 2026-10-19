package backupwork

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// Progress is execution state, not diagnostic history. Source cursors survive
// attempt replacement and are removed only with the settled work's retention.
type Progress struct {
	Schema     uint32              `json:"schema"`
	TaskID     string              `json:"task_id"`
	WorkItemID string              `json:"work_item_id"`
	Input      workexecution.Input `json:"input"`
	Sources    []SourceProgress    `json:"sources"`
}

// The commit revision is read from the immutable receipt. It cannot be known
// while encoding the transaction that writes both receipt and cursor.
type SourceProgress struct {
	Resume           []byte `json:"resume"`
	CheckpointSHA256 string `json:"checkpoint_sha256,omitempty"`
}

func ProgressKey(workID string) string { return "/v1/runtime/backup-work-progress/" + workID }

func prepareInitialProgress(item workexecution.Item, input *agentpb.BackupWorkInput) (workqueue.Change, error) {
	if item.Input == nil || input == nil || operation(input.Operation) != item.Operation {
		return workqueue.Change{}, conflict("Initial Backup progress requires matching captured input")
	}
	sources, err := executionplan.InitialBackupSourceResumes(input)
	if err != nil {
		return workqueue.Change{}, err
	}
	record := Progress{Schema: 1, TaskID: item.TaskID, WorkItemID: item.ID, Input: *item.Input,
		Sources: make([]SourceProgress, len(sources))}
	for index, source := range sources {
		encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(source)
		if err != nil {
			return workqueue.Change{}, errs.Wrap(errs.KindInternal, err)
		}
		record.Sources[index] = SourceProgress{Resume: encoded}
	}
	value, err := recordcodec.Encode("backup-work-progress", record)
	if err != nil {
		return workqueue.Change{}, err
	}
	key := ProgressKey(item.ID)
	return workqueue.Change{Conditions: []keyvalue.Condition{{Key: key}},
		Mutations: []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: key, Value: value}}}, nil
}

func (repository *InputRepository) LoadProgress(ctx context.Context, view workqueue.View,
	input *agentpb.BackupWorkInput,
) ([]*agentpb.BackupStepResume, keyvalue.Condition, error) {
	item := view.Item.Item
	if view.ReadRevision <= 0 || item.Validate() != nil || item.Input == nil ||
		executionplan.ValidateBackupWorkInput(input) != nil {
		return nil, keyvalue.Condition{}, conflict("Backup progress requires accepted captured input")
	}
	_, encodedInput, reference, err := executionplan.CaptureBackupWorkInput(item.Input.ID, input)
	clear(encodedInput)
	if err != nil {
		return nil, keyvalue.Condition{}, err
	}
	if reference != *item.Input {
		return nil, keyvalue.Condition{}, conflict("Backup progress input digest changed")
	}
	key := ProgressKey(item.ID)
	read, err := repository.store.GetMany(
		ctx,
		keyvalue.GetManyRequest{Keys: []string{key}, Revision: view.ReadRevision},
	)
	if err != nil {
		return nil, keyvalue.Condition{}, err
	}
	if read == nil {
		return nil, keyvalue.Condition{}, conflict("Backup progress read is unavailable")
	}
	defer keyvalue.ClearValues(read.Values)
	if read.ReadRevision != view.ReadRevision || len(read.Values) != 1 || read.Values[0] == nil ||
		read.Values[0].Key != key || read.Values[0].ModRevision <= 0 || read.Values[0].ModRevision > view.ReadRevision ||
		len(read.Values[0].Value) > executionplan.MaximumPlanBytes {
		return nil, keyvalue.Condition{}, conflict("Backup progress is missing or invalid; refusing to restart sources")
	}
	record, err := recordcodec.Decode[Progress](read.Values[0].Value, "backup-work-progress")
	if err != nil {
		return nil, keyvalue.Condition{}, err
	}
	if record.Schema != 1 || record.TaskID != item.TaskID || record.WorkItemID != item.ID ||
		record.Input != *item.Input || len(record.Sources) != len(input.Steps) {
		return nil, keyvalue.Condition{}, conflict("Backup progress belongs to different captured work")
	}
	sources := make([]*agentpb.BackupStepResume, len(record.Sources))
	for index, stored := range record.Sources {
		sources[index] = &agentpb.BackupStepResume{}
		if err := proto.Unmarshal(stored.Resume, sources[index]); err != nil {
			return nil, keyvalue.Condition{}, conflict("Backup source progress encoding is invalid")
		}
	}
	if err := repository.restoreProgressFences(ctx, view, input, record.Sources, sources, read.Values[0].ModRevision); err != nil {
		return nil, keyvalue.Condition{}, err
	}
	if err := executionplan.ValidateBackupSourceResumes(sources, input.Steps); err != nil {
		return nil, keyvalue.Condition{}, err
	}
	return sources, keyvalue.Condition{Key: key, ModRevision: read.Values[0].ModRevision}, nil
}
