package imageoperations

import (
	"context"
	"crypto/sha256"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// TagSelection is the latest accepted request for a host-local tag. Input remains
// immutable even if a later request selects different content for that tag.
type TagSelection struct {
	TaskID     string              `json:"task_id"`
	WorkItemID string              `json:"work_item_id"`
	Input      workexecution.Input `json:"input"`
	ImageID    string              `json:"image_id"`
}

func tagKeys(input Input) (string, string, error) {
	if err := input.Validate(); err != nil {
		return "", "", err
	}
	if input.Fetch == nil {
		return "", "", nil
	}
	tag, err := input.Fetch.TagReference()
	if err != nil || tag == "" {
		return "", "", err
	}
	digest := sha256.Sum256([]byte(string(input.Placement.Executor) + "/" + input.Placement.HostID + "/" + tag))
	suffix := hex.EncodeToString(digest[:])
	return "/v1/images/tag-selections/" + suffix, "/v1/images/tag-writes/" + suffix, nil
}

// PrepareCapturedInput stores typed image authority only; the accepted Task and
// its work graph are promoted atomically by the shared preparation coordinator.
func (repository *Repository) PrepareCapturedInput(
	ctx context.Context,
	input Input,
	revision int64,
) (workexecution.Task, workexecution.Item, workqueue.Change, error) {
	record, task, item, err := NewTask(input)
	if err != nil {
		return workexecution.Task{}, workexecution.Item{}, workqueue.Change{}, err
	}
	_, writeKey, err := tagKeys(input)
	if err != nil {
		return workexecution.Task{}, workexecution.Item{}, workqueue.Change{}, err
	}
	if writeKey != "" {
		read, err := repository.store.GetMany(
			ctx,
			keyvalue.GetManyRequest{Keys: []string{writeKey}, Revision: revision},
		)
		if err != nil {
			return workexecution.Task{}, workexecution.Item{}, workqueue.Change{}, err
		}
		if read == nil || read.ReadRevision != revision || len(read.Values) != 1 {
			return workexecution.Task{}, workexecution.Item{}, workqueue.Change{}, errs.New(
				errs.KindInternal,
				"image tag ownership read is incomplete",
			)
		}
		defer keyvalue.ClearValues(read.Values)
		if read.Values[0] != nil {
			owner, err := recordcodec.Decode[TagSelection](read.Values[0].Value, "image_tag_write")
			if err != nil {
				return workexecution.Task{}, workexecution.Item{}, workqueue.Change{}, err
			}
			return workexecution.Task{}, workexecution.Item{}, workqueue.Change{}, errs.Newf(
				errs.KindResourceInUse,
				"Task %s has an unresolved write to this image tag; inspect that Task before fetching the tag again",
				owner.TaskID,
			)
		}
	}
	change, err := prepareCapture(record, task, item)
	return task, item, change, err
}

func prepareCapture(record Record, task workexecution.Task, item workexecution.Item) (workqueue.Change, error) {
	input := record.Input
	change := workqueue.Change{}
	value, err := Encode(record)
	if err != nil {
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, keyvalue.Condition{Key: Key(task.ID)})
	change.Mutations = append(
		change.Mutations,
		keyvalue.Mutation{Type: keyvalue.MutationPut, Key: Key(task.ID), Value: value},
	)
	selectionKey, writeKey, err := tagKeys(input)
	if err != nil {
		return workqueue.Change{}, err
	}
	if selectionKey != "" {
		selection, err := recordcodec.Encode("image_tag_selection", TagSelection{
			TaskID: task.ID, WorkItemID: item.ID, Input: task.Intent, ImageID: input.Fetch.ConfigDigest})
		if err != nil {
			return workqueue.Change{}, err
		}
		change.Conditions = append(change.Conditions, keyvalue.Condition{Key: writeKey})
		change.Mutations = append(
			change.Mutations,
			keyvalue.Mutation{Type: keyvalue.MutationPut, Key: selectionKey, Value: selection},
		)
	}
	return change, nil
}
