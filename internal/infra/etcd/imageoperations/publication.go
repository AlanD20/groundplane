package imageoperations

import (
	"crypto/sha256"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
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

// PreparePublication must commit with the idempotency marker. A pending tag
// write blocks a newer selection until its exact external effect is resolved.
func PreparePublication(input Input, marker idempotency.IdempotencyMarker) (workqueue.Change, error) {
	record, task, item, err := NewTask(input)
	if err != nil {
		return workqueue.Change{}, err
	}
	route, method := "/images/fetch", "POST"
	if input.Action == workexecution.ActionRemove {
		route, method = "/images/{id}", "DELETE"
	}
	if marker.TaskID != task.ID || marker.Locator.ScopeKind != idempotency.IdempotencyScopePlatform ||
		marker.Locator.ScopeID != "-" || marker.Locator.Method != method || marker.Locator.Route != route ||
		marker.Kind != idempotency.IdempotencyMarkerTask || marker.State != idempotency.IdempotencyMarkerPending ||
		!marker.CreatedAt.Equal(task.CreatedAt) || !marker.UpdatedAt.Equal(task.CreatedAt) ||
		idempotency.ValidateIdempotencyMarker(marker) != nil {
		return workqueue.Change{}, invalid()
	}
	change, err := workqueue.PrepareSingleTask(task, item, marker)
	if err != nil {
		return workqueue.Change{}, err
	}
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
