package etcd

import (
	"bytes"
	"context"
	"slices"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releasegroups"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const groupReleaseDraftPrefix = "/v1/execution/group-release-drafts/"

func groupReleaseDraftKey(taskID string) string       { return groupReleaseDraftPrefix + taskID + "/" }
func groupReleaseDraftHeaderKey(taskID string) string { return groupReleaseDraftKey(taskID) + "header" }
func groupReleaseDraftSealKey(taskID string) string   { return groupReleaseDraftKey(taskID) + "seal" }
func groupReleaseDraftMemberKey(taskID, workID, part string) string {
	return groupReleaseDraftKey(taskID) + "members/" + workID + "/" + part
}
func GroupReleasePreflightCaptureKey(taskID, workID string) string {
	return groupReleaseDraftKey(taskID) + "hooks/" + workID
}

type groupReleaseDraft struct {
	TaskID    string              `json:"task_id"`
	Intent    workexecution.Input `json:"intent"`
	CreatedAt time.Time           `json:"created_at"`
	Deadline  time.Time           `json:"deadline"`
}

// stageGroupRelease seals immutable admission material in bounded writes before
// accepting the visible Task. The seal compares one completed stamp per member,
// not every large leaf, keeping a 32-member Group inside both transaction limits.
// No record here is executable or an active Script/Secret reference.
func (ledger *ReleaseLedger) stageGroupRelease(
	ctx context.Context,
	capture GroupReleaseCapture,
) (keyvalue.Condition, error) {
	task, err := capture.Validate()
	if err != nil {
		return keyvalue.Condition{}, err
	}
	if !time.Now().Before(task.Deadline) {
		return keyvalue.Condition{}, errs.New(errs.KindStateConflict, "Group admission exceeded its captured deadline")
	}
	draft := groupReleaseDraft{TaskID: task.ID, Intent: task.Intent, CreatedAt: task.CreatedAt, Deadline: task.Deadline}
	header, err := recordcodec.Encode("group-release-draft", draft)
	if err != nil {
		return keyvalue.Condition{}, err
	}
	defer clear(header)
	headerGuard, err := ledger.stageGroupReleaseValue(ctx, groupReleaseDraftHeaderKey(task.ID), header,
		[]keyvalue.Condition{{Key: workqueue.TaskKey(task.ID)}, {Key: releasegroups.ExecutionCaptureKey(task.ID)}})
	if err != nil {
		return keyvalue.Condition{}, err
	}
	guards := []keyvalue.Condition{headerGuard, {Key: workqueue.TaskKey(task.ID)},
		{Key: releasegroups.ExecutionCaptureKey(task.ID)}, {Key: groupReleaseDraftSealKey(task.ID)}}
	completed := append([]keyvalue.Condition(nil), guards...)
	for index, member := range capture.Members {
		workID := capture.Root.Policy.Members[index].ForwardID
		stampKey := groupReleaseDraftMemberKey(task.ID, workID, "ready")
		leafGuards := append(slices.Clone(guards), keyvalue.Condition{Key: stampKey})
		preparation, reference, err := EncodeDirectReleasePreparation(member.Preparation)
		if err != nil {
			return keyvalue.Condition{}, err
		}
		prepared, err := ledger.stageGroupReleaseValue(
			ctx,
			groupReleaseDraftMemberKey(task.ID, workID, "preparation"),
			preparation,
			leafGuards,
		)
		clear(preparation)
		if err != nil {
			return keyvalue.Condition{}, err
		}
		rollout, err := releaserender.EncodeServiceRolloutCapture(member.Rollout)
		if err != nil {
			return keyvalue.Condition{}, err
		}
		rendered, err := ledger.stageGroupReleaseValue(
			ctx,
			groupReleaseDraftMemberKey(task.ID, workID, "rollout"),
			rollout,
			leafGuards,
		)
		clear(rollout)
		if err != nil {
			return keyvalue.Condition{}, err
		}
		stamp, err := recordcodec.Encode("group-release-member-ready", reference)
		if err != nil {
			return keyvalue.Condition{}, err
		}
		stampGuard, err := ledger.stageGroupReleaseValue(
			ctx,
			stampKey,
			stamp,
			append(slices.Clone(guards), prepared, rendered),
		)
		clear(stamp)
		if err != nil {
			return keyvalue.Condition{}, err
		}
		completed = append(completed, stampGuard)
	}
	for _, preflight := range capture.Preflight {
		value, err := recordcodec.Encode("group-release-preflight", preflight)
		if err != nil {
			return keyvalue.Condition{}, err
		}
		guard, err := ledger.stageGroupReleaseValue(
			ctx,
			GroupReleasePreflightCaptureKey(task.ID, preflight.Execution.WorkItemID),
			value,
			guards,
		)
		clear(value)
		if err != nil {
			return keyvalue.Condition{}, err
		}
		completed = append(completed, guard)
	}
	// A published seal forbids every further leaf write. Acceptance compares
	// this seal and its header; aborted-draft cleanup compares Task/root absence.
	return ledger.stageGroupReleaseValue(ctx, groupReleaseDraftSealKey(task.ID), header, completed)
}

func (ledger *ReleaseLedger) stageGroupReleaseValue(ctx context.Context, key string, value []byte,
	guards []keyvalue.Condition,
) (keyvalue.Condition, error) {
	if len(value) == 0 || len(value) > keyvalue.MaximumBytes || len(guards) == 0 {
		return keyvalue.Condition{}, errs.New(
			errs.KindReleasePlanTooLarge,
			"Group admission record exceeds its bounded storage format",
		)
	}
	read, err := ledger.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}})
	if err != nil {
		return keyvalue.Condition{}, err
	}
	if read == nil || len(read.Values) != 1 {
		return keyvalue.Condition{}, errs.New(errs.KindStateConflict, "Group admission staging read is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	if saved := read.Values[0]; saved != nil {
		if saved.Key != key || saved.ModRevision <= 0 || !bytes.Equal(saved.Value, value) {
			return keyvalue.Condition{}, errs.New(
				errs.KindStateConflict,
				"Group admission identity already holds different immutable input",
			)
		}
		return keyvalue.Condition{Key: key, ModRevision: saved.ModRevision}, nil
	}
	conditions, err := resourcePublicationConditions(guards, []keyvalue.Condition{{Key: key}})
	if err != nil {
		return keyvalue.Condition{}, err
	}
	result, err := ledger.store.Transact(
		ctx,
		conditions,
		[]keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: key, Value: value}},
	)
	keyvalue.ClearValues(result.FailureReads)
	if err != nil {
		return keyvalue.Condition{}, err
	}
	if !result.Succeeded {
		return keyvalue.Condition{}, errs.New(
			errs.KindStateConflict,
			"Group admission draft changed before immutable staging",
		)
	}
	return keyvalue.Condition{Key: key, ModRevision: result.Revision}, nil
}
