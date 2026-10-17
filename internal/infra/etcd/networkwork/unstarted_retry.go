package networkwork

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/networkexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func unstartedEvidencePrefix(workID string) string {
	return "/v1/network-unstarted-evidence/" + workID + "/"
}

func (repository *InputRepository) PrepareUnstartedRetry(ctx context.Context, view workqueue.View, at time.Time) (workqueue.Change, error) {
	task, item := view.Task.Task, view.Item.Item
	if item.Operation != workexecution.Network || task.Stop != nil || task.CollectionStartedAt != nil ||
		item.AttemptNumber != 0 || item.Input == nil || item.Result == nil || item.Phase != workexecution.Settled ||
		item.Outcome != workexecution.Failed || item.Cleanup != workexecution.CleanupNone || item.ForwardRevoked {
		return workqueue.Change{}, errs.New(errs.KindTaskNotRetryable, "Network Retry requires a captured action that failed before execution")
	}
	input, guards, err := repository.Load(ctx, view)
	if err != nil {
		return workqueue.Change{}, err
	}
	desired, err := repository.guardRetryDesired(ctx, view)
	if err != nil {
		return workqueue.Change{}, err
	}
	guards = append(guards, desired...)
	budget, err := networkexecution.ExecutionBudget(input.Action)
	if err != nil {
		return workqueue.Change{}, err
	}
	key := UnstartedKey(item.ID)
	read, err := repository.read(ctx, []string{key}, view.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil {
		return workqueue.Change{}, conflict("Network Retry is missing the saved queued failure")
	}
	raw := read.Values[0].Value
	digest := sha256.Sum256(raw)
	if item.Result.ID != ids.DeriveAt(ids.KindPlan, item.CreatedAt, item.ID, "network-unstarted") || item.Result.SHA256 != hex.EncodeToString(digest[:]) {
		return workqueue.Change{}, conflict("Network Retry queued failure differs from its recorded result")
	}
	failure, err := recordcodec.Decode[UnstartedResult](raw, "network-unstarted")
	if err != nil || failure.TaskID != task.ID || failure.WorkItemID != item.ID || failure.NetworkID != input.NetworkId ||
		failure.Input != *item.Input || failure.Outcome != workexecution.Failed || !failure.At.Equal(item.UpdatedAt) {
		return workqueue.Change{}, conflict("Network Retry queued failure belongs to different work")
	}
	change, err := workqueue.PrepareUnstartedRetry(view.Task, view.Item, at.Add(budget), at)
	if err != nil {
		return workqueue.Change{}, err
	}
	evidenceKey := unstartedEvidencePrefix(item.ID) + item.Result.SHA256
	fence, err := repository.prepareRemovalRetryFence(ctx, view)
	if err != nil {
		keyvalue.ZeroMutationBytes(change.Mutations)
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, fence.Conditions...)
	change.Mutations = append(change.Mutations, fence.Mutations...)
	change.Conditions = append(change.Conditions, guards...)
	change.Conditions = append(change.Conditions, keyvalue.Condition{Key: key, ModRevision: read.Values[0].ModRevision}, keyvalue.Condition{Key: evidenceKey})
	change.Mutations = append(change.Mutations,
		keyvalue.Mutation{Type: keyvalue.MutationPut, Key: evidenceKey, Value: append([]byte(nil), raw...)},
		keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: key})
	return change, nil
}
