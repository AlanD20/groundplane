package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releasegroups"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PrepareGroupReleaseMemberPublication moves one sealed member into its native
// input owner. The publication barrier is still closed. Its returned change must
// advance the owning Task revision, fencing final graph publication and cleanup.
func (ledger *ReleaseLedger) PrepareGroupReleaseMemberPublication(ctx context.Context, task workqueue.TaskSnapshot,
	root keyvalue.Versioned[releasegroups.ExecutionCapture], index int, revision int64,
) (bool, workqueue.Change, error) {
	if task.Revision <= 0 || task.Task.PublicationComplete || root.Revision <= 0 || root.ReadRevision != revision ||
		root.Record.Policy.TaskID != task.Task.ID || index < 0 || index >= len(root.Record.Policy.Members) {
		return false, workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"Group member publication requires its open captured graph",
		)
	}
	accepted, err := root.Record.Task()
	if err != nil || accepted.Intent != task.Task.Intent || accepted.ID != task.Task.ID {
		return false, workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"Group member publication differs from its accepted Task",
		)
	}
	member := root.Record.Policy.Members[index]
	preparationKey := GroupReleasePreparationKey(member.ForwardID)
	rolloutKey := releaserender.ServiceRolloutKey(task.Task.Scope.EnvironmentID, task.Task.ID, member.ForwardID)
	stagedPreparation := groupReleaseDraftMemberKey(task.Task.ID, member.ForwardID, "preparation")
	stagedRollout := groupReleaseDraftMemberKey(task.Task.ID, member.ForwardID, "rollout")
	keys := []string{preparationKey, rolloutKey, stagedPreparation, stagedRollout}
	read, err := ledger.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return false, workqueue.Change{}, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != len(keys) {
		return false, workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"Group member publication read is incomplete",
		)
	}
	defer keyvalue.ClearValues(read.Values)
	for index, value := range read.Values {
		if value != nil && (value.Key != keys[index] || value.ModRevision <= 0 || value.ModRevision > revision) {
			return false, workqueue.Change{}, errs.New(
				errs.KindStateConflict,
				"Group member publication has an invalid record revision",
			)
		}
	}
	preparation, rollout := read.Values[0], read.Values[1]
	ready := preparation != nil && rollout != nil
	if ready {
		if read.Values[2] != nil || read.Values[3] != nil {
			return false, workqueue.Change{}, errs.New(
				errs.KindStateConflict,
				"Group member has both staged and published ownership",
			)
		}
	} else {
		if preparation != nil || rollout != nil || read.Values[2] == nil || read.Values[3] == nil {
			return false, workqueue.Change{}, errs.New(errs.KindStateConflict, "Group member has incomplete staged or native input")
		}
		preparation, rollout = read.Values[2], read.Values[3]
	}
	request, err := DecodeDirectReleasePreparation(preparation.Value, root.Record.Members[index].Forward)
	if err != nil {
		return false, workqueue.Change{}, err
	}
	capture, err := releaserender.DecodeServiceRolloutCapture(rollout.Value)
	if err != nil {
		return false, workqueue.Change{}, err
	}
	defer clear(capture.CandidateArtifact)
	var descriptor workexecution.Item
	for _, item := range root.Record.Graph {
		if item.ID == member.ForwardID {
			descriptor = item
			break
		}
	}
	if request.Intent.ID != member.Member.ReleaseID || request.Intent.GroupMemberOrdinal != member.Member.Ordinal ||
		request.Intent.GroupOperationID != root.Record.Policy.Manifest.OperationID || request.GroupID != task.Task.TargetID {
		return false, workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"Group member differs from its sealed admission reference",
		)
	}
	if err := request.Matches(task.Task, descriptor, capture); err != nil {
		return false, workqueue.Change{}, err
	}
	if ready {
		return true, workqueue.Change{}, nil
	}
	return false, workqueue.Change{Conditions: []keyvalue.Condition{
		{Key: workqueue.TaskKey(task.Task.ID), ModRevision: task.Revision},
		{Key: releasegroups.ExecutionCaptureKey(task.Task.ID), ModRevision: root.Revision},
		{Key: preparationKey}, {Key: rolloutKey},
		{
			Key:         stagedPreparation,
			ModRevision: preparation.ModRevision,
		}, {Key: stagedRollout, ModRevision: rollout.ModRevision},
	}, Mutations: []keyvalue.Mutation{
		{Type: keyvalue.MutationPut, Key: preparationKey, Value: append([]byte(nil), preparation.Value...)},
		{Type: keyvalue.MutationPut, Key: rolloutKey, Value: append([]byte(nil), rollout.Value...)},
		{
			Type:   keyvalue.MutationDelete,
			Key:    groupReleaseDraftMemberKey(task.Task.ID, member.ForwardID, ""),
			Prefix: true,
		},
	}}, nil
}
