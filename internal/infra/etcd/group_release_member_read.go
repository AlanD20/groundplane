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

// ReadGroupPublicationMember reads already promoted immutable source material
// while native membership publication is still open. It neither fabricates a
// stored item revision nor grants preparation/assignment of unpublished work.
func (ledger *ReleaseLedger) ReadGroupPublicationMember(ctx context.Context, task workqueue.TaskSnapshot,
	root keyvalue.Versioned[releasegroups.ExecutionCapture], index int, revision int64,
) (GroupReleaseMemberCapture, error) {
	if task.Revision <= 0 || task.Task.PublicationComplete || root.Revision <= 0 || root.ReadRevision != revision ||
		root.Record.Policy.TaskID != task.Task.ID || index < 0 || index >= len(root.Record.Policy.Members) {
		return GroupReleaseMemberCapture{}, errs.New(errs.KindStateConflict, "Group publication source requires its open captured graph")
	}
	accepted, err := root.Record.Task()
	if err != nil || accepted.Intent != task.Task.Intent || accepted.ID != task.Task.ID {
		return GroupReleaseMemberCapture{}, errs.New(errs.KindStateConflict, "Group publication source differs from its accepted Task")
	}
	member := root.Record.Policy.Members[index]
	keys := []string{GroupReleasePreparationKey(member.ForwardID), releaserender.ServiceRolloutKey(task.Task.Scope.EnvironmentID, task.Task.ID, member.ForwardID)}
	read, err := ledger.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return GroupReleaseMemberCapture{}, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 2 {
		return GroupReleaseMemberCapture{}, errs.New(errs.KindStateConflict, "Group publication member read is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	for index, value := range read.Values {
		if value == nil || value.Key != keys[index] || value.ModRevision <= 0 || value.ModRevision > revision {
			return GroupReleaseMemberCapture{}, errs.New(errs.KindStateConflict, "Group publication member has not been promoted")
		}
	}
	request, err := DecodeDirectReleasePreparation(read.Values[0].Value, root.Record.Members[index].Forward)
	if err != nil {
		return GroupReleaseMemberCapture{}, err
	}
	rollout, err := releaserender.DecodeServiceRolloutCapture(read.Values[1].Value)
	if err != nil {
		return GroupReleaseMemberCapture{}, err
	}
	var descriptor workexecution.Item
	for _, item := range root.Record.Graph {
		if item.ID == member.ForwardID {
			descriptor = item
			break
		}
	}
	if err := request.Matches(task.Task, descriptor, rollout); err != nil {
		clear(rollout.CandidateArtifact)
		return GroupReleaseMemberCapture{}, err
	}
	return GroupReleaseMemberCapture{Preparation: request, Rollout: rollout}, nil
}
