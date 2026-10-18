package etcd

import (
	"context"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releasegroups"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func GroupReleasePreparationKey(workID string) string {
	return "/v1/execution/group-release-preparations/" + workID
}

// Group members use the same typed Service preparation as standalone actions.
// Their immutable reference comes from the native Group root, never from a new
// hidden Task or by pretending the Group's Task intent is one member's input.
func (ledger *ReleaseLedger) readGroupReleasePreparation(ctx context.Context, work workqueue.View) (DirectReleasePreparation, keyvalue.Condition, error) {
	groups, err := releasegroups.NewExecutionRepository(ledger.store)
	if err != nil {
		return DirectReleasePreparation{}, keyvalue.Condition{}, err
	}
	root, err := groups.Load(ctx, work.Task, work.ReadRevision)
	if err != nil {
		return DirectReleasePreparation{}, keyvalue.Condition{}, err
	}
	index := slices.IndexFunc(root.Record.Policy.Members, func(member domain.GroupWorkMember) bool { return member.ForwardID == work.Item.Item.ID })
	if index < 0 || work.Item.Item.Operation != workexecution.Release {
		return DirectReleasePreparation{}, keyvalue.Condition{}, errs.New(errs.KindStateConflict, "Native Group does not own this forward Release")
	}
	key := GroupReleasePreparationKey(work.Item.Item.ID)
	read, err := ledger.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: work.ReadRevision})
	if err != nil {
		return DirectReleasePreparation{}, keyvalue.Condition{}, err
	}
	if read == nil || read.ReadRevision != work.ReadRevision || len(read.Values) != 1 {
		return DirectReleasePreparation{}, keyvalue.Condition{}, errs.New(errs.KindStateConflict, "Group Release preparation read is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	value := read.Values[0]
	if value == nil || value.Key != key || value.ModRevision <= 0 || value.ModRevision > work.ReadRevision {
		return DirectReleasePreparation{}, keyvalue.Condition{}, errs.New(errs.KindStateConflict, "Group Release has no accepted member preparation")
	}
	request, err := DecodeDirectReleasePreparation(value.Value, root.Record.Members[index].Forward)
	if err != nil {
		return DirectReleasePreparation{}, keyvalue.Condition{}, err
	}
	member := root.Record.Policy.Members[index]
	position := slices.IndexFunc(root.Record.Graph, func(item workexecution.Item) bool { return item.ID == member.ForwardID })
	if position < 0 {
		return DirectReleasePreparation{}, keyvalue.Condition{}, errs.New(errs.KindStateConflict, "Group Release has no captured work descriptor")
	}
	expected, actual := root.Record.Graph[position], work.Item.Item
	if request.GroupID != work.Task.Task.TargetID || request.Configuration.TaskID != work.Task.Task.ID ||
		request.Configuration.WorkItemID != member.ForwardID || request.Intent.ServiceID != member.Member.ServiceID ||
		request.Intent.ID != member.Member.ReleaseID || request.Intent.GroupMemberOrdinal != member.Member.Ordinal ||
		request.Intent.GroupOperationID != root.Record.Policy.Manifest.OperationID || request.Source.ReadRevision != root.Record.SourceReadRevision ||
		request.Intent.OnFailure != root.Record.Policy.Manifest.FailurePolicy || request.Source.ReadRevision > value.ModRevision ||
		actual.TaskID != expected.TaskID || actual.TargetID != expected.TargetID || actual.Placement != expected.Placement ||
		!actual.CreatedAt.Equal(expected.CreatedAt) || !actual.Deadline.Equal(expected.Deadline) ||
		!slices.Equal(actual.Claims, expected.Claims) || !slices.Equal(actual.Dependencies, expected.Dependencies) {
		return DirectReleasePreparation{}, keyvalue.Condition{}, errs.New(errs.KindStateConflict, "Group Release preparation differs from its accepted membership")
	}
	return request, keyvalue.Condition{Key: key, ModRevision: value.ModRevision}, nil
}
