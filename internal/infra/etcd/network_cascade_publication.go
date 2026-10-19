package etcd

import (
	"bytes"
	"context"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/attachremoval"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/networkwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *EnvironmentBlueprintRepository) prepareNetworkCascadePublication(ctx context.Context,
	networks *networkwork.InputRepository, request NetworkWorkPublication, admission []keyvalue.Condition,
) (workqueue.Change, error) {
	cascade, task := request.Cascade, request.Task
	if cascade == nil || cascade.SnapshotRevision <= 0 || len(cascade.Items) != len(cascade.Inputs)+1 ||
		len(cascade.Inputs) != len(cascade.Captures) || task.ExpectedItems != uint32(len(cascade.Items)) {
		return workqueue.Change{}, errs.New(
			errs.KindValidationFailed,
			"Backing Zone removal requires all captured consumer inputs",
		)
	}
	if err := workexecution.ValidateGraph(task, cascade.Items); err != nil {
		return workqueue.Change{}, err
	}
	bridge, err := workqueue.EncodeItem(cascade.Items[len(cascade.Items)-1])
	if err != nil {
		return workqueue.Change{}, err
	}
	defer clear(bridge)
	expectedBridge, err := workqueue.EncodeItem(request.Item)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer clear(expectedBridge)
	if !bytes.Equal(bridge, expectedBridge) {
		return workqueue.Change{}, errs.New(
			errs.KindValidationFailed,
			"Backing Zone graph differs from its final bridge operation",
		)
	}
	members, err := attachments.NewReader(repository.store).ListAttachesByBackingNetworkAtRevision(ctx,
		task.Scope.ProjectID, task.TargetID, cascade.SnapshotRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	if len(members) != len(cascade.Inputs) {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Backing Zone captured graph omits a consumer")
	}
	byID := make(map[string]int64, len(members))
	for _, member := range members {
		byID[member.Record.ID] = member.Revision
	}
	creation, err := attachments.BackingZoneCreationCondition(
		ctx,
		repository.store,
		task.TargetID,
		cascade.SnapshotRevision,
	)
	if err != nil {
		return workqueue.Change{}, err
	}
	admission, err = resourcePublicationConditions(admission, []keyvalue.Condition{creation})
	if err != nil {
		return workqueue.Change{}, err
	}
	inputs, err := attachremoval.NewInputRepository(repository.store)
	if err != nil {
		return workqueue.Change{}, err
	}
	removed := make(map[string][]string)
	previous := ""
	for ordinal, input := range cascade.Inputs {
		item, source := cascade.Items[ordinal], input.Snapshot.Source
		step := input.Cascade
		before := slices.Clone(removed[source.Attach.ServiceID])
		slices.Sort(before)
		if input.Validate(ctx) != nil || step == nil || step.ZoneID != task.TargetID ||
			step.Ordinal != uint32(
				ordinal,
			) || step.Count != uint32(len(cascade.Inputs)) || !slices.Equal(step.RemovedBefore, before) ||
			input.TaskID != task.ID || input.WorkItemID != item.ID || item.TargetID != source.Attach.ID ||
			source.SnapshotRevision != cascade.SnapshotRevision || byID[source.Attach.ID] != source.AttachRevision ||
			source.Attach.BackingProjectID != task.Scope.ProjectID || source.Attach.BackingEnvironmentID != task.Scope.EnvironmentID ||
			item.Placement.HostID != request.Item.Placement.HostID ||
			previous == "" && len(item.Dependencies) != 0 ||
			previous != "" && (len(item.Dependencies) != 1 || item.Dependencies[0] != previous) {
			return workqueue.Change{}, errs.New(
				errs.KindStateConflict,
				"Backing Zone consumer graph differs from its complete captured membership",
			)
		}
		delete(byID, source.Attach.ID)
		guards, err := resourcePublicationConditions(
			admission,
			input.Snapshot.ConsumerConditions(),
			[]keyvalue.Condition{
				{Key: attachments.AttachKey(source.Attach.ID), ModRevision: source.AttachRevision},
				{Key: attachments.AttachFactsKey(source.Attach.ID), ModRevision: source.FactsRevision}},
		)
		if err != nil {
			return workqueue.Change{}, err
		}
		if err := inputs.StageCascadeCapture(ctx, task, item, input, cascade.Captures[ordinal], guards); err != nil {
			return workqueue.Change{}, err
		}
		previous = item.ID
		if source.Attach.Status != "detached" {
			removed[source.Attach.ServiceID] = append(removed[source.Attach.ServiceID], source.Attach.ID)
		}
	}
	if previous != "" && (len(request.Item.Dependencies) != 1 || request.Item.Dependencies[0] != previous) ||
		previous == "" && len(request.Item.Dependencies) != 0 {
		return workqueue.Change{}, errs.New(
			errs.KindValidationFailed,
			"Backing Zone bridge must follow the last captured consumer",
		)
	}
	return networks.PrepareCascadePublication(ctx, task, cascade.Items, request.Input, request.Marker,
		admission, request.Claim.RevisionID, cascade.SnapshotRevision)
}
