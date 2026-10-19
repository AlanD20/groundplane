package etcd

import (
	"context"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/serviceremovalwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// A never-deployed omission records positive declaration retirement, not an
// invented Agent receipt. The caller joins this with runtime absence, reference
// cleanup and native work settlement in the same transaction.
func (repository *ServiceRepository) prepareServiceDeclarationReceipt(
	ctx context.Context,
	ledger *blueprintunits.Repository,
	snapshot blueprintunits.Snapshot,
	view workqueue.View,
	input serviceremovalwork.Input,
) (workqueue.Change, error) {
	graph, err := ledger.LoadCapturedGraph(ctx, view.Task.Task, view.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	var unit *blueprintunits.Unit
	for index, captured := range graph.Record.Items {
		item := view.Item.Item
		if captured.ID != item.ID {
			continue
		}
		if captured.Operation != item.Operation || captured.TargetID != item.TargetID ||
			captured.Placement != item.Placement ||
			!slices.Equal(captured.Claims, item.Claims) ||
			!slices.Equal(captured.Dependencies, item.Dependencies) {
			return workqueue.Change{}, errs.New(
				errs.KindStateConflict,
				"Service declaration removal differs from its captured work",
			)
		}
		unit = &graph.Record.Units[index]
	}
	if unit == nil {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Service declaration removal has no captured unit")
	}
	owned, found, err := blueprints.ReadOwnedIdentitiesRevision(ctx, repository.store,
		input.Scope.EnvironmentID, input.BaseRevisionID, view.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	if !found {
		return workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"Removed Service declaration identities are unavailable",
		)
	}
	birthID := ""
	for _, service := range owned.Record.Services {
		if service.ID == input.ServiceID {
			birthID = service.BirthRevisionID
		}
	}
	operationID := ids.DeriveAt(
		ids.KindOperation,
		view.Task.Task.CreatedAt,
		view.Item.Item.ID,
		"service-declaration-removal",
	)
	receipt, eligible, err := blueprintunits.TryPrepareControllerServiceDeclarationRemoval(snapshot, *unit,
		operationID, input.BaseRevisionID, input.ServiceRevision, birthID)
	if err != nil {
		return workqueue.Change{}, err
	}
	if !eligible {
		return workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"Service declaration has runtime effects that require Agent removal",
		)
	}
	return workqueue.Change{
		Conditions: append(
			receipt.Conditions(),
			keyvalue.Condition{
				Key:         blueprintunits.CapturedGraphKey(input.Scope.EnvironmentID, input.TaskID),
				ModRevision: graph.Revision,
			},
			keyvalue.Condition{
				Key: blueprints.EnvironmentBlueprintOwnedIdentitiesKey(
					input.Scope.EnvironmentID,
					input.BaseRevisionID,
				),
				ModRevision: owned.Revision,
			},
		),
		Mutations: receipt.Mutations(),
	}, nil
}
