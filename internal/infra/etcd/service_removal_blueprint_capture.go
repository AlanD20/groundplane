package etcd

import (
	"context"
	"encoding/hex"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentqueries"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/serviceremovalwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type blueprintRemovalCapture struct {
	waiting   bool
	rejection string
	input     serviceremovalwork.Input
	service   keyvalue.Versioned[services.ServiceRecord]
	guards    []keyvalue.Condition
}

func (repository *ServiceRepository) captureBlueprintServiceRemoval(
	ctx context.Context,
	view workqueue.View,
) (blueprintRemovalCapture, error) {
	task, item := view.Task.Task, view.Item.Item
	if task.Action != workexecution.ActionApply || task.Stop != nil || task.FinishedAt != nil ||
		task.CollectionStartedAt != nil ||
		!task.PublicationComplete ||
		item.Operation != workexecution.ServiceRemoval ||
		item.TaskID != task.ID ||
		item.Phase != workexecution.Waiting ||
		item.Wait == nil ||
		item.Wait.Reason != workexecution.WaitInput ||
		item.Input != nil ||
		item.ForwardRevoked ||
		item.AttemptNumber != 0 {
		return blueprintRemovalCapture{}, errs.New(
			errs.KindStateConflict,
			"Blueprint Service removal is not waiting for captured input",
		)
	}
	ledger, err := blueprintunits.NewRepository(repository.store)
	if err != nil {
		return blueprintRemovalCapture{}, err
	}
	graph, err := ledger.LoadCapturedGraph(ctx, task, view.ReadRevision)
	if err != nil {
		return blueprintRemovalCapture{}, err
	}
	matched := false
	for index, candidate := range graph.Record.Items {
		if candidate.ID != item.ID {
			continue
		}
		unit := graph.Record.Units[index]
		matched = candidate.Operation == item.Operation && candidate.TargetID == item.TargetID &&
			candidate.Placement == item.Placement &&
			slices.Equal(candidate.Claims, item.Claims) &&
			slices.Equal(candidate.Dependencies, item.Dependencies) &&
			unit.Target.Kind == ids.KindService &&
			unit.Target.ID == item.TargetID &&
			unit.Removal
	}
	if !matched {
		return blueprintRemovalCapture{}, errs.New(
			errs.KindStateConflict,
			"Service removal differs from its captured Blueprint graph",
		)
	}
	owned, found, err := blueprints.ReadOwnedIdentitiesRevision(
		ctx,
		repository.store,
		task.Scope.EnvironmentID,
		task.ID,
		view.ReadRevision,
	)
	if err != nil {
		return blueprintRemovalCapture{}, err
	}
	if !found {
		return blueprintRemovalCapture{}, errs.New(
			errs.KindStateConflict,
			"Blueprint omission identities are unavailable",
		)
	}
	sourceID := ""
	for _, service := range owned.Record.Services {
		if service.ID == item.TargetID {
			return blueprintRemovalCapture{}, errs.New(
				errs.KindStateConflict,
				"Blueprint still declares the removed Service",
			)
		}
	}
	for _, omission := range owned.Record.PendingOmissions {
		if omission.Kind == ids.KindService && omission.ID == item.TargetID {
			if sourceID != "" {
				return blueprintRemovalCapture{}, errs.New(
					errs.KindStateConflict,
					"Blueprint Service omission is duplicated",
				)
			}
			sourceID = omission.SourceRevisionID
		}
	}
	if sourceID == "" {
		return blueprintRemovalCapture{}, errs.New(
			errs.KindStateConflict,
			"Blueprint Service removal has no retained declaration",
		)
	}
	declared, found, err := environmentqueries.ReadDeclaredResourcesRevision(
		ctx,
		repository.store,
		task.Scope.EnvironmentID,
		sourceID,
		view.ReadRevision,
	)
	if err != nil {
		return blueprintRemovalCapture{}, err
	}
	if !found {
		return blueprintRemovalCapture{}, errs.New(errs.KindStateConflict, "Removed Service declaration is unavailable")
	}
	current, err := services.ReadJoined(ctx, repository.store, services.DesiredSelection{
		Services: declared.Record.DesiredServices, Revision: declared.Revision, ReadRevision: view.ReadRevision,
	}, item.TargetID, blueprints.EnvironmentBlueprintRootKey(task.Scope.EnvironmentID, sourceID))
	if err != nil {
		return blueprintRemovalCapture{}, err
	}
	rootKey := blueprints.EnvironmentBlueprintRootKey(task.Scope.EnvironmentID, task.ID)
	read, err := repository.store.GetMany(
		ctx,
		keyvalue.GetManyRequest{Keys: []string{rootKey}, Revision: view.ReadRevision},
	)
	if err != nil {
		return blueprintRemovalCapture{}, err
	}
	if read == nil {
		return blueprintRemovalCapture{}, errs.New(errs.KindStateConflict, "Blueprint removal seal is unavailable")
	}
	defer keyvalue.ClearValues(read.Values)
	if read.ReadRevision != view.ReadRevision || len(read.Values) != 1 || read.Values[0] == nil ||
		read.Values[0].Key != rootKey {
		return blueprintRemovalCapture{}, errs.New(
			errs.KindStateConflict,
			"Blueprint removal seal snapshot is incomplete",
		)
	}
	seal, err := blueprints.DecodeEnvironmentBlueprintSeal(read.Values[0].Value)
	if err != nil {
		return blueprintRemovalCapture{}, err
	}
	if seal.EnvironmentID != task.Scope.EnvironmentID || seal.RevisionID != task.ID ||
		seal.RenderGeneration != owned.Record.RenderGeneration {
		return blueprintRemovalCapture{}, errs.New(
			errs.KindStateConflict,
			"Blueprint removal seal differs from its captured identities",
		)
	}
	runtime, err := repository.GetServiceRemovalRuntime(ctx, task.Scope.EnvironmentID, item.TargetID, view.ReadRevision)
	if err != nil {
		return blueprintRemovalCapture{}, err
	}
	var source *blueprintunits.ServiceRemovalSource
	for index := range graph.Record.ServiceRemovals {
		if graph.Record.ServiceRemovals[index].WorkItemID == item.ID {
			source = &graph.Record.ServiceRemovals[index]
		}
	}
	if source == nil || source.ServiceID != item.TargetID {
		return blueprintRemovalCapture{}, errs.New(
			errs.KindStateConflict,
			"Service removal has no captured runtime source",
		)
	}
	claim, _, err := repository.GetServiceRemovalPredecessor(ctx, item.TargetID, view.ReadRevision)
	if err != nil {
		return blueprintRemovalCapture{}, err
	}
	if claim.Owner != nil {
		if source.Predecessor != nil && *claim.Owner == *source.Predecessor {
			return blueprintRemovalCapture{waiting: true}, nil
		}
		return blueprintRemovalCapture{}, errs.New(
			errs.KindStateConflict,
			"Service removal is fenced by a different operation",
		)
	}
	if source.RuntimeRevision != runtime.Revision {
		reject := func(reason string) blueprintRemovalCapture {
			return blueprintRemovalCapture{rejection: reason, guards: []keyvalue.Condition{
				{Key: blueprintunits.CapturedGraphKey(task.Scope.EnvironmentID, task.ID), ModRevision: graph.Revision},
				{Key: serviceruntimerecord.Key(item.TargetID), ModRevision: runtime.Revision},
				{Key: workqueue.ClaimKey(workexecution.Claim{ScopeID: item.TargetID})},
				{Key: serviceremovalwork.InputKey(item.ID)},
			}}
		}
		if source.Predecessor == nil || runtime.Revision == 0 {
			return reject(
				"Service runtime changed after this Apply was accepted. Apply the Blueprint again to capture the current runtime.",
			), nil
		}
		fingerprint, err := serviceruntimerecord.Fingerprint(runtime.Record)
		if err != nil {
			return blueprintRemovalCapture{}, err
		}
		fromPredecessor := runtime.Record.Source.TaskID == source.Predecessor.TaskID &&
			runtime.Record.Source.WorkItemID == source.Predecessor.WorkItemID
		if !fromPredecessor && fingerprint != source.RuntimeSHA256 {
			return reject(
				"Service runtime was changed by another operation. Apply the Blueprint again; this removal did not run.",
			), nil
		}
	}
	input := serviceremovalwork.Input{
		TaskID:                task.ID,
		WorkItemID:            item.ID,
		PlanID:                ids.DeriveAt(ids.KindPlan, task.CreatedAt, item.ID, "service-removal"),
		Scope:                 task.Scope,
		ServiceID:             item.TargetID,
		ServiceName:           current.Record.Desired.Name,
		ServiceRevision:       current.Revision,
		RuntimeRevision:       services.ServiceRuntimeRevision(current),
		RuntimeSourceRevision: runtime.Revision,
		DesiredRevisionID:     task.ID,
		DesiredSHA256:         hex.EncodeToString(seal.ProjectionSHA256[:]),
		BaseRevisionID:        sourceID,
		HeadRevision:          seal.BaselineHeadRevision,
		Generation:            seal.RenderGeneration,
		CreatedAt:             task.CreatedAt,
	}
	input.HostID = item.Placement.HostID
	if runtime.Revision != 0 {
		input.Runtime, input.HostID = &runtime.Record, runtime.Record.Source.Executor.HostID
	}
	input, guards, err := repository.captureRemovalConfiguration(ctx, input, view.ReadRevision)
	if err != nil {
		return blueprintRemovalCapture{}, err
	}
	expected, err := serviceremovalwork.NewItem(input, item.Deadline)
	if err != nil {
		return blueprintRemovalCapture{}, err
	}
	if expected.Placement != item.Placement || len(expected.Claims) != len(item.Claims) {
		return blueprintRemovalCapture{}, errs.New(
			errs.KindStateConflict,
			"Service removal host ownership changed during Blueprint preparation",
		)
	}
	for _, claim := range expected.Claims {
		if !slices.Contains(item.Claims, claim) {
			return blueprintRemovalCapture{}, errs.New(
				errs.KindStateConflict,
				"Service removal claims differ from the captured Blueprint",
			)
		}
	}
	guards = append(
		guards,
		keyvalue.Condition{Key: rootKey, ModRevision: read.Values[0].ModRevision},
		keyvalue.Condition{Key: workqueue.ClaimKey(workexecution.Claim{ScopeID: item.TargetID})},
		keyvalue.Condition{
			Key:         blueprintunits.CapturedGraphKey(task.Scope.EnvironmentID, task.ID),
			ModRevision: graph.Revision,
		},
		keyvalue.Condition{
			Key:         blueprints.EnvironmentBlueprintOwnedIdentitiesKey(task.Scope.EnvironmentID, task.ID),
			ModRevision: owned.Revision,
		},
	)
	return blueprintRemovalCapture{input: input, service: current, guards: guards}, nil
}
