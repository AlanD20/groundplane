package blueprint

import (
	"context"
	"slices"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/componentwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	zonerecord "github.com/AlanD20/groundplane/internal/infra/etcd/zones"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpins"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type preparedComponentWork struct {
	Input     *agentpb.ComponentWorkInput
	Capture   componentwork.Capture
	Secrets   tasksecretpins.Prepared
	Placement componentWorkPlacement
}

// prepareComponentWork captures a singleton operation without creating a Task,
// executable plan or host effect. Publication must still join graph/dependency
// comparisons, proxy transitions, input and this preparation's reservation writes.
func (service *Service) prepareComponentWork(ctx context.Context, work workqueue.View, services []workqueue.View,
	unit blueprintunits.Unit, snapshot blueprintunits.Snapshot,
) (result preparedComponentWork, err error) {
	placement, err := service.prepareComponentWorkPlacement(ctx, work, unit, snapshot)
	if err != nil {
		return result, err
	}
	defer func() {
		if err != nil {
			keyvalue.ClearMutationValues(placement.Change.Mutations)
		}
	}()
	rendered, err := service.renderComponentWork(ctx, work, placement)
	if err != nil {
		return result, err
	}
	files, descriptors, err := service.prepareComponentWorkFiles(ctx, work, placement, rendered)
	if err != nil {
		return result, err
	}
	configuration, err := service.captureComponentWorkConfiguration(ctx, work, placement, files)
	if err != nil {
		return result, err
	}
	action, err := service.prepareComponentWorkAction(ctx, placement, rendered, files, configuration)
	if err != nil {
		return result, err
	}
	predecessor, err := service.prepareComponentWorkPredecessor(ctx, placement, configuration)
	if err != nil {
		return result, err
	}
	operationID := ids.DeriveAt(ids.KindOperation, work.Item.Item.CreatedAt, work.Item.Item.ID, "component-operation")
	input := &agentpb.ComponentWorkInput{
		OperationId: operationID, EnvironmentId: work.Task.Task.Scope.EnvironmentID, ComponentId: work.Item.Item.TargetID,
		RenderGeneration: configuration.Generation, Remove: !placement.Candidate.Desired.Enabled,
		Candidate: rendered.Artifact, Predecessor: predecessor, ConfigurationAction: action, Files: descriptors,
	}
	if len(descriptors) != 0 {
		input.Configuration, err = componentWorkConfigurationReference(configuration.Current)
		if err != nil {
			return result, err
		}
	}
	if configuration.Prior != nil {
		input.PriorConfiguration, err = componentWorkConfigurationReference(*configuration.Prior)
		if err != nil {
			return result, err
		}
	}
	if err = executionplan.ValidateComponentWorkInput(input); err != nil {
		return result, err
	}
	capture := componentwork.Capture{
		Removal: unit.Removal,
		Current: placement.Selection.Current.Component.Record, CurrentRevision: placement.Selection.Current.Component.Revision,
		Candidate: placement.Candidate, RuntimeRevision: placement.Selection.Current.Runtime.Revision, Configuration: configuration,
	}
	if placement.Selection.Current.RuntimePresent {
		prior, captureErr := componentwork.CaptureRuntimePredecessor(placement.Selection.Current.Runtime.Record)
		if captureErr != nil {
			return result, captureErr
		}
		capture.Predecessor = &prior
	}
	for _, zone := range placement.Zones {
		capture.Zones = append(capture.Zones, zone.Record)
	}
	slices.SortFunc(capture.Zones, func(left, right zonerecord.Record) int { return strings.Compare(left.Desired.ID, right.Desired.ID) })
	if _, changed := placement.Authority.ProxyServiceCapture(); changed {
		var proxyChange workqueue.Change
		capture.Proxy, proxyChange, err = service.prepareComponentProxyRollouts(ctx, work, services,
			snapshot, placement.Selection.Source, placement.Candidate, placement.Authority)
		if err != nil {
			return result, err
		}
		placement.Change.Conditions = append(placement.Change.Conditions, proxyChange.Conditions...)
		placement.Change.Mutations = append(placement.Change.Mutations, proxyChange.Mutations...)
	}
	secrets, err := service.repository.PrepareComponentWorkSecretPins(ctx, work, operationID, configuration)
	if err != nil {
		return result, err
	}
	if !secrets.IsZero() {
		binding, bindErr := secrets.Binding()
		if bindErr != nil {
			return result, bindErr
		}
		capture.Secrets = &binding
	}
	return preparedComponentWork{Input: input, Capture: capture, Secrets: secrets, Placement: placement}, nil
}
