package etcd

import (
	"slices"

	"github.com/AlanD20/groundplane/internal/common/edgeconnectioncheck"
	"github.com/AlanD20/groundplane/internal/common/serviceobservation"
	"github.com/AlanD20/groundplane/internal/common/workloadimage"
	"github.com/AlanD20/groundplane/internal/infra/etcd/edgechecks"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func (source EdgeCheckSource) ObservationTargets() []*agentpb.ServiceObservationTarget {
	if source.target == nil {
		return nil
	}
	targets := []*agentpb.ServiceObservationTarget{proto.CloneOf(source.target)}
	if source.originTarget != nil {
		targets = append(targets, proto.CloneOf(source.originTarget))
	}
	return targets
}

func (source EdgeCheckSource) ImageSelector() *agentpb.WorkloadImageSelector {
	return &agentpb.WorkloadImageSelector{
		Selector: &agentpb.WorkloadImageSelector_RequestedReference{
			RequestedReference: source.configuration.HelperImage,
		},
	}
}

// BindObservationAction supplies only the compiled public-SDK action and
// readiness endpoint. Runtime identity and generation stay repository-selected.
func (source *EdgeCheckSource) BindObservationAction(
	action *agentpb.ComponentApply,
	readyURL, operationID string,
) error {
	if source == nil || source.artifact == nil || action == nil || source.boundAction {
		return errs.New(errs.KindValidationFailed, "Tunnel check observation action is missing or already bound")
	}
	bound := proto.CloneOf(action)
	bound.ComponentId = source.configuration.Request.ComponentID
	bound.ArtifactId = source.artifact.ArtifactId
	bound.ArtifactDigest = slices.Clone(source.artifact.YamlSha256)
	bound.Generation = source.connector.Record.RenderGeneration
	parameters := source.configuration.Request
	parameters.ReadyURL, parameters.RequestID = readyURL, operationID
	if err := edgeconnectioncheck.ValidateCapturedRuntime(parameters, source.artifact, bound); err != nil {
		return err
	}
	value, err := proto.MarshalOptions{Deterministic: true}.Marshal(bound)
	if err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	source.configuration.Request, source.configuration.Action, source.boundAction = parameters, value, true
	return nil
}

func (source *EdgeCheckSource) BindHostSelection(
	images *agentpb.WorkloadImageResolutionResult,
	observation *agentpb.ServiceObservationResult,
) error {
	if source == nil || source.target == nil || source.configuration.ConnectorContainerID != "" ||
		source.configuration.HelperImageID != "" {
		return errs.New(errs.KindValidationFailed, "Tunnel check host selection is missing or already bound")
	}
	if images == nil || workloadimage.ValidateResult(&agentpb.ResolveWorkloadImages{
		RequestId: images.GetRequestId(), Selectors: []*agentpb.WorkloadImageSelector{source.ImageSelector()},
	}, images) != nil || images.GetSuccess() == nil {
		return errs.New(errs.KindStateConflict, "installed Agent helper image is unavailable on the host")
	}
	if observation == nil || serviceobservation.ValidateResult(&agentpb.ObserveServices{
		RequestId: observation.GetRequestId(), Targets: source.ObservationTargets(),
	}, observation) != nil || observation.Observations[0].GetReplicas() == nil {
		return errs.New(
			errs.KindStateConflict,
			"Tunnel runtime could not be observed; wait for the Agent and submit a new check",
		)
	}
	row := observation.Observations[0]
	if serviceobservation.Total(row.GetReplicas()) != 1 || len(row.Containers) != 1 ||
		row.Containers[0].State != "running" ||
		row.Containers[0].Replica != 1 {
		return errs.New(
			errs.KindStateConflict,
			"Tunnel must have exactly one running acknowledged connector; apply or start it before checking access",
		)
	}
	if source.originTarget != nil {
		origin := observation.Observations[1]
		if origin.GetReplicas() == nil ||
			serviceobservation.Total(origin.GetReplicas()) != uint64(source.originReplicas) ||
			len(origin.Containers) != int(source.originReplicas) ||
			origin.ProxyState != agentpb.ServiceProxyObservationState_SERVICE_PROXY_OBSERVATION_STATE_MATCHING {
			return errs.New(
				errs.KindStateConflict,
				"origin workload or proxy does not match its acknowledged serving runtime; finish Deploy before checking Tunnel access",
			)
		}
		for _, container := range origin.Containers {
			if container.State != "running" {
				return errs.New(
					errs.KindStateConflict,
					"origin workload is not running; start or Deploy it before checking Tunnel access",
				)
			}
		}
	}
	source.configuration.HelperImageID = images.GetSuccess().Resolutions[0].LocalImageId
	source.configuration.ConnectorContainerID = row.Containers[0].Id
	return nil
}

func (source EdgeCheckSource) Configuration() (edgechecks.Source, error) {
	if !source.boundAction || source.configuration.Validate() != nil {
		return edgechecks.Source{}, errs.New(errs.KindValidationFailed, "Tunnel check source selection is incomplete")
	}
	configuration := source.configuration
	configuration.Runtime, configuration.Action, configuration.Zones = slices.Clone(
		configuration.Runtime,
	), slices.Clone(
		configuration.Action,
	), slices.Clone(
		configuration.Zones,
	)
	return configuration, nil
}
