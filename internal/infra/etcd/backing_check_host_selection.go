package etcd

import (
	"github.com/AlanD20/groundplane/internal/common/backingaccess"
	"github.com/AlanD20/groundplane/internal/common/serviceobservation"
	"github.com/AlanD20/groundplane/internal/common/workloadimage"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backingchecks"
	"github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func (source BackingCheckSource) ObservationTarget() *agentpb.ServiceObservationTarget {
	if source.target == nil {
		return nil
	}
	return proto.Clone(source.target).(*agentpb.ServiceObservationTarget)
}

func (source BackingCheckSource) ImageSelector() *agentpb.WorkloadImageSelector {
	return &agentpb.WorkloadImageSelector{
		Selector: &agentpb.WorkloadImageSelector_RequestedReference{RequestedReference: source.HelperImage()},
	}
}

// BindHostSelection consumes validated read-only responses for this capture.
// The source's private selection cannot be substituted during Task publication.
func (source *BackingCheckSource) BindHostSelection(images *agentpb.WorkloadImageResolutionResult,
	observation *agentpb.ServiceObservationResult,
) error {
	if source == nil || source.target == nil || source.helperImageID != "" || source.containerID != "" {
		return errs.New(errs.KindValidationFailed, "access check host selection is missing or already bound")
	}
	if images == nil || workloadimage.ValidateResult(&agentpb.ResolveWorkloadImages{
		RequestId: images.GetRequestId(), Selectors: []*agentpb.WorkloadImageSelector{source.ImageSelector()},
	}, images) != nil || images.GetSuccess() == nil {
		return errs.New(errs.KindStateConflict, "installed Agent helper image is unavailable on the host")
	}
	if observation == nil || serviceobservation.ValidateResult(&agentpb.ObserveServices{
		RequestId: observation.GetRequestId(), Targets: []*agentpb.ServiceObservationTarget{source.target},
	}, observation) != nil || observation.Observations[0].GetReplicas() == nil {
		return errs.New(
			errs.KindStateConflict,
			"consumer runtime could not be observed; wait for the Agent and retry the check",
		)
	}
	row := observation.Observations[0]
	if serviceobservation.Total(row.GetReplicas()) != uint64(source.replicas) {
		return errs.New(errs.KindStateConflict, "consumer replica count differs from its acknowledged deployment")
	}
	var selected *agentpb.ServiceContainerObservation
	for _, container := range row.Containers {
		if container.State == "running" && (selected == nil || container.Replica < selected.Replica) {
			selected = container
		}
	}
	if selected == nil {
		return errs.New(
			errs.KindStateConflict,
			"consumer has no running workload container; Deploy or Start it before checking access",
		)
	}
	source.helperImageID = images.GetSuccess().GetResolutions()[0].GetLocalImageId()
	source.containerID = selected.Id
	return nil
}

// Configuration exposes only safe parameters and versions. It is called inside
// the fact callback, without retaining its plaintext password slice.
func (source BackingCheckSource) Configuration(input backingaccess.Request) (backingchecks.Source, error) {
	consumer := source.facts.Consumer.Record
	if source.helperImageID == "" || source.containerID == "" || input.Validate() != nil ||
		input.AttachID != consumer.ID || input.CredentialAttachID != consumer.CredentialAttachID ||
		input.EnvironmentID != consumer.EnvironmentID || input.ServiceID != consumer.ServiceID ||
		input.BackingServiceID != consumer.BackingServiceID || string(input.Family) != source.AdapterKey() {
		return backingchecks.Source{}, errs.New(
			errs.KindValidationFailed,
			"access check input differs from its captured Attach",
		)
	}
	input.Password = nil
	runtime, err := proto.MarshalOptions{Deterministic: true}.Marshal(source.target)
	if err != nil {
		return backingchecks.Source{}, errs.Wrap(errs.KindInternal, err)
	}
	result := backingchecks.Source{Parameters: input, AttachRevision: source.facts.Consumer.Revision,
		CredentialRevision: source.facts.Owner.Revision, FactsRevision: source.facts.Facts.Revision,
		AgentID: source.agent.Record.ID, AgentRevision: source.agent.Revision,
		HelperImage: source.agent.Record.Image, HelperImageID: source.helperImageID, ContainerID: source.containerID,
		RuntimeRevision: source.runtimeRevision, Runtime: runtime, BackingEnvironmentID: consumer.BackingEnvironmentID}
	result.ConsumerDesiredRevision = source.consumerService.Revision
	result.ConsumerIntentRevision = services.ServiceRuntimeRevision(source.consumerService)
	result.BackingDesiredRevision = source.backingService.Revision
	result.BackingRuntimeRevision = services.ServiceRuntimeRevision(source.backingService)
	if source.facts.Grant != nil {
		result.GrantID, result.GrantRevision = source.facts.Grant.Record.ID, source.facts.Grant.Revision
	}
	return result, nil
}
