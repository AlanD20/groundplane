package executionplan

import (
	"bytes"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workloadimage"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// NativePredecessorWitness is the immutable, per-Service native runtime
// witness carried by a Blueprint candidate. An empty CurrentArtifact is an
// explicit candidate-absence witness; it never means "inspect applied state".
type NativePredecessorWitness struct {
	ServiceID             string
	CurrentArtifact       []byte
	RetainedPriorArtifact []byte
}

// ValidateNativePredecessorWitness validates one native runtime snapshot
// without consulting a plan, a repository, or host state. It is also used by
// the durable claim validator, which cannot consume an ExecutionPlan.
func ValidateNativePredecessorWitness(environmentID, serviceID string, current, retained []byte) error {
	if ids.Validate(ids.KindEnvironment, environmentID) != nil || ids.Validate(ids.KindService, serviceID) != nil ||
		len(current) == 0 {
		return invalidNativePredecessorWitness()
	}
	currentArtifact, err := openNativePredecessorArtifact(environmentID, current)
	if err != nil {
		return err
	}
	currentWorkload, currentProxy, err := nativePredecessorServices(currentArtifact, serviceID)
	if err != nil || currentWorkload == nil {
		return invalidNativePredecessorWitness()
	}
	if err := validateNativePredecessorWorkload(currentWorkload); err != nil {
		return err
	}
	if currentProxy != nil && validateNativePredecessorProxy(currentProxy) != nil {
		return invalidNativePredecessorWitness()
	}
	if retained == nil {
		return nil
	}
	retainedArtifact, err := openNativePredecessorArtifact(environmentID, retained)
	if err != nil {
		return err
	}
	retainedWorkload, retainedProxy, err := nativePredecessorServices(retainedArtifact, serviceID)
	if err != nil || retainedWorkload == nil || retainedProxy != nil ||
		retainedArtifact.GetArtifactId() == currentArtifact.GetArtifactId() {
		return invalidNativePredecessorWitness()
	}
	if err := validateNativePredecessorWorkload(retainedWorkload); err != nil {
		return err
	}
	if currentWorkload.GetRole() != agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT ||
		retainedWorkload.GetRole() != agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT ||
		currentWorkload.GetSlot() == retainedWorkload.GetSlot() ||
		nativePredecessorReleaseID(currentWorkload) == nativePredecessorReleaseID(retainedWorkload) {
		return invalidNativePredecessorWitness()
	}
	return nil
}

// SelectNativeRestorationMembers binds every candidate to exactly one sorted
// native witness. Selection is deterministic and never falls back to an
// acknowledged Environment artifact.
func SelectNativeRestorationMembers(
	environmentID string,
	candidates []CandidateServiceIdentity,
	witnesses []NativePredecessorWitness,
) ([]CandidateRestorationSelection, error) {
	if ids.Validate(ids.KindEnvironment, environmentID) != nil || len(candidates) == 0 ||
		len(candidates) != len(witnesses) {
		return nil, invalidNativePredecessorWitness()
	}
	result := make([]CandidateRestorationSelection, len(candidates))
	previous := ""
	for index, candidate := range candidates {
		if ids.Validate(ids.KindService, candidate.ServiceID) != nil ||
			ids.Validate(ids.KindDeployment, candidate.ReleaseID) != nil ||
			index > 0 && candidate.ServiceID <= previous {
			return nil, invalidNativePredecessorWitness()
		}
		previous = candidate.ServiceID
		witness := witnesses[index]
		if witness.ServiceID != candidate.ServiceID || index > 0 && witnesses[index-1].ServiceID >= witness.ServiceID {
			return nil, invalidNativePredecessorWitness()
		}
		if len(witness.CurrentArtifact) == 0 {
			if len(witness.RetainedPriorArtifact) != 0 {
				return nil, invalidNativePredecessorWitness()
			}
			result[index] = CandidateRestorationSelection{
				ServiceID: candidate.ServiceID, ReleaseID: candidate.ReleaseID,
				Target: agentpb.ReleaseRestorationTarget_RELEASE_RESTORATION_TARGET_CANDIDATE_ABSENCE,
			}
			continue
		}
		if err := ValidateNativePredecessorWitness(environmentID, witness.ServiceID, witness.CurrentArtifact, witness.RetainedPriorArtifact); err != nil {
			return nil, err
		}
		artifact, err := openNativePredecessorArtifact(environmentID, witness.CurrentArtifact)
		if err != nil {
			return nil, err
		}
		workload, _, err := nativePredecessorServices(artifact, candidate.ServiceID)
		if err != nil || workload == nil || nativePredecessorReleaseID(workload) == candidate.ReleaseID {
			return nil, invalidNativePredecessorWitness()
		}
		result[index] = CandidateRestorationSelection{
			ServiceID: candidate.ServiceID, ReleaseID: candidate.ReleaseID,
			Target: agentpb.ReleaseRestorationTarget_RELEASE_RESTORATION_TARGET_SERVING_PREDECESSOR,
		}
	}
	return result, nil
}

func openNativePredecessorArtifact(environmentID string, encoded []byte) (*agentpb.ComposeArtifact, error) {
	if len(encoded) == 0 || len(encoded) > MaximumPlanBytes {
		return nil, invalidNativePredecessorWitness()
	}
	artifact := new(agentpb.ComposeArtifact)
	if (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(encoded, artifact) != nil ||
		RejectUnknown(artifact) != nil {
		return nil, invalidNativePredecessorWitness()
	}
	canonical, err := (proto.MarshalOptions{Deterministic: true}).Marshal(artifact)
	if err != nil || !bytes.Equal(canonical, encoded) ||
		ids.Validate(ids.KindConfig, artifact.GetArtifactId()) != nil ||
		artifact.GetOwnerKind() != agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT ||
		artifact.GetOwnerId() != environmentID ||
		artifact.GetProjectName() != "gp-"+strings.ToLower(environmentID) {
		return nil, invalidNativePredecessorWitness()
	}
	return artifact, nil
}

func nativePredecessorServices(
	artifact *agentpb.ComposeArtifact,
	serviceID string,
) (*agentpb.ComposeService, *agentpb.ComposeService, error) {
	var workload, proxy *agentpb.ComposeService
	for _, service := range artifact.GetServices() {
		if service == nil || service.GetServiceId() != serviceID {
			return nil, nil, invalidNativePredecessorWitness()
		}
		switch service.GetRole() {
		case agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY:
			if proxy != nil {
				return nil, nil, invalidNativePredecessorWitness()
			}
			proxy = service
		case agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_RECREATE_SINGLETON,
			agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT:
			if workload != nil {
				return nil, nil, invalidNativePredecessorWitness()
			}
			workload = service
		default:
			return nil, nil, invalidNativePredecessorWitness()
		}
	}
	return workload, proxy, nil
}

func validateNativePredecessorWorkload(service *agentpb.ComposeService) error {
	labels, err := nativePredecessorLabels(service.GetExpectedLabels())
	if err != nil || service.GetComposeName() == "" || service.GetExpectedReplicas() == 0 ||
		!service.GetHasHealthcheck() || !workloadimage.LocalIDValid(service.GetImageReference()) ||
		service.GetOwnerComponentId() != "" || ids.Validate(ids.KindService, service.GetServiceId()) != nil {
		return invalidNativePredecessorWitness()
	}
	releaseID, runtimeRole := labels[labelReleaseID], labels[labelRuntimeRole]
	if ids.Validate(ids.KindDeployment, releaseID) != nil {
		return invalidNativePredecessorWitness()
	}
	switch service.GetRole() {
	case agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_RECREATE_SINGLETON:
		if service.GetSlot() != "" || labels[labelSlot] != "" || runtimeRole != "singleton" {
			return invalidNativePredecessorWitness()
		}
	case agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT:
		if (service.GetSlot() != "blue" && service.GetSlot() != "green") || labels[labelSlot] != service.GetSlot() ||
			runtimeRole != "slot" {
			return invalidNativePredecessorWitness()
		}
	default:
		return invalidNativePredecessorWitness()
	}
	return nil
}

func validateNativePredecessorProxy(service *agentpb.ComposeService) error {
	labels, err := nativePredecessorLabels(service.GetExpectedLabels())
	if err != nil || service.GetComposeName() == "" || service.GetOwnerComponentId() != "" ||
		labels[labelRuntimeRole] != "proxy" || labels[labelReleaseID] != "" || labels[labelSlot] != "" {
		return invalidNativePredecessorWitness()
	}
	return nil
}

func nativePredecessorLabels(pairs []*agentpb.LabelPair) (map[string]string, error) {
	labels := make(map[string]string, len(pairs))
	previous := ""
	for _, pair := range pairs {
		if pair == nil || pair.GetKey() <= previous || !validLabelKey(pair.GetKey()) {
			return nil, invalidNativePredecessorWitness()
		}
		previous = pair.GetKey()
		labels[pair.GetKey()] = pair.GetValue()
	}
	return labels, nil
}

func nativePredecessorReleaseID(service *agentpb.ComposeService) string {
	for _, pair := range service.GetExpectedLabels() {
		if pair.GetKey() == labelReleaseID {
			return pair.GetValue()
		}
	}
	return ""
}

func invalidNativePredecessorWitness() error {
	return errs.New(errs.KindValidationFailed, "native predecessor witness is invalid or ambiguous")
}
