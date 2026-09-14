package executionplan

import (
	"slices"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// PrepareEntryRuntime projects only one selected Service's changed workloads
// from a sealed Entry artifact. The prior acknowledged runtime remains authority
// for its Release/slot identity and stable proxy bytes.
func PrepareEntryRuntime(
	source *agentpb.ComposeArtifact,
	previous CandidateRuntime,
	currentArtifactID, retainedPriorArtifactID string,
) (CandidateRuntime, error) {
	if source == nil || ids.Validate(ids.KindConfig, currentArtifactID) != nil ||
		ValidateNativePredecessorWitness(
			source.GetOwnerId(), previous.ServiceID, previous.CurrentArtifact, previous.RetainedPriorArtifact,
		) != nil {
		return CandidateRuntime{}, invalidEntryRuntime()
	}
	priorCurrent, err := openNativePredecessorArtifact(source.GetOwnerId(), previous.CurrentArtifact)
	if err != nil {
		return CandidateRuntime{}, invalidEntryRuntime()
	}
	priorWorkload, priorProxy, err := nativePredecessorServices(priorCurrent, previous.ServiceID)
	if err != nil || priorWorkload == nil {
		return CandidateRuntime{}, invalidEntryRuntime()
	}
	selected, err := entryRuntimeWorkload(source, previous.ServiceID, previous.ReleaseID, previous.Target)
	if err != nil || selected.GetComposeName() != priorWorkload.GetComposeName() {
		return CandidateRuntime{}, invalidEntryRuntime()
	}
	current, err := projectAttachRuntimeArtifact(
		source, selected, priorCurrent, priorWorkload, priorProxy, previous.ReleaseID, previous.Target,
	)
	if err != nil {
		return CandidateRuntime{}, err
	}
	current.ArtifactId = currentArtifactID
	currentBytes, err := marshalCandidateRuntimeArtifact(current)
	if err != nil {
		return CandidateRuntime{}, err
	}

	var retainedBytes []byte
	if len(previous.RetainedPriorArtifact) == 0 {
		if retainedPriorArtifactID != "" {
			return CandidateRuntime{}, invalidEntryRuntime()
		}
	} else {
		if ids.Validate(ids.KindConfig, retainedPriorArtifactID) != nil || retainedPriorArtifactID == currentArtifactID {
			return CandidateRuntime{}, invalidEntryRuntime()
		}
		priorRetained, openErr := openNativePredecessorArtifact(
			source.GetOwnerId(), previous.RetainedPriorArtifact,
		)
		if openErr != nil {
			return CandidateRuntime{}, invalidEntryRuntime()
		}
		retainedWorkload, retainedProxy, selectErr := nativePredecessorServices(priorRetained, previous.ServiceID)
		if selectErr != nil || retainedWorkload == nil || retainedProxy != nil {
			return CandidateRuntime{}, invalidEntryRuntime()
		}
		retainedRelease, retainedTarget := nativePredecessorReleaseID(retainedWorkload), attachRuntimeTarget(retainedWorkload)
		selectedRetained, selectErr := entryRuntimeWorkload(
			source, previous.ServiceID, retainedRelease, retainedTarget,
		)
		if selectErr != nil || selectedRetained.GetComposeName() != retainedWorkload.GetComposeName() {
			return CandidateRuntime{}, invalidEntryRuntime()
		}
		retained, projectErr := projectAttachRuntimeArtifact(
			source, selectedRetained, priorRetained, retainedWorkload, nil, retainedRelease, retainedTarget,
		)
		if projectErr != nil {
			return CandidateRuntime{}, projectErr
		}
		retained.ArtifactId = retainedPriorArtifactID
		retainedBytes, err = marshalCandidateRuntimeArtifact(retained)
		if err != nil {
			return CandidateRuntime{}, err
		}
	}
	result := CandidateRuntime{
		ServiceID: previous.ServiceID, ReleaseID: previous.ReleaseID, Target: previous.Target,
		ProxyGeneration: previous.ProxyGeneration, ProxyConfigSHA256: slices.Clone(previous.ProxyConfigSHA256),
		CurrentArtifact: currentBytes, RetainedPriorArtifact: retainedBytes,
	}
	if ValidateNativePredecessorWitness(source.GetOwnerId(), result.ServiceID,
		result.CurrentArtifact, result.RetainedPriorArtifact) != nil {
		return CandidateRuntime{}, invalidEntryRuntime()
	}
	return result, nil
}

func entryRuntimeWorkload(
	artifact *agentpb.ComposeArtifact,
	serviceID, releaseID, target string,
) (*agentpb.ComposeService, error) {
	var selected *agentpb.ComposeService
	for _, service := range artifact.GetServices() {
		if service.GetServiceId() != serviceID ||
			service.GetRole() == agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY ||
			nativePredecessorReleaseID(service) != releaseID || attachRuntimeTarget(service) != target {
			continue
		}
		if selected != nil {
			return nil, invalidEntryRuntime()
		}
		selected = service
	}
	if selected == nil {
		return nil, invalidEntryRuntime()
	}
	return selected, nil
}

func invalidEntryRuntime() error {
	return errs.New(errs.KindValidationFailed, "Entry runtime projection is invalid")
}
