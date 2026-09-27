package executionplan

import (
	"bytes"
	"crypto/sha256"
	"strconv"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

const maximumManagedComponentServices = 2

func validateManagedComponentSourceOwnership(
	plan *agentpb.ExecutionPlan,
	artifact *agentpb.ComposeArtifact,
) error {
	procedure := plan.GetManagedComponentProcedure()
	if procedure == nil || artifact == nil {
		return nil
	}
	seenServices := make(map[string]struct{})
	seenComponents := make(map[string]struct{})
	for _, candidate := range procedure.GetServices() {
		if candidate.GetSourceArtifactId() != artifact.GetArtifactId() {
			continue
		}
		if _, duplicate := seenServices[candidate.GetServiceId()]; duplicate {
			return errs.New(errs.KindValidationFailed, "managed Component source service identity is duplicated")
		}
		if _, duplicate := seenComponents[candidate.GetComponentId()]; duplicate {
			return errs.New(errs.KindValidationFailed, "managed Component source Component identity is duplicated")
		}
		service := artifactService(artifact, candidate.GetServiceId())
		if service == nil || service.GetOwnerComponentId() != candidate.GetComponentId() ||
			service.GetComposeName() != candidate.GetComposeServiceName() {
			return errs.New(errs.KindValidationFailed, "managed Component source service identity changed")
		}
		labels := labelValues(service.GetExpectedLabels())
		generation, generationErr := strconv.ParseUint(labels[labelRenderGen], 10, 64)
		if validateID(ids.KindPlan, labels[labelPlanID]) != nil || generationErr != nil || generation == 0 ||
			labels[labelComponentID] != candidate.GetComponentId() || labels[labelServiceID] != candidate.GetServiceId() {
			return errs.New(errs.KindValidationFailed, "managed Component source ownership is invalid")
		}
		seenServices[candidate.GetServiceId()] = struct{}{}
		seenComponents[candidate.GetComponentId()] = struct{}{}
	}
	// A captured Environment artifact may retain Components from different
	// successful Tasks. Validate each source's exact identity here; normal label
	// validation uses the actual new plan and bounds every retained generation.
	return nil
}

func validateManagedComponentProcedure(
	plan *agentpb.ExecutionPlan,
	artifacts map[string]*agentpb.ComposeArtifact,
) error {
	procedure := plan.GetManagedComponentProcedure()
	if procedure == nil {
		return nil
	}
	if plan.GetOperation() != agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY ||
		len(procedure.GetServices()) == 0 || len(procedure.GetServices()) > maximumManagedComponentServices {
		return errs.New(errs.KindValidationFailed, "managed Component procedure shape is invalid")
	}
	steps := make(map[string]*agentpb.ExecutionStep, len(plan.GetSteps()))
	for _, step := range plan.GetSteps() {
		steps[step.GetStepId()] = step
	}
	previousKind := ""
	for _, source := range procedure.GetServices() {
		if source == nil || source.GetComponentKind() <= previousKind ||
			(source.GetComponentKind() != "caddy" && source.GetComponentKind() != "cloudflare-tunnel") ||
			validateID(ids.KindComponent, source.GetComponentId()) != nil ||
			validateID(ids.KindService, source.GetServiceId()) != nil ||
			validateComposeName(source.GetComposeServiceName()) != nil ||
			validateID(ids.KindTask, source.GetSourceRevisionId()) != nil ||
			validateID(ids.KindConfig, source.GetSourceArtifactId()) != nil ||
			len(source.GetSourceArtifactSha256()) != sha256.Size ||
			validateID(ids.KindStep, source.GetRemoveStepId()) != nil {
			return errs.New(errs.KindValidationFailed, "managed Component procedure source is invalid or unsorted")
		}
		artifact := artifacts[source.GetSourceArtifactId()]
		service := artifactService(artifact, source.GetServiceId())
		remove := steps[source.GetRemoveStepId()].GetComposeRemove()
		var encoded []byte
		var encodeErr error
		if artifact != nil {
			encoded, encodeErr = (proto.MarshalOptions{Deterministic: true}).Marshal(artifact)
		}
		digest := sha256.Sum256(encoded)
		if artifact == nil || artifact.GetOwnerId() != plan.GetTargetId() || service == nil ||
			service.GetOwnerComponentId() != source.GetComponentId() ||
			service.GetComposeName() != source.GetComposeServiceName() || remove == nil ||
			remove.GetArtifactId() != source.GetSourceArtifactId() || remove.GetWholeProject() ||
			len(remove.GetServiceIds()) != 1 || remove.GetServiceIds()[0] != source.GetServiceId() ||
			encodeErr != nil || !bytes.Equal(digest[:], source.GetSourceArtifactSha256()) {
			return errs.New(errs.KindValidationFailed, "managed Component removal authority changed")
		}
		previousKind = source.GetComponentKind()
	}
	return nil
}

func artifactService(artifact *agentpb.ComposeArtifact, serviceID string) *agentpb.ComposeService {
	if artifact == nil {
		return nil
	}
	var result *agentpb.ComposeService
	for _, service := range artifact.GetServices() {
		if service.GetServiceId() != serviceID {
			continue
		}
		if result != nil {
			return nil
		}
		result = service
	}
	return result
}

func labelValues(labels []*agentpb.LabelPair) map[string]string {
	result := make(map[string]string, len(labels))
	for _, label := range labels {
		result[label.GetKey()] = label.GetValue()
	}
	return result
}
