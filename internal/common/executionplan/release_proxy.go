package executionplan

import (
	"crypto/sha256"
	"crypto/subtle"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/serviceproxy"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// ProxyConfigGeneration opens the generation embedded in every canonical
// server identity of a sealed Groundplane proxy config.
func ProxyConfigGeneration(
	implementation agentpb.ServiceProxyImplementation,
	config []byte,
	releaseID string,
) (uint64, error) {
	selected, err := OpenProxyImplementation(implementation)
	if err != nil {
		return 0, err
	}
	configuration, err := serviceproxy.Read(selected, config)
	if err != nil || ids.Validate(ids.KindDeployment, releaseID) != nil || configuration.ReleaseID != releaseID {
		return 0, errs.New(
			errs.KindValidationFailed,
			"Service proxy configuration does not identify the captured Release",
		)
	}
	return configuration.Generation, nil
}

func validateReleaseWorkloadStep(
	operation agentpb.PlanOperation,
	artifactID, serviceID, target string,
	replicaOrdinal uint32,
	artifacts map[string]*agentpb.ComposeArtifact,
) error {
	if !releaseOperation(operation) || validateID(ids.KindService, serviceID) != nil {
		return errs.New(errs.KindValidationFailed, "release workload step is invalid")
	}
	artifact := artifacts[artifactID]
	_, err := ReleaseWorkloadService(artifact, serviceID, target, replicaOrdinal)
	return err
}

func validateServiceProxySwitch(
	operation agentpb.PlanOperation,
	value *agentpb.ServiceProxySwitch,
	artifacts map[string]*agentpb.ComposeArtifact,
) error {
	if value == nil || !releaseOperation(operation) || validateID(ids.KindService, value.ServiceId) != nil ||
		(!validReleaseTarget(value.FromTarget) && value.FromTarget != "rolling") ||
		!validReleaseTarget(value.ToTarget) || value.FromTarget == value.ToTarget ||
		value.ProxyGeneration == 0 || ids.Validate(ids.KindDeployment, value.ReleaseId) != nil ||
		!validSealedProxyConfig(value.ConfigJson, value.ConfigSha256) {
		return errs.New(errs.KindValidationFailed, "release proxy switch is invalid")
	}
	if value.FromTarget == "rolling" {
		if value.ToTarget != "blue" && value.ToTarget != "green" {
			return errs.New(errs.KindValidationFailed, "rolling proxy handoff requires a blue-green candidate")
		}
		workloads, proxy, err := nativePredecessorServices(artifacts[value.PriorArtifactId], value.ServiceId)
		if err != nil || proxy == nil ||
			workloads[0].GetRole() != agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_ROLLING_REPLICA {
			return errs.New(
				errs.KindValidationFailed,
				"rolling proxy handoff requires the complete captured predecessor",
			)
		}
	}
	return validateProxyArtifacts(value.CandidateArtifactId, value.PriorArtifactId, value.ServiceId, artifacts)
}

func validateServiceProxyProbe(
	operation agentpb.PlanOperation,
	value *agentpb.ServiceProxyProbe,
	artifacts map[string]*agentpb.ComposeArtifact,
) error {
	if value == nil || !releaseOperation(operation) || validateID(ids.KindService, value.ServiceId) != nil ||
		!validReleaseTarget(value.ExpectedTarget) || value.ProxyGeneration == 0 || value.ReleaseId == "" ||
		!validSealedProxyConfig(value.ConfigJson, value.ConfigSha256) || !validReleaseTarget(value.AlternateTarget) ||
		value.AlternateTarget == value.ExpectedTarget || value.AlternateProxyGeneration == 0 ||
		ids.Validate(ids.KindDeployment, value.AlternateReleaseId) != nil ||
		!validSealedProxyConfig(value.AlternateConfigJson, value.AlternateConfigSha256) {
		return errs.New(errs.KindValidationFailed, "release proxy probe is invalid")
	}
	return validateProxyArtifacts(value.CandidateArtifactId, value.PriorArtifactId, value.ServiceId, artifacts)
}

func validateServiceProxyCompensate(
	operation agentpb.PlanOperation,
	value *agentpb.ServiceProxyCompensate,
	artifacts map[string]*agentpb.ComposeArtifact,
) error {
	if value == nil || !releaseOperation(operation) || validateID(ids.KindService, value.ServiceId) != nil ||
		!validReleaseTarget(value.CandidateTarget) || !validReleaseTarget(value.PriorTarget) ||
		value.CandidateTarget == value.PriorTarget || value.ProxyGeneration == 0 || value.PriorReleaseId == "" ||
		!validSealedProxyConfig(value.ConfigJson, value.ConfigSha256) {
		return errs.New(errs.KindValidationFailed, "release proxy compensation is invalid")
	}
	return validateProxyArtifacts(value.CandidateArtifactId, value.PriorArtifactId, value.ServiceId, artifacts)
}

func validateProxyArtifacts(
	candidateArtifactID, priorArtifactID, serviceID string,
	artifacts map[string]*agentpb.ComposeArtifact,
) error {
	artifactID := candidateArtifactID
	artifact := artifacts[artifactID]
	proxy := releaseRuntimeService(
		artifact,
		serviceID,
		"",
		agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY,
	)
	if artifact == nil || proxy == nil {
		return errs.New(errs.KindValidationFailed, "stable release proxy is absent from its Compose artifact")
	}
	configuration, err := OpenProxyConfiguration(proxy)
	if err != nil {
		return err
	}
	if err := serviceproxy.ValidateTrafficSwitchPorts(configuration.Implementation, configuration.Ports); err != nil {
		return err
	}
	if priorArtifactID != "" && artifacts[priorArtifactID] == nil {
		return errs.New(errs.KindValidationFailed, "prior release topology artifact is absent")
	}
	if priorArtifactID != "" {
		priorProxy := releaseRuntimeService(
			artifacts[priorArtifactID],
			serviceID,
			"",
			agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY,
		)
		if err := validateStableProxyContinuity(proxy, priorProxy); err != nil {
			return err
		}
	}
	return nil
}

func validateServiceRecreateAcknowledge(
	operation agentpb.PlanOperation,
	value *agentpb.ServiceRecreateAcknowledge,
	artifacts map[string]*agentpb.ComposeArtifact,
) error {
	if value == nil || !releaseOperation(operation) || validateID(ids.KindService, value.ServiceId) != nil ||
		ids.Validate(ids.KindDeployment, value.ReleaseId) != nil ||
		validateRecreateArtifact(value.ArtifactId, value.ServiceId, value.ReleaseId, artifacts) != nil {
		return errs.New(errs.KindValidationFailed, "release recreate acknowledgement is invalid")
	}
	return nil
}

func validateServiceRecreateProbe(
	operation agentpb.PlanOperation,
	value *agentpb.ServiceRecreateProbe,
	artifacts map[string]*agentpb.ComposeArtifact,
) error {
	if value == nil || !releaseOperation(operation) || validateID(ids.KindService, value.ServiceId) != nil ||
		ids.Validate(
			ids.KindDeployment,
			value.CandidateReleaseId,
		) != nil || !validPriorReleaseID(value.PriorReleaseId) ||
		validateRecreateArtifact(
			value.CandidateArtifactId,
			value.ServiceId,
			value.CandidateReleaseId,
			artifacts,
		) != nil ||
		validatePriorTopologyArtifact(
			value.PriorArtifactId,
			value.ServiceId,
			value.PriorReleaseId,
			"",
			artifacts,
		) != nil {
		return errs.New(errs.KindValidationFailed, "release recreate probe is invalid")
	}
	return nil
}

func validateServiceRecreateCompensate(
	operation agentpb.PlanOperation,
	value *agentpb.ServiceRecreateCompensate,
	artifacts map[string]*agentpb.ComposeArtifact,
) error {
	if value == nil || !releaseOperation(operation) || validateID(ids.KindService, value.ServiceId) != nil ||
		ids.Validate(
			ids.KindDeployment,
			value.CandidateReleaseId,
		) != nil || !validPriorReleaseID(value.PriorReleaseId) ||
		!validReleaseTarget(value.PriorTarget) ||
		validateRecreateArtifact(
			value.CandidateArtifactId,
			value.ServiceId,
			value.CandidateReleaseId,
			artifacts,
		) != nil ||
		validatePriorTopologyArtifact(
			value.ArtifactId, value.ServiceId, value.PriorReleaseId, value.PriorTarget, artifacts,
		) != nil {
		return errs.New(errs.KindValidationFailed, "release recreate compensation is invalid")
	}
	return nil
}

func validatePriorTopologyArtifact(
	artifactID, serviceID, releaseID, target string,
	artifacts map[string]*agentpb.ComposeArtifact,
) error {
	artifact := artifacts[artifactID]
	if artifact == nil {
		return errs.New(errs.KindValidationFailed, "prior release topology artifact is absent")
	}
	workloads := 0
	var selected *agentpb.ComposeService
	for _, service := range artifact.GetServices() {
		if service.GetServiceId() == serviceID &&
			(service.GetRole() == agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_RECREATE_SINGLETON || service.GetRole() == agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT) {
			workloads++
			serviceTarget := service.GetSlot()
			if serviceTarget == "" {
				serviceTarget = "singleton"
			}
			if expectedReleaseLabel(service) == releaseID && (target == "" || serviceTarget == target) {
				if selected != nil {
					return errs.New(errs.KindValidationFailed, "prior release workload topology is ambiguous")
				}
				selected = service
			}
		}
	}
	if workloads != 1 && workloads != 2 || !validObservedRecreateService(selected) {
		return errs.New(errs.KindValidationFailed, "prior release workload topology is invalid")
	}
	return nil
}

func validateRecreateArtifact(
	artifactID, serviceID, releaseID string,
	artifacts map[string]*agentpb.ComposeArtifact,
) error {
	artifact := artifacts[artifactID]
	service := releaseRuntimeService(
		artifact,
		serviceID,
		"",
		agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_RECREATE_SINGLETON,
	)
	if artifact == nil || service == nil || expectedReleaseLabel(service) != releaseID ||
		!validObservedRecreateService(service) {
		return errs.New(errs.KindValidationFailed, "release recreate singleton is absent from its Compose artifact")
	}
	return nil
}

func validObservedRecreateService(service *agentpb.ComposeService) bool {
	if service == nil || service.GetExpectedReplicas() < 1 ||
		service.GetImageReference() == "" {
		return false
	}
	return service.GetRole() != agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT ||
		service.GetExpectedReplicas() == 1 && service.GetHasHealthcheck()
}

func expectedReleaseLabel(service *agentpb.ComposeService) string {
	for _, label := range service.GetExpectedLabels() {
		if label.GetKey() == labelReleaseID {
			return label.GetValue()
		}
	}
	return ""
}

func validPriorReleaseID(value string) bool {
	return ids.Validate(ids.KindDeployment, value) == nil
}

func releaseRuntimeService(
	artifact *agentpb.ComposeArtifact,
	serviceID, slot string,
	role agentpb.ComposeServiceRole,
) *agentpb.ComposeService {
	for _, service := range artifact.GetServices() {
		if service.GetServiceId() == serviceID && service.GetRole() == role && service.GetSlot() == slot {
			return service
		}
	}
	return nil
}

func validSealedProxyConfig(config, digest []byte) bool {
	computed := sha256.Sum256(config)
	return len(config) != 0 && len(config) <= MaximumArtifactYAMLBytes && len(digest) == sha256.Size &&
		subtle.ConstantTimeCompare(computed[:], digest) == 1
}

func validReleaseTarget(target string) bool {
	return target == "singleton" || target == "blue" || target == "green"
}

func releaseTargetService(artifact *agentpb.ComposeArtifact, serviceID, target string) *agentpb.ComposeService {
	if target == "singleton" {
		return releaseRuntimeService(
			artifact,
			serviceID,
			"",
			agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_RECREATE_SINGLETON,
		)
	}
	return releaseRuntimeService(
		artifact,
		serviceID,
		target,
		agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT,
	)
}

func releaseOperation(operation agentpb.PlanOperation) bool {
	return operation == agentpb.PlanOperation_PLAN_OPERATION_DEPLOY ||
		operation == agentpb.PlanOperation_PLAN_OPERATION_ROLLBACK ||
		operation == agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY
}
