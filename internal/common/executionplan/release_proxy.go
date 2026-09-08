package executionplan

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"strconv"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// ProxyConfigGeneration opens the generation embedded in every canonical
// server identity of a sealed Groundplane proxy config.
func ProxyConfigGeneration(config []byte, releaseID string) (uint64, error) {
	var document struct {
		Apps struct {
			HTTP struct {
				Servers map[string]json.RawMessage `json:"servers"`
			} `json:"http"`
		} `json:"apps"`
	}
	if ids.Validate(ids.KindDeployment, releaseID) != nil || json.Unmarshal(config, &document) != nil ||
		len(document.Apps.HTTP.Servers) == 0 {
		return 0, errs.New(errs.KindValidationFailed, "release proxy config generation is invalid")
	}
	wantSuffix := "_" + strings.ToLower(releaseID) + "_p"
	var generation uint64
	for name := range document.Apps.HTTP.Servers {
		if !strings.HasPrefix(name, "gp_g") {
			return 0, errs.New(errs.KindValidationFailed, "release proxy config generation is invalid")
		}
		rest := strings.TrimPrefix(name, "gp_g")
		separator := strings.IndexByte(rest, '_')
		if separator <= 0 || !strings.HasPrefix(rest[separator:], wantSuffix) {
			return 0, errs.New(errs.KindValidationFailed, "release proxy config generation is invalid")
		}
		parsed, err := strconv.ParseUint(rest[:separator], 10, 64)
		if err != nil || parsed == 0 || generation != 0 && generation != parsed {
			return 0, errs.New(errs.KindValidationFailed, "release proxy config generation is invalid")
		}
		generation = parsed
	}
	return generation, nil
}

func validateReleaseWorkloadStep(
	operation agentpb.PlanOperation,
	artifactID, serviceID, target string,
	artifacts map[string]*agentpb.ComposeArtifact,
) error {
	if !releaseOperation(operation) || validateID(ids.KindService, serviceID) != nil || !validReleaseTarget(target) {
		return errs.New(errs.KindValidationFailed, "release workload step is invalid")
	}
	artifact := artifacts[artifactID]
	if artifact == nil || releaseTargetService(artifact, serviceID, target) == nil {
		return errs.New(errs.KindValidationFailed, "release workload is absent from its Compose artifact")
	}
	return nil
}

func validateServiceProxySwitch(
	operation agentpb.PlanOperation,
	value *agentpb.ServiceProxySwitch,
	artifacts map[string]*agentpb.ComposeArtifact,
) error {
	if value == nil || !releaseOperation(operation) || validateID(ids.KindService, value.ServiceId) != nil ||
		!validReleaseTarget(
			value.FromTarget,
		) || !validReleaseTarget(value.ToTarget) || value.FromTarget == value.ToTarget ||
		value.ProxyGeneration == 0 || ids.Validate(ids.KindDeployment, value.ReleaseId) != nil ||
		!validSealedProxyConfig(value.ConfigJson, value.ConfigSha256) {
		return errs.New(errs.KindValidationFailed, "release proxy switch is invalid")
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
	if artifact == nil ||
		releaseRuntimeService(
			artifact,
			serviceID,
			"",
			agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY,
		) == nil {
		return errs.New(errs.KindValidationFailed, "stable release proxy is absent from its Compose artifact")
	}
	if priorArtifactID != "" && artifacts[priorArtifactID] == nil {
		return errs.New(errs.KindValidationFailed, "prior release topology artifact is absent")
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
	if service == nil || service.GetExpectedReplicas() < 1 || !service.GetHasHealthcheck() ||
		service.GetImageReference() == "" {
		return false
	}
	return service.GetRole() != agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT ||
		service.GetExpectedReplicas() == 1
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
		operation == agentpb.PlanOperation_PLAN_OPERATION_ROLLBACK
}
