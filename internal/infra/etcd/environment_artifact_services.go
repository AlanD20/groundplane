package etcd

import (
	"strconv"

	"github.com/AlanD20/groundplane/internal/common/ids"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type environmentArtifactServiceIdentity struct {
	name             string
	componentID      string
	renderGeneration uint64
	capturedRuntime  bool
}

// One logical Service is either the normalized authored service, a portless
// singleton, or a stable proxy plus its exact singleton or blue/green workloads.
func validateEnvironmentArtifactServices(
	artifact *agentpb.ComposeArtifact,
	expected map[string]environmentArtifactServiceIdentity,
) error {
	groups := make(map[string]map[string]*agentpb.ComposeService, len(expected))
	names := make(map[string]struct{}, len(artifact.Services))
	for _, service := range artifact.Services {
		owner, exists := expected[service.GetServiceId()]
		if service == nil || !exists || service.ComposeName == "" || owner.componentID != service.OwnerComponentId {
			return errs.New(errs.KindValidationFailed, "Environment Compose Service ownership is invalid")
		}
		if _, duplicate := names[service.ComposeName]; duplicate {
			return errs.New(errs.KindValidationFailed, "Environment Compose Service name is duplicated")
		}
		names[service.ComposeName] = struct{}{}
		if groups[service.ServiceId] == nil {
			groups[service.ServiceId] = make(map[string]*agentpb.ComposeService)
		}
		key, wantName := "", owner.name
		var nameErr error
		switch service.Role {
		case agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_UNSPECIFIED:
			key = "authored"
		case agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_RECREATE_SINGLETON:
			key = "singleton"
			if service.ComposeName != owner.name {
				wantName, nameErr = domain.WorkloadComposeName(owner.name, domain.WorkloadSingleton)
			}
		case agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY:
			key = "proxy"
		case agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT:
			if service.Slot != string(domain.WorkloadBlue) && service.Slot != string(domain.WorkloadGreen) {
				return errs.New(errs.KindValidationFailed, "Environment Compose workload slot is invalid")
			}
			key = service.Slot
			wantName, nameErr = domain.WorkloadComposeName(owner.name, domain.WorkloadTarget(service.Slot))
		default:
			return errs.New(errs.KindValidationFailed, "Environment Compose Service role is invalid")
		}
		if nameErr != nil || (owner.name != "" && service.ComposeName != wantName) ||
			(key != "blue" && key != "green" && service.Slot != "") {
			return errs.New(errs.KindValidationFailed, "Environment Compose Service runtime identity changed")
		}
		if _, duplicate := groups[service.ServiceId][key]; duplicate {
			return errs.New(errs.KindValidationFailed, "Environment Compose Service role is duplicated")
		}
		groups[service.ServiceId][key] = service
	}
	for id, owner := range expected {
		group := groups[id]
		if len(group) == 1 &&
			(group["authored"] != nil || group["singleton"] != nil && group["singleton"].ComposeName == owner.name) {
			continue
		}
		if group["proxy"] != nil &&
			(len(group) == 2 && group["singleton"] != nil || len(group) == 3 && group["blue"] != nil && group["green"] != nil) {
			continue
		}
		if len(group) == 2 && retainedSingleSlotCoverage(group, owner) {
			continue
		}
		return errs.New(errs.KindValidationFailed, "Environment Compose logical Service coverage is incomplete")
	}
	return nil
}

// Mixed Blueprint artifacts retain only physically authoritative old workloads.
// A fresh slot topology still requires both slots. Publication independently
// fences the historical source and binds the exact mixed artifact digest.
func retainedSingleSlotCoverage(
	group map[string]*agentpb.ComposeService,
	owner environmentArtifactServiceIdentity,
) bool {
	proxy := group["proxy"]
	workload := group["blue"]
	if workload == nil {
		workload = group["green"]
	}
	if proxy == nil || workload == nil || owner.renderGeneration == 0 {
		return false
	}
	proxyPlan, proxyGeneration, valid := historicalArtifactIdentity(proxy, owner)
	if !valid {
		return false
	}
	workloadPlan, workloadGeneration, valid := historicalArtifactIdentity(workload, owner)
	return valid && proxyPlan == workloadPlan && proxyGeneration == workloadGeneration
}

func historicalArtifactIdentity(
	service *agentpb.ComposeService,
	owner environmentArtifactServiceIdentity,
) (string, uint64, bool) {
	plan, encodedGeneration := "", ""
	for _, label := range service.ExpectedLabels {
		switch label.GetKey() {
		case "com.groundplane.plan-id":
			if plan != "" || label.GetValue() == "" {
				return "", 0, false
			}
			plan = label.GetValue()
		case "com.groundplane.render-generation":
			if encodedGeneration != "" || label.GetValue() == "" {
				return "", 0, false
			}
			encodedGeneration = label.GetValue()
		}
	}
	prior, err := strconv.ParseUint(encodedGeneration, 10, 64)
	allowedGeneration := prior < owner.renderGeneration || owner.capturedRuntime && prior == owner.renderGeneration
	return plan, prior, err == nil && prior > 0 && allowedGeneration &&
		strconv.FormatUint(prior, 10) == encodedGeneration && ids.Validate(ids.KindPlan, plan) == nil
}
