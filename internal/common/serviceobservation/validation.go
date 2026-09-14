// Package serviceobservation owns the closed, workload-only Agent read exchange.
package serviceobservation

import (
	"crypto/sha256"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

const (
	MaximumTargets       = 200
	MaximumContainers    = 4096
	MaximumEnvelopeBytes = 262144
	Timeout              = 5 * time.Second
	Freshness            = 15 * time.Second
)

func ValidateRequest(request *agentpb.ObserveServices) error {
	if request == nil || len(request.ProtoReflect().GetUnknown()) != 0 ||
		ids.Validate(ids.KindOperation, "op_"+request.RequestId) != nil ||
		len(request.Targets) == 0 || len(request.Targets) > MaximumTargets {
		return invalid()
	}
	envelope := &agentpb.ControllerMessage{
		Payload: &agentpb.ControllerMessage_ObserveServices{ObserveServices: request},
	}
	if proto.Size(envelope) > MaximumEnvelopeBytes {
		return invalid()
	}
	seen := make(map[string]bool, len(request.Targets))
	seenNames := make(map[string]bool, len(request.Targets))
	for _, target := range request.Targets {
		if target == nil || len(target.ProtoReflect().GetUnknown()) != 0 ||
			ids.Validate(ids.KindEnvironment, target.EnvironmentId) != nil ||
			ids.Validate(ids.KindService, target.ServiceId) != nil ||
			ids.Validate(ids.KindDeployment, target.ReleaseId) != nil ||
			ids.Validate(ids.KindPlan, target.PlanId) != nil || target.RenderGeneration == 0 ||
			!composeNameValid(target.ComposeName) || target.EnvironmentId != request.Targets[0].EnvironmentId ||
			seen[target.ServiceId] || seenNames[target.ComposeName] {
			return invalid()
		}
		if target.RuntimeRole == "singleton" && target.Slot != "" ||
			target.RuntimeRole == "slot" && target.Slot != "blue" && target.Slot != "green" ||
			target.RuntimeRole != "singleton" && target.RuntimeRole != "slot" {
			return invalid()
		}
		proxyFields := target.ProxyPlanId != "" || target.ProxyRenderGeneration != 0 ||
			len(target.ProxyConfigSha256) != 0
		if target.ProxyComposeName == "" && proxyFields {
			return invalid()
		}
		if target.ProxyComposeName != "" && (!composeNameValid(target.ProxyComposeName) ||
			ids.Validate(ids.KindPlan, target.ProxyPlanId) != nil || target.ProxyRenderGeneration == 0 ||
			len(target.ProxyConfigSha256) != sha256.Size || target.ProxyComposeName == target.ComposeName ||
			target.ProxyComposeName == "") {
			return invalid()
		}
		seen[target.ServiceId], seenNames[target.ComposeName] = true, true
	}
	return nil
}

func ValidateResult(request *agentpb.ObserveServices, result *agentpb.ServiceObservationResult) error {
	if err := ValidateRequest(request); err != nil {
		return err
	}
	if result == nil || result.RequestId != request.RequestId || len(result.ProtoReflect().GetUnknown()) != 0 ||
		len(result.Observations) != len(request.Targets) {
		return invalid()
	}
	envelope := &agentpb.AgentMessage{
		Payload: &agentpb.AgentMessage_ServiceObservationResult{ServiceObservationResult: result},
	}
	if proto.Size(envelope) > MaximumEnvelopeBytes {
		return invalid()
	}
	var total uint64
	for index, row := range result.Observations {
		target := request.Targets[index]
		if row == nil || len(row.ProtoReflect().GetUnknown()) != 0 ||
			row.ServiceId != target.ServiceId || row.ReleaseId != target.ReleaseId {
			return invalid()
		}
		switch outcome := row.Outcome.(type) {
		case *agentpb.ServiceObservationRow_Replicas:
			if outcome == nil || outcome.Replicas == nil || len(outcome.Replicas.ProtoReflect().GetUnknown()) != 0 {
				return invalid()
			}
			total += Total(outcome.Replicas)
			if !validProxyState(target, row.ProxyState, false) {
				return invalid()
			}
		case *agentpb.ServiceObservationRow_Unavailable:
			if outcome == nil || !outcome.Unavailable || !validProxyState(target, row.ProxyState, true) {
				return invalid()
			}
		default:
			return invalid()
		}
	}
	if total > MaximumContainers {
		return invalid()
	}
	return nil
}

func validProxyState(
	target *agentpb.ServiceObservationTarget,
	state agentpb.ServiceProxyObservationState,
	unavailable bool,
) bool {
	if unavailable || target.ProxyComposeName == "" {
		return state == agentpb.ServiceProxyObservationState_SERVICE_PROXY_OBSERVATION_STATE_UNSPECIFIED
	}
	switch state {
	case agentpb.ServiceProxyObservationState_SERVICE_PROXY_OBSERVATION_STATE_MATCHING,
		agentpb.ServiceProxyObservationState_SERVICE_PROXY_OBSERVATION_STATE_MISSING,
		agentpb.ServiceProxyObservationState_SERVICE_PROXY_OBSERVATION_STATE_STOPPED,
		agentpb.ServiceProxyObservationState_SERVICE_PROXY_OBSERVATION_STATE_CONFIG_MISMATCH:
		return true
	default:
		return false
	}
}

func ValidateCancel(request *agentpb.CancelServiceObservation) error {
	if request == nil || len(request.ProtoReflect().GetUnknown()) != 0 ||
		ids.Validate(ids.KindOperation, "op_"+request.RequestId) != nil {
		return invalid()
	}
	return nil
}

func composeNameValid(value string) bool {
	if len(value) == 0 || len(value) > 255 {
		return false
	}
	for index, char := range value {
		if char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char >= '0' && char <= '9' {
			continue
		}
		if index == 0 || !strings.ContainsRune("_.-", char) {
			return false
		}
	}
	return true
}

func invalid() error {
	return errs.New(errs.KindValidationFailed, "invalid Service observation exchange")
}
