// Package serviceobservation owns the closed, workload-only Agent read exchange.
package serviceobservation

import (
	"crypto/sha256"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/internal/common/databaseversion"
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
		if ValidateTarget(target) != nil || target.EnvironmentId != request.Targets[0].EnvironmentId ||
			seen[target.ServiceId] || seenNames[target.ComposeName] {
			return invalid()
		}
		seen[target.ServiceId], seenNames[target.ComposeName] = true, true
	}
	return nil
}

// ValidateTarget is shared by observation and fixed-source log requests.
func ValidateTarget(target *agentpb.ServiceObservationTarget) error {
	if target == nil || len(target.ProtoReflect().GetUnknown()) != 0 ||
		ids.Validate(ids.KindEnvironment, target.EnvironmentId) != nil ||
		ids.Validate(ids.KindService, target.ServiceId) != nil ||
		ids.Validate(
			ids.KindPlan,
			target.PlanId,
		) != nil || target.RenderGeneration == 0 || !composeNameValid(target.ComposeName) {
		return invalid()
	}
	if probe := target.DatabaseProbe; probe != nil {
		if len(probe.ProtoReflect().GetUnknown()) != 0 || probe.Authority == nil ||
			len(probe.Authority.ProtoReflect().GetUnknown()) != 0 || probe.Authority.GetRestore() == nil ||
			len(probe.Services) == 0 || len(probe.Artifacts) == 0 || target.ProxyComposeName != "" {
			return invalid()
		}
		serviceID := probe.Authority.GetRestore().GetPostgres().GetDatabaseServiceId()
		if mysql := probe.Authority.GetRestore().GetMysql(); mysql != nil {
			serviceID = mysql.DatabaseServiceId
		}
		if serviceID != target.ServiceId {
			return invalid()
		}
	}
	if target.RuntimeRole == "backing" {
		if target.ReleaseId != "" || target.Slot != "" || target.ProxyComposeName != "" {
			return invalid()
		}
	} else if ids.Validate(ids.KindDeployment, target.ReleaseId) != nil ||
		target.RuntimeRole == "singleton" && target.Slot != "" ||
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
			if !validContainers(row.Containers, outcome.Replicas) {
				return invalid()
			}
			if !validProxyState(target, row.ProxyState, false) {
				return invalid()
			}
			if target.DatabaseProbe != nil {
				versions, err := databaseversion.FromWire(row.DatabaseVersions)
				if err != nil || len(row.Containers) != 1 || versions.ContainerID != row.Containers[0].Id {
					return invalid()
				}
			} else if row.DatabaseVersions != nil {
				return invalid()
			}
		case *agentpb.ServiceObservationRow_Unavailable:
			if outcome == nil || !outcome.Unavailable || len(row.Containers) != 0 || row.DatabaseVersions != nil || !validProxyState(target, row.ProxyState, true) {
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

func validContainers(containers []*agentpb.ServiceContainerObservation, counts *agentpb.ServiceReplicaCounts) bool {
	if uint64(len(containers)) != Total(counts) || len(containers) > MaximumContainers {
		return false
	}
	seenIDs, seenReplicas := make(map[string]bool), make(map[uint32]bool)
	actual := &agentpb.ServiceReplicaCounts{}
	for _, item := range containers {
		if item == nil || len(item.ProtoReflect().GetUnknown()) != 0 || item.Id == "" || len(item.Id) > 128 ||
			item.Name == "" || len(item.Name) > 255 || len(item.Image) > 1024 || item.Replica == 0 ||
			!utf8.ValidString(item.Id) || !utf8.ValidString(item.Name) || !utf8.ValidString(item.Image) ||
			seenIDs[item.Id] || seenReplicas[item.Replica] {
			return false
		}
		seenIDs[item.Id], seenReplicas[item.Replica] = true, true
		if item.Health != "none" && item.Health != "healthy" && item.Health != "starting" &&
			item.Health != "unhealthy" {
			return false
		}
		switch item.State {
		case "running":
			switch item.Health {
			case "none":
				actual.Running++
			case "healthy":
				actual.Healthy++
			case "starting":
				actual.Starting++
			case "unhealthy":
				actual.Unhealthy++
			}
		case "created", "paused", "restarting", "removing":
			actual.Transitional++
		case "exited", "dead":
			// Exit classification belongs to the Agent, which does not expose exit diagnostics.
			// Both bins share these Docker states; total and all other bins remain exact.
			actual.Failed++
		default:
			return false
		}
	}
	return actual.Running == counts.Running && actual.Healthy == counts.Healthy && actual.Starting == counts.Starting &&
		actual.Unhealthy == counts.Unhealthy && actual.Transitional == counts.Transitional && actual.Failed == counts.Failed+counts.Stopped
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
