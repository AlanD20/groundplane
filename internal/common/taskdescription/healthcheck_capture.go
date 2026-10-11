package taskdescription

import (
	"net/netip"
	"net/url"
	"sort"
	"strconv"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"gopkg.in/yaml.v3"
)

type healthcheckDocument struct {
	Services map[string]struct {
		Healthcheck *struct {
			Test          []string `yaml:"test"`
			Disable       bool     `yaml:"disable"`
			Interval      string   `yaml:"interval"`
			Timeout       string   `yaml:"timeout"`
			StartPeriod   string   `yaml:"start_period"`
			StartInterval string   `yaml:"start_interval"`
			Retries       *uint32  `yaml:"retries"`
		} `yaml:"healthcheck"`
	} `yaml:"services"`
}

// CaptureHealthchecks uses only the assigned artifact and the same physical
// workload selection as the health-wait executor. It never reads live desired
// state. A projection failure is explicitly unavailable, not a guessed check.
func CaptureHealthchecks(plan *agentpb.ExecutionPlan, step *agentpb.ExecutionStep) []ConfiguredHealthcheck {
	var artifactID, serviceID, target string
	var replicaOrdinal uint32
	var serviceIDs []string
	switch payload := step.GetPayload().(type) {
	case *agentpb.ExecutionStep_WaitHealthy:
		artifactID, serviceIDs = payload.WaitHealthy.GetArtifactId(), payload.WaitHealthy.GetServiceIds()
	case *agentpb.ExecutionStep_WaitWorkloadHealthy:
		artifactID, serviceID, target = payload.WaitWorkloadHealthy.GetArtifactId(), payload.WaitWorkloadHealthy.GetServiceId(), payload.WaitWorkloadHealthy.GetTarget()
		replicaOrdinal = payload.WaitWorkloadHealthy.GetRollingReplicaOrdinal()
	default:
		return nil
	}
	var artifact *agentpb.ComposeArtifact
	for _, candidate := range plan.GetArtifacts() {
		if candidate.GetArtifactId() == artifactID {
			artifact = candidate
			break
		}
	}
	if artifact == nil {
		return nil // A missing artifact is rejected by execution-plan validation.
	}
	var workloadName string
	if serviceID != "" {
		workload, err := executionplan.ReleaseWorkloadService(artifact, serviceID, target, replicaOrdinal)
		if err != nil {
			return nil // Execution-plan validation owns missing or ambiguous selectors.
		}
		workloadName = workload.GetComposeName()
	}
	var document healthcheckDocument
	parsed := yaml.Unmarshal(artifact.GetCanonicalYaml(), &document) == nil
	selected := make(map[string]bool, len(serviceIDs))
	for _, id := range serviceIDs {
		selected[id] = true
	}
	var result []ConfiguredHealthcheck
	for _, member := range artifact.GetServices() {
		if serviceID != "" {
			if member.GetServiceId() != serviceID || member.GetComposeName() != workloadName {
				continue
			}
		} else if len(selected) > 0 && (!selected[member.GetServiceId()] ||
			member.GetRole() == agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY) {
			continue
		}
		value := ConfiguredHealthcheck{ServiceID: member.GetServiceId(), Workload: member.GetComposeName(),
			ExpectedReplicas: member.GetExpectedReplicas(), RequiresHealthy: member.GetHasHealthcheck(), Kind: "unavailable"}
		if service, exists := document.Services[member.GetComposeName()]; parsed && exists {
			health := service.Healthcheck
			if health == nil {
				value.Kind = "image_default"
			} else {
				value.Kind, value.LoopbackPort, value.HTTPProtocol = describeHealthcheckTest(health.Test, health.Disable)
				value.Interval = canonicalHealthcheckDuration(health.Interval)
				value.Timeout = canonicalHealthcheckDuration(health.Timeout)
				value.StartPeriod = canonicalHealthcheckDuration(health.StartPeriod)
				value.StartInterval = canonicalHealthcheckDuration(health.StartInterval)
				value.Retries = health.Retries
			}
		}
		result = append(result, value)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Workload < result[j].Workload })
	return result
}

func describeHealthcheckTest(test []string, disabled bool) (kind string, port uint16, protocol string) {
	if disabled || len(test) > 0 && test[0] == "NONE" {
		return "disabled", 0, ""
	}
	if len(test) == 0 {
		return "image_default", 0, ""
	}
	if len(test) == 2 && test[0] == "CMD-SHELL" {
		return "custom_shell", 0, ""
	}
	if len(test) < 2 || test[0] != "CMD" {
		return "unavailable", 0, ""
	}
	if len(test) == 5 && test[1] == "pgrep" && test[2] == "-f" && test[3] == "--" {
		return "process", 0, ""
	}
	if len(test) == 6 && test[1] == "nc" && test[2] == "-z" && test[3] == "--" {
		if address, err := netip.ParseAddr(test[4]); err == nil && address.IsLoopback() {
			port = healthcheckPort(test[5])
		}
		return "tcp", port, ""
	}
	if len(test) == 5 && test[1] == "curl" && test[2] == "-fsS" && test[3] == "--" {
		target, err := url.Parse(test[4])
		if err == nil && target.User == nil && target.Fragment == "" &&
			(target.Scheme == "http" || target.Scheme == "https") {
			if address, err := netip.ParseAddr(target.Hostname()); err == nil && address.IsLoopback() {
				port = healthcheckPort(target.Port())
				if target.Port() == "" {
					port = 80
					if target.Scheme == "https" {
						port = 443
					}
				}
				if port != 0 {
					return "http", port, target.Scheme
				}
			}
		}
	}
	return "custom_exec", 0, ""
}

func healthcheckPort(value string) uint16 {
	port, err := strconv.ParseUint(value, 10, 16)
	if err != nil {
		return 0
	}
	return uint16(port)
}
