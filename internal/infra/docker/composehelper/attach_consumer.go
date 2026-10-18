package composehelper

import (
	"context"
	"encoding/json"
	"slices"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/networkname"
	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type AttachConsumerObservation struct {
	NetworkID string
	ServiceID string
	Detached  bool
	Connected bool
	At        time.Time
}

// ApplyAttachConsumerDocker changes existing endpoints only. It never starts,
// recreates or stops a workload, and never removes the shared Backing network.
// The caller owns the acknowledged work boundary and separate file publication.
func ApplyAttachConsumerDocker(ctx context.Context, taskRunner runner.Runner, input *agentpb.AttachConsumerTransition) (AttachConsumerObservation, error) {
	if err := validateAttachConsumerExecution(ctx, taskRunner, input); err != nil {
		return AttachConsumerObservation{}, err
	}
	containers, err := inspectAttachConsumers(ctx, taskRunner, input)
	if err != nil {
		return AttachConsumerObservation{}, err
	}
	networkID, connected, err := inspectAttachBackingNetwork(ctx, taskRunner, input)
	if err != nil {
		return AttachConsumerObservation{}, err
	}
	for _, containerID := range containers {
		_, found := connected[containerID]
		connect := input.Action == agentpb.AttachNetworkAction_ATTACH_NETWORK_ACTION_CONNECT
		if found == connect {
			continue
		}
		action := "disconnect"
		if connect {
			action = "connect"
		}
		if _, err := runNetworkWork(ctx, taskRunner, "network", action, networkID, containerID); err != nil {
			return AttachConsumerObservation{}, err
		}
	}
	observation, err := ObserveAttachConsumerDocker(ctx, taskRunner, input)
	if err == nil && ((input.Action == agentpb.AttachNetworkAction_ATTACH_NETWORK_ACTION_CONNECT && !observation.Connected) || (input.Action == agentpb.AttachNetworkAction_ATTACH_NETWORK_ACTION_DISCONNECT && !observation.Detached)) {
		err = errs.New(errs.KindStateConflict, "Service backing-network membership does not match the requested action")
	}
	return observation, err
}

func ObserveAttachConsumerDocker(ctx context.Context, taskRunner runner.Runner, input *agentpb.AttachConsumerTransition) (AttachConsumerObservation, error) {
	if err := validateAttachConsumerExecution(ctx, taskRunner, input); err != nil {
		return AttachConsumerObservation{}, err
	}
	containers, err := inspectAttachConsumers(ctx, taskRunner, input)
	if err != nil {
		return AttachConsumerObservation{}, err
	}
	_, connected, err := inspectAttachBackingNetwork(ctx, taskRunner, input)
	if err != nil {
		return AttachConsumerObservation{}, err
	}
	detached, fullyConnected := true, true
	for _, containerID := range containers {
		if _, found := connected[containerID]; found {
			detached = false
		} else {
			fullyConnected = false
		}
	}
	return AttachConsumerObservation{NetworkID: input.NetworkId, ServiceID: input.ServiceId,
		Detached: detached, Connected: fullyConnected, At: time.Now().UTC()}, nil
}

func validateAttachConsumerExecution(ctx context.Context, taskRunner runner.Runner, input *agentpb.AttachConsumerTransition) error {
	if taskRunner == nil {
		return errs.New(errs.KindInternal, "Detach consumer executor is not configured")
	}
	if err := executionplan.ValidateAttachConsumerTransition(ctx, input); err != nil {
		return err
	}
	if _, bounded := ctx.Deadline(); !bounded {
		return errs.New(errs.KindValidationFailed, "Detach consumer execution requires a bounded deadline")
	}
	return nil
}

func inspectAttachConsumers(ctx context.Context, taskRunner runner.Runner, input *agentpb.AttachConsumerTransition) ([]string, error) {
	names, err := executionplan.AttachConsumerNames(input)
	if err != nil {
		return nil, err
	}
	var containers []string
	for _, name := range names {
		members, err := inspectCapturedWorkloads(ctx, taskRunner, input.Baseline, name)
		if err != nil {
			return nil, err
		}
		containers = append(containers, members...)
	}
	slices.Sort(containers)
	if len(containers) != len(slices.Compact(slices.Clone(containers))) {
		return nil, errs.New(errs.KindStateConflict, "Detach found duplicate workload containers")
	}
	// Discover by stable logical identity too. A workload created under a new
	// slot or Compose name must not be hidden by inspecting only old names.
	all, err := runNetworkWork(ctx, taskRunner, "container", "ls", "--all", "--quiet", "--no-trunc",
		"--filter", "label=com.groundplane.environment-id="+input.EnvironmentId,
		"--filter", "label=com.groundplane.service-id="+input.ServiceId)
	if err != nil {
		return nil, err
	}
	if len(all.Stdout) >= maximumComponentConfig {
		return nil, errs.New(errs.KindStateConflict, "Detach workload inventory exceeds its bounded response")
	}
	for _, id := range strings.Fields(string(all.Stdout)) {
		if !validOwnedNetworkID(id) {
			return nil, errs.New(errs.KindStateConflict, "Detach workload inventory contains an invalid container ID")
		}
		if _, found := slices.BinarySearch(containers, id); found {
			continue
		}
		if err := inspectAttachProxy(ctx, taskRunner, input, id); err != nil {
			return nil, err
		}
	}
	return containers, nil
}

func inspectAttachProxy(ctx context.Context, taskRunner runner.Runner, input *agentpb.AttachConsumerTransition, id string) error {
	inspected, err := runNetworkWork(ctx, taskRunner, "container", "inspect", "--format", "{{json .Config.Labels}}", id)
	if err != nil {
		return err
	}
	var labels map[string]string
	if json.Unmarshal(inspected.Stdout, &labels) != nil {
		return errs.New(errs.KindStateConflict, "Detach could not inspect an unexpected Service container")
	}
	for _, service := range input.Baseline.Services {
		if service != nil && service.ServiceId == input.ServiceId &&
			service.Role == agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY &&
			labels["com.docker.compose.service"] == service.ComposeName &&
			labels["com.docker.compose.project"] == input.Baseline.ProjectName &&
			labels["com.groundplane.managed"] == "true" &&
			labels["com.groundplane.environment-id"] == input.EnvironmentId &&
			labels["com.groundplane.service-id"] == input.ServiceId && hasAllExpectedLabels(labels, service.ExpectedLabels) {
			return inspectRestorationImage(ctx, taskRunner, id, service)
		}
	}
	return errs.New(errs.KindStateConflict, "Service containers changed after Detach was accepted; inspect the current deployment")
}

func inspectAttachBackingNetwork(ctx context.Context, taskRunner runner.Runner, input *agentpb.AttachConsumerTransition) (string, map[string]struct{}, error) {
	name, err := networkname.New(input.NetworkId)
	if err != nil {
		return "", nil, err
	}
	listed, err := runNetworkWork(ctx, taskRunner, "network", "ls", "--filter", "name=^"+name+"$", "--format", "{{.ID}} {{.Name}}", "--no-trunc")
	if err != nil {
		return "", nil, err
	}
	identity := strings.Fields(string(listed.Stdout))
	if len(identity) == 0 {
		return "", nil, nil
	}
	if len(identity) != 2 || identity[1] != name || !validOwnedNetworkID(identity[0]) {
		return "", nil, errs.New(errs.KindStateConflict, "Backing network identity is ambiguous")
	}
	inspected, err := runNetworkWork(ctx, taskRunner, "network", "inspect", "--format", "{{json .}}", identity[0])
	if err != nil {
		return "", nil, err
	}
	var network struct {
		ID         string              `json:"Id"`
		Name       string              `json:"Name"`
		Labels     map[string]string   `json:"Labels"`
		Containers map[string]struct{} `json:"Containers"`
	}
	if json.Unmarshal(inspected.Stdout, &network) != nil || network.ID != identity[0] || network.Name != name ||
		network.Labels["com.groundplane.managed"] != "true" || network.Labels["com.groundplane.kind"] != "network" ||
		network.Labels["com.groundplane.environment-id"] != input.BackingEnvironmentId {
		return "", nil, errs.New(errs.KindStateConflict, "Backing network ownership changed after Detach was accepted")
	}
	return network.ID, network.Containers, nil
}
