package composehelper

import (
	"context"
	"encoding/json"
	"slices"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Disconnect only captured same-Environment consumers. Discovery is bounded and
// every endpoint is checked before the first mutation. A new endpoint racing
// this snapshot makes Docker refuse bridge removal, not expand our authority.
func disconnectNetworkConsumers(
	ctx context.Context,
	taskRunner runner.Runner,
	input *agentpb.NetworkWorkInput,
	dockerID string,
) error {
	if len(input.DisconnectServiceIds) == 0 {
		return nil
	}
	result, err := runNetworkWork(ctx, taskRunner, "network", "inspect", "--format",
		"{{range $id, $_ := .Containers}}{{$id}} {{end}}", dockerID)
	if err != nil {
		return err
	}
	containers := strings.Fields(string(result.Stdout))
	if len(containers) > 4096 {
		return errs.New(errs.KindStateConflict, "Zone has too many endpoints for one bounded removal")
	}
	slices.Sort(containers)
	for index, containerID := range containers {
		if !validOwnedNetworkID(containerID) || index > 0 && containers[index-1] == containerID {
			return errs.New(errs.KindStateConflict, "Zone endpoint identity is invalid")
		}
		inspected, err := runNetworkWork(ctx, taskRunner, "container", "inspect", "--format", "{{json .}}", containerID)
		if err != nil {
			return err
		}
		var container struct {
			ID     string `json:"Id"`
			Config struct {
				Labels map[string]string `json:"Labels"`
			} `json:"Config"`
		}
		if json.Unmarshal(inspected.Stdout, &container) != nil || container.ID != containerID ||
			container.Config.Labels["com.groundplane.managed"] != "true" ||
			container.Config.Labels["com.groundplane.environment-id"] != input.EnvironmentId {
			return errs.New(errs.KindStateConflict, "Zone endpoint is not owned by the captured Environment")
		}
		if _, found := slices.BinarySearch(input.DisconnectServiceIds, container.Config.Labels["com.groundplane.service-id"]); !found {
			return errs.New(errs.KindStateConflict, "Zone endpoint belongs to a Service outside this removal")
		}
	}
	for _, containerID := range containers {
		if _, err := runNetworkWork(ctx, taskRunner, "network", "disconnect", dockerID, containerID); err != nil {
			return err
		}
	}
	return nil
}
