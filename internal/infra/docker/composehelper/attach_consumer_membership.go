package composehelper

import (
	"context"
	"encoding/json"
	"github.com/AlanD20/groundplane/internal/common/networkname"
	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Docker's network inventory lists active endpoints only. A stopped container
// retains configured membership in its own network settings; connecting it
// again or treating that membership as absent gives a false execution result.
func inspectStoppedAttachMembership(ctx context.Context, taskRunner runner.Runner,
	input *agentpb.AttachConsumerTransition, networkID string, containers []string, connected map[string]struct{},
) error {
	if networkID == "" {
		return nil
	}
	name, err := networkname.New(input.NetworkId)
	if err != nil {
		return err
	}
	for _, id := range containers {
		if _, active := connected[id]; active {
			continue
		}
		inspected, err := runNetworkWork(
			ctx,
			taskRunner,
			"container",
			"inspect",
			"--format",
			`{"Id":{{json .Id}},"State":{"Running":{{json .State.Running}}},"NetworkSettings":{"Networks":{{json .NetworkSettings.Networks}}}}`,
			id,
		)
		if err != nil {
			return err
		}
		var container struct {
			ID    string `json:"Id"`
			State struct {
				Running bool `json:"Running"`
			} `json:"State"`
			NetworkSettings struct {
				Networks map[string]struct {
					NetworkID string `json:"NetworkID"`
				} `json:"Networks"`
			} `json:"NetworkSettings"`
		}
		if json.Unmarshal(inspected.Stdout, &container) != nil || container.ID != id {
			return errs.New(errs.KindStateConflict, "Attach container network settings are unavailable")
		}
		if container.State.Running {
			continue
		}
		if membership, exists := container.NetworkSettings.Networks[name]; exists &&
			(membership.NetworkID == networkID || membership.NetworkID == "") {
			connected[id] = struct{}{}
		}
	}
	return nil
}
