package etcd

import (
	"bytes"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// DirectReleaseNetwork is one captured bridge prerequisite. Data is the exact
// deterministic NetworkWorkInput encoding, not a command or an executable plan.
type DirectReleaseNetwork struct {
	WorkItemID string              `json:"work_item_id"`
	NetworkID  string              `json:"network_id"`
	Input      workexecution.Input `json:"input"`
	Data       []byte              `json:"data"`
}

func (network DirectReleaseNetwork) Open(environmentID string) (*agentpb.NetworkWorkInput, error) {
	if ids.Validate(ids.KindWorkItem, network.WorkItemID) != nil || ids.Validate(ids.KindNetwork, network.NetworkID) != nil ||
		network.Input.Validate() != nil || len(network.Data) == 0 || len(network.Data) > executionplan.MaximumPlanBytes {
		return nil, errs.New(errs.KindValidationFailed, "Direct Release network prerequisite is invalid")
	}
	input := &agentpb.NetworkWorkInput{}
	if proto.Unmarshal(network.Data, input) != nil {
		return nil, errs.New(errs.KindValidationFailed, "Direct Release network input cannot be decoded")
	}
	owned, raw, reference, err := executionplan.CaptureNetworkWorkInput(network.Input.ID, input)
	defer clear(raw)
	if err != nil {
		return nil, err
	}
	if reference != network.Input || !bytes.Equal(raw, network.Data) || owned.NetworkId != network.NetworkID ||
		owned.EnvironmentId != environmentID || owned.Action != agentpb.NetworkWorkAction_NETWORK_WORK_ACTION_ENSURE {
		return nil, errs.New(errs.KindStateConflict, "Direct Release network differs from its captured prerequisite")
	}
	return owned, nil
}

func (request DirectReleasePreparation) releaseDependencies() []string {
	if len(request.Networks) == 0 {
		return nil
	}
	return []string{request.Networks[len(request.Networks)-1].WorkItemID}
}

// WorkGraph preserves the previous ordered bridge preparation, then allows the
// selected Service release to prepare and run. One visible Task owns all items;
// chained dependencies avoid imposing a smaller limit than 256 DNS bridges.
func (request DirectReleasePreparation) WorkGraph(task workexecution.Task, release workexecution.Item) ([]workexecution.Item, error) {
	if err := request.validate(); err != nil {
		return nil, err
	}
	if task.ID != request.Configuration.TaskID || release.ID != request.Configuration.WorkItemID ||
		release.TaskID != task.ID || release.Operation != workexecution.Release ||
		release.TargetID != request.Intent.ServiceID || release.Placement != request.Configuration.Placement {
		return nil, errs.New(errs.KindStateConflict, "Direct Release graph belongs to another native action")
	}
	graph := make([]workexecution.Item, 0, len(request.Networks)+1)
	// Reconstruct accepted membership, not a mutable post-Retry deadline.
	// Assignment/recovery budgets are owned by the native attempt records.
	release.Deadline = task.Deadline
	previous := ""
	for _, network := range request.Networks {
		item := workexecution.Item{ID: network.WorkItemID, TaskID: task.ID, Operation: workexecution.Network,
			TargetID: network.NetworkID, Placement: release.Placement, Input: &network.Input,
			Phase: workexecution.Ready, Cleanup: workexecution.CleanupNone,
			CreatedAt: release.CreatedAt, UpdatedAt: release.CreatedAt, Deadline: release.Deadline,
			Claims: []workexecution.Claim{{ScopeID: task.Scope.EnvironmentID},
				{HostID: release.Placement.HostID, ScopeID: task.Scope.EnvironmentID},
				{HostID: release.Placement.HostID, ScopeID: network.NetworkID}}}
		if previous != "" {
			item.Dependencies = []string{previous}
		}
		graph = append(graph, item)
		previous = item.ID
	}
	release.Dependencies = request.releaseDependencies()
	graph = append(graph, release)
	if err := workexecution.ValidateGraph(task, graph); err != nil {
		return nil, err
	}
	return graph, nil
}
