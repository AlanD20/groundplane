package controller

import (
	"maps"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

const blueprintNetworkPrepareStepsParam = "blueprint_network_prepare_steps"

// RenderCompose lists only owned Networks in artifact.Networks. External
// networks remain references in canonical Compose and are never ensured here.
func prepareBlueprintReleaseNetworks(
	task etcd.TaskRecord,
	artifact *agentpb.ComposeArtifact,
	prerequisite string,
) (etcd.TaskRecord, []*agentpb.ExecutionStep, error) {
	planTime, err := ids.Timestamp(ids.KindPlan, task.PlanID)
	if err != nil {
		return etcd.TaskRecord{}, nil, err
	}
	stepIDs := make([]string, len(artifact.GetNetworks()))
	steps := make([]*agentpb.ExecutionStep, len(stepIDs))
	for index, network := range artifact.GetNetworks() {
		stepID := ids.DeriveAt(ids.KindStep, planTime, task.PlanID, "blueprint-network-ensure:"+network.GetNetworkId())
		stepIDs[index] = stepID
		steps[index] = &agentpb.ExecutionStep{
			StepId: stepID, PrerequisiteStepId: prerequisite, TimeoutSeconds: uint32(task.TimeoutSeconds),
			Policy: agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_FORWARD,
			Payload: &agentpb.ExecutionStep_ManagedNetworkEnsure{
				ManagedNetworkEnsure: &agentpb.ManagedNetworkEnsure{
					ArtifactId: artifact.GetArtifactId(),
					NetworkId:  network.GetNetworkId(),
				},
			},
		}
		prerequisite = stepID
	}
	encoded := strings.Join(stepIDs, ",")
	if stored, exists := task.Params[blueprintNetworkPrepareStepsParam]; exists && (stored == "" || stored != encoded) {
		return etcd.TaskRecord{}, nil, errs.New(
			errs.KindInternal,
			"Blueprint network preparation differs from its published authority",
		)
	}
	if encoded != "" {
		task.Params = maps.Clone(task.Params)
		task.Params[blueprintNetworkPrepareStepsParam] = encoded
	}
	return task, steps, nil
}

func blueprintReleaseNetworkStepIDs(task etcd.TaskRecord) ([]string, error) {
	encoded, exists := task.Params[blueprintNetworkPrepareStepsParam]
	if !exists {
		return nil, nil
	}
	stepIDs := strings.Split(encoded, ",")
	seen := make(map[string]bool, len(stepIDs))
	for _, stepID := range stepIDs {
		if ids.Validate(ids.KindStep, stepID) != nil || seen[stepID] {
			return nil, errs.New(errs.KindInternal, "durable Blueprint network preparation identities are invalid")
		}
		seen[stepID] = true
	}
	return stepIDs, nil
}
