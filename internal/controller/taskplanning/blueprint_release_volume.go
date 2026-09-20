package taskplanning

import (
	"maps"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

const blueprintVolumePrepareStepsParam = "blueprint_volume_prepare_steps"

func blueprintReleaseResourceStepIDs(task etcd.TaskRecord) ([]string, int, error) {
	var result []string
	params := 0
	for _, read := range []func(etcd.TaskRecord) ([]string, error){blueprintReleaseNetworkStepIDs, blueprintReleaseVolumeStepIDs} {
		ids, err := read(task)
		if err != nil {
			return nil, 0, err
		}
		if len(ids) != 0 {
			params++
		}
		result = append(result, ids...)
	}
	return result, params, nil
}

// Preparation selects only the immutable artifact's managed Volume table.
// The filesystem directory prefix must complete before these Docker resources.
func prepareBlueprintReleaseVolumes(
	task etcd.TaskRecord,
	artifact *agentpb.ComposeArtifact,
	prerequisite string,
) (etcd.TaskRecord, []*agentpb.ExecutionStep, error) {
	planTime, err := ids.Timestamp(ids.KindPlan, task.PlanID)
	if err != nil {
		return etcd.TaskRecord{}, nil, err
	}
	stepIDs := make([]string, len(artifact.GetVolumes()))
	steps := make([]*agentpb.ExecutionStep, len(stepIDs))
	for index, volume := range artifact.GetVolumes() {
		stepID := ids.DeriveAt(ids.KindStep, planTime, task.PlanID, "blueprint-volume-ensure:"+volume.GetVolumeId())
		stepIDs[index] = stepID
		steps[index] = &agentpb.ExecutionStep{
			StepId: stepID, PrerequisiteStepId: prerequisite, TimeoutSeconds: uint32(task.TimeoutSeconds),
			Policy: agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_FORWARD,
			Payload: &agentpb.ExecutionStep_ManagedVolumeEnsure{
				ManagedVolumeEnsure: &agentpb.ManagedVolumeEnsure{
					ArtifactId: artifact.GetArtifactId(),
					VolumeId:   volume.GetVolumeId(),
				},
			},
		}
		prerequisite = stepID
	}
	encoded := strings.Join(stepIDs, ",")
	if stored, exists := task.Params[blueprintVolumePrepareStepsParam]; exists && (stored == "" || stored != encoded) {
		return etcd.TaskRecord{}, nil, errs.New(
			errs.KindInternal,
			"Blueprint volume preparation differs from its published authority",
		)
	}
	if encoded != "" {
		task.Params = maps.Clone(task.Params)
		task.Params[blueprintVolumePrepareStepsParam] = encoded
	}
	return task, steps, nil
}

func blueprintReleaseVolumeStepIDs(task etcd.TaskRecord) ([]string, error) {
	encoded, exists := task.Params[blueprintVolumePrepareStepsParam]
	if !exists {
		return nil, nil
	}
	stepIDs := strings.Split(encoded, ",")
	seen := make(map[string]bool, len(stepIDs))
	for _, stepID := range stepIDs {
		if ids.Validate(ids.KindStep, stepID) != nil || seen[stepID] {
			return nil, errs.New(errs.KindInternal, "durable Blueprint volume preparation identities are invalid")
		}
		seen[stepID] = true
	}
	return stepIDs, nil
}
