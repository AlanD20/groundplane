package executionplan

import (
	"bytes"
	"strconv"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func validateConfigurationRestorationDescriptor(
	operation agentpb.PlanOperation, procedure *agentpb.CandidateReleaseProcedure,
) error {
	configuration := procedure.GetConfigurationRestoration()
	if configuration == nil {
		return nil
	}
	if operation != agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY ||
		len(configuration.GetFiles()) == 0 || len(configuration.GetFiles()) > 512 {
		return invalidConfigurationRestoration("unsupported scope or file count")
	}
	if configuration.PriorSnapshotId == "" {
		if len(configuration.PriorSnapshotSha256) != 0 {
			return invalidConfigurationRestoration("incomplete prior absence")
		}
	} else if ids.Validate(ids.KindConfig, configuration.PriorSnapshotId) != nil || len(configuration.PriorSnapshotSha256) != 32 {
		return invalidConfigurationRestoration("invalid prior snapshot identity")
	}
	seen := make(map[string]bool)
	for _, member := range procedure.GetMembers() {
		for _, id := range member.GetForwardStepIds() {
			seen[id] = true
		}
		probe, compensate := restorationStepIDs(member.GetServingPredecessor(), member.GetCandidateAbsence())
		seen[probe], seen[compensate] = true, true
	}
	previous := ""
	for _, file := range configuration.GetFiles() {
		if file == nil || file.GetForwardStepId() <= previous {
			return invalidConfigurationRestoration("files are not canonical")
		}
		previous = file.GetForwardStepId()
		for _, id := range []string{file.GetForwardStepId(), file.GetProbeStepId(), file.GetCompensateStepId()} {
			if ids.Validate(ids.KindStep, id) != nil || seen[id] {
				return invalidConfigurationRestoration("overlapping or invalid step identity")
			}
			seen[id] = true
		}
	}
	return nil
}

func validateConfigurationRestorationPlan(plan *agentpb.ExecutionPlan) error {
	procedure := plan.GetCandidateReleaseProcedure()
	if err := validateConfigurationRestorationDescriptor(plan.GetOperation(), procedure); err != nil {
		return err
	}
	steps := make(map[string][]*agentpb.ExecutionStep)
	for _, step := range plan.GetSteps() {
		steps[step.GetStepId()] = append(steps[step.GetStepId()], step)
	}
	referenced := make(map[string]bool)
	for _, binding := range procedure.GetConfigurationRestoration().GetFiles() {
		forward, probe, compensate := steps[binding.ForwardStepId], steps[binding.ProbeStepId], steps[binding.CompensateStepId]
		if len(forward) != 1 || len(probe) != 1 || len(compensate) != 1 {
			return invalidConfigurationRestoration("missing or duplicated step")
		}
		write, inspect, restore := forward[0].GetMaterializeFile(), probe[0].GetMaterializeFile(), compensate[0].GetMaterializeFile()
		if write == nil || inspect == nil || restore == nil ||
			forward[0].GetPolicy() != agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_FORWARD ||
			probe[0].GetPolicy() != agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_RECOVERY_PROBE ||
			compensate[0].GetPolicy() != agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_COMPENSATE ||
			probe[0].GetPrerequisiteStepId() != "" || compensate[0].GetPrerequisiteStepId() != binding.ForwardStepId {
			return invalidConfigurationRestoration("invalid file procedure")
		}
		if write.ArtifactId != restore.ArtifactId || write.EnvironmentId != restore.EnvironmentId ||
			write.Destination != restore.Destination || !sameRestorationFile(inspect, restore) {
			return invalidConfigurationRestoration("file procedure escapes its forward destination")
		}
		if procedure.GetConfigurationRestoration().GetPriorSnapshotId() == "" &&
			!removalOutput(restore.GetOutputKind()) {
			return invalidConfigurationRestoration("initial absence authorizes file creation")
		}
		referenced[binding.ProbeStepId], referenced[binding.CompensateStepId] = true, true
	}
	for _, step := range plan.GetSteps() {
		if step.GetMaterializeFile() == nil {
			continue
		}
		switch step.GetPolicy() {
		case agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_RECOVERY_PROBE,
			agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_COMPENSATE:
			if !referenced[step.GetStepId()] {
				return invalidConfigurationRestoration("unbound recovery file")
			}
		}
	}
	return nil
}

func sameRestorationFile(left, right *agentpb.MaterializeFile) bool {
	cloned := proto.CloneOf(left)
	cloned.MaterializationId = right.GetMaterializationId()
	return proto.Equal(cloned, right)
}

func removalOutput(kind agentpb.MaterializationOutputKind) bool {
	return kind == agentpb.MaterializationOutputKind_MATERIALIZATION_OUTPUT_KIND_REMOVE_GENERATED_ENV ||
		kind == agentpb.MaterializationOutputKind_MATERIALIZATION_OUTPUT_KIND_REMOVE_PLAIN_FILE ||
		kind == agentpb.MaterializationOutputKind_MATERIALIZATION_OUTPUT_KIND_REMOVE_SECRET_FILE
}

func invalidConfigurationRestoration(reason string) error {
	return errs.New(errs.KindValidationFailed, "configuration restoration: "+reason)
}

// ConfigurationFilePair returns the sealed file procedure owning this step.
// Callers must first validate the complete plan and assignment authority.
func ConfigurationFilePair(plan *agentpb.ExecutionPlan, stepID string) *agentpb.ConfigurationFileRestoration {
	for _, file := range plan.GetCandidateReleaseProcedure().GetConfigurationRestoration().GetFiles() {
		if file.GetProbeStepId() == stepID || file.GetCompensateStepId() == stepID {
			return proto.CloneOf(file)
		}
	}
	return nil
}

// ConfigurationSnapshotMatches binds the retained source identity without
// consulting current desired state or the host.
func ConfigurationSnapshotMatches(configuration *agentpb.ConfigurationRestoration, id string, digest []byte) bool {
	return configuration != nil && configuration.GetPriorSnapshotId() == id &&
		bytes.Equal(configuration.GetPriorSnapshotSha256(), digest)
}

func materializationDestinationKey(step *agentpb.ExecutionStep) string {
	file := step.GetMaterializeFile()
	key := file.GetArtifactId() + "\x00" + file.GetDestination()
	if step.GetPolicy() == agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_RECOVERY_PROBE ||
		step.GetPolicy() == agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_COMPENSATE {
		key += "\x00" + strconv.Itoa(int(step.GetPolicy()))
	}
	return key
}
