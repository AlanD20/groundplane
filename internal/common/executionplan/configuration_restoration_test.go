package executionplan

import (
	"bytes"
	"crypto/sha256"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// SVC-15/JOURNEY-02: a recovery file must be sealed to the same destination
// the Task wrote, with an exact prior snapshot and matching probe/restore bytes.
func TestConfigurationRestorationPlanBindsOnlyWrittenDestinations(t *testing.T) {
	t.Parallel()
	plan, _ := configurationRestorationPlanFixture(t)
	sealed, err := Seal(plan)
	if err != nil {
		t.Fatal(err)
	}
	sealed.CandidateReleaseProcedure.ConfigurationRestoration.PriorSnapshotSha256[0] ^= 1
	if _, err := Validate(sealed); err == nil {
		t.Fatal("source snapshot changed without changing plan authority")
	}
	for _, scenario := range []string{"unbound", "foreign destination", "different metadata", "forward probe", "overlap", "invented initial file", "duplicate forward destination"} {
		t.Run(scenario, func(t *testing.T) {
			plan, steps := configurationRestorationPlanFixture(t)
			binding := plan.CandidateReleaseProcedure.ConfigurationRestoration
			switch scenario {
			case "unbound":
				plan.CandidateReleaseProcedure.ConfigurationRestoration = nil
			case "foreign destination":
				steps[1].GetMaterializeFile().Destination = "config/unrelated"
				steps[2].GetMaterializeFile().Destination = "config/unrelated"
			case "different metadata":
				steps[2].GetMaterializeFile().Uid++
			case "forward probe":
				steps[1].Policy = agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_FORWARD
			case "overlap":
				binding.Files[0].CompensateStepId = binding.Files[0].ForwardStepId
			case "invented initial file":
				binding.PriorSnapshotId, binding.PriorSnapshotSha256 = "", nil
			case "duplicate forward destination":
				duplicate := proto.CloneOf(steps[0])
				duplicate.StepId, duplicate.GetMaterializeFile().MaterializationId = ids.New(
					ids.KindStep,
				), ids.New(
					ids.KindConfig,
				)
				plan.Steps = append(plan.Steps, duplicate)
			}
			if _, err := Seal(plan); err == nil {
				t.Fatal("invalid recovery file authority accepted")
			}
		})
	}
}

func configurationRestorationPlanFixture(t *testing.T) (*agentpb.ExecutionPlan, []*agentpb.ExecutionStep) {
	t.Helper()
	plan := validBlueprintScriptReconcilePlan(t)
	artifact := plan.Artifacts[0]
	digest := sha256.Sum256([]byte("next"))
	forward := &agentpb.ExecutionStep{
		StepId: ids.New(ids.KindStep), TimeoutSeconds: 30,
		Policy: agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_FORWARD,
		Payload: &agentpb.ExecutionStep_MaterializeFile{MaterializeFile: &agentpb.MaterializeFile{
			ArtifactId: artifact.ArtifactId, EnvironmentId: artifact.OwnerId, MaterializationId: ids.New(ids.KindConfig),
			Destination: "config/recovery", OutputKind: agentpb.MaterializationOutputKind_MATERIALIZATION_OUTPUT_KIND_PLAIN_FILE,
			Mode: 0o444, Length: 4, Sha256: digest[:],
		}},
	}
	probe := proto.CloneOf(forward)
	probe.StepId, probe.GetMaterializeFile().MaterializationId = ids.New(ids.KindStep), ids.New(ids.KindConfig)
	probe.Policy = agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_RECOVERY_PROBE
	before := sha256.Sum256([]byte("prior"))
	probe.GetMaterializeFile().Length, probe.GetMaterializeFile().Sha256 = 5, before[:]
	compensate := proto.CloneOf(probe)
	compensate.StepId, compensate.GetMaterializeFile().MaterializationId = ids.New(
		ids.KindStep,
	), ids.New(
		ids.KindConfig,
	)
	compensate.Policy, compensate.PrerequisiteStepId = agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_COMPENSATE, forward.StepId
	plan.CandidateReleaseProcedure.ConfigurationRestoration = &agentpb.ConfigurationRestoration{
		PriorSnapshotId: ids.New(ids.KindConfig), PriorSnapshotSha256: bytes.Repeat([]byte{0x41}, 32),
		Files: []*agentpb.ConfigurationFileRestoration{
			{ForwardStepId: forward.StepId, ProbeStepId: probe.StepId, CompensateStepId: compensate.StepId},
		},
	}
	plan.Steps = append([]*agentpb.ExecutionStep{forward}, plan.Steps...)
	plan.Steps = append(plan.Steps, probe, compensate)
	return plan, []*agentpb.ExecutionStep{forward, probe, compensate}
}
