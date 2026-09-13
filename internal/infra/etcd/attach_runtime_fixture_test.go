package etcd

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func nativeAttachRuntimeFixture(t *testing.T, environmentID, serviceID string) serviceruntimerecord.Record {
	t.Helper()
	planID, releaseID := ids.New(ids.KindPlan), ids.New(ids.KindDeployment)
	image := "sha256:" + strings.Repeat("a", 64)
	labels := []*agentpb.LabelPair{
		{Key: "com.groundplane.environment-id", Value: environmentID},
		{Key: "com.groundplane.kind", Value: "service"},
		{Key: "com.groundplane.managed", Value: "true"},
		{Key: "com.groundplane.plan-id", Value: planID},
		{Key: "com.groundplane.release-id", Value: releaseID},
		{Key: "com.groundplane.render-generation", Value: "1"},
		{Key: "com.groundplane.runtime-role", Value: "singleton"},
		{Key: "com.groundplane.service-id", Value: serviceID},
	}
	yaml := "services:\n  consumer--singleton:\n    image: " + image + "\n    labels:\n"
	for _, label := range labels {
		yaml += "      " + label.Key + ": '" + label.Value + "'\n"
	}
	yaml += "    networks: [old-backing]\nnetworks:\n  old-backing:\n    external: true\n"
	artifact := &agentpb.ComposeArtifact{
		ArtifactId: ids.New(ids.KindConfig), OwnerId: environmentID,
		OwnerKind: agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT,
		ProjectName: "gp-" + strings.ToLower(
			environmentID,
		), AuthorizedVolumeDir: "/var/lib/groundplane/vol/" + environmentID,
		Services: []*agentpb.ComposeService{{ServiceId: serviceID, ComposeName: "consumer--singleton",
			Role:             agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_RECREATE_SINGLETON,
			ExpectedReplicas: 1, HasHealthcheck: true, ImageReference: image, ExpectedLabels: labels}},
	}
	setAttachFixtureYAML(artifact, yaml)
	record := serviceruntimerecord.Record{EnvironmentID: environmentID,
		Runtime: executionplan.CandidateRuntime{ServiceID: serviceID, ReleaseID: releaseID, Target: "singleton",
			CurrentArtifact: marshalAttachFixtureArtifact(t, artifact)},
		Source: serviceruntimerecord.Acknowledgement{
			TaskID: ids.New(ids.KindTask), PlanID: planID, PlanHash: strings.Repeat("a", 64),
			StepID: ids.New(ids.KindStep), AgentID: ids.New(ids.KindAgent), AssignmentID: ids.New(ids.KindAssignment),
			ExecutionEpoch: 1, RenderGeneration: 1, EffectDigest: strings.Repeat("b", 64), AcknowledgedAt: testAttachTime,
		}}
	if err := serviceruntimerecord.Validate(record); err != nil {
		t.Fatal(err)
	}
	return record
}

func setAttachFixtureYAML(artifact *agentpb.ComposeArtifact, value string) {
	artifact.CanonicalYaml = []byte(value)
	digest := sha256.Sum256(artifact.CanonicalYaml)
	artifact.YamlSha256 = digest[:]
}

func marshalAttachFixtureArtifact(t *testing.T, artifact *agentpb.ComposeArtifact) []byte {
	t.Helper()
	value, err := (proto.MarshalOptions{Deterministic: true}).Marshal(artifact)
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func nativeAttachFixturePlan(t *testing.T, record serviceruntimerecord.Record, task TaskRecord,
	artifactID, network string, running bool) *agentpb.ExecutionPlan {
	t.Helper()
	artifact := &agentpb.ComposeArtifact{}
	if err := proto.Unmarshal(record.Runtime.CurrentArtifact, artifact); err != nil {
		t.Fatal(err)
	}
	artifact.ArtifactId = artifactID
	// This two-step fixture starts on old-backing and acknowledges new-backing
	// before Detach selects its explicitly supplied remaining network.
	replace := strings.NewReplacer("old-backing", network, "new-backing", network)
	setAttachFixtureYAML(artifact, replace.Replace(string(artifact.CanonicalYaml)))
	operation := agentpb.PlanOperation_PLAN_OPERATION_ATTACH
	if task.Type == TaskDetach {
		operation = agentpb.PlanOperation_PLAN_OPERATION_DETACH
	}
	var selected []string
	if running {
		selected = []string{record.Runtime.ServiceID}
	}
	plan, err := executionplan.Seal(&agentpb.ExecutionPlan{Schema: 1, PlanId: task.PlanID,
		RenderGeneration: uint64(task.RenderGeneration), Operation: operation, TargetId: task.Target,
		Artifacts: []*agentpb.ComposeArtifact{artifact}, Steps: []*agentpb.ExecutionStep{{StepId: task.Steps[0].ID,
			TimeoutSeconds: 120, Payload: &agentpb.ExecutionStep_ComposeApply{ComposeApply: &agentpb.ComposeApply{
				ArtifactId: artifactID, ServiceIds: selected, NoDependencies: true}}}}})
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

func putNativeAttachRuntime(t *testing.T, store Store, record serviceruntimerecord.Record) int64 {
	t.Helper()
	value, err := encodeReleaseRecord("service-acknowledged-runtime", record)
	if err != nil {
		t.Fatal(err)
	}
	result, err := store.Transact(t.Context(), nil, []Mutation{{Type: MutationPut,
		Key: serviceruntimerecord.Key(record.Runtime.ServiceID), Value: value}})
	if err != nil || !result.Succeeded {
		t.Fatalf("seed runtime: %v", err)
	}
	return result.Revision
}

func prepareNativeAttachEnvelope(t *testing.T, repository *AttachRepository, record serviceruntimerecord.Record,
	input AttachTaskRenderInput, task TaskRecord, network string) (AttachTaskRenderInput, TaskRecord) {
	t.Helper()
	plan := nativeAttachFixturePlan(t, record, task, input.ArtifactID, network, true)
	prepared, err := repository.PrepareAttachRuntime(t.Context(), plan)
	if err != nil {
		t.Fatal(err)
	}
	task.PlanHash = hex.EncodeToString(plan.PlanHash)
	input.RunningServiceIDs, input.RuntimePreparation = []string{record.Runtime.ServiceID}, &prepared
	return input, task
}
