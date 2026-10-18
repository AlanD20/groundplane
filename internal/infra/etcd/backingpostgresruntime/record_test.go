package backingpostgresruntime

import (
	"crypto/sha256"
	"encoding/hex"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/backingprovision"
	"github.com/AlanD20/groundplane/internal/common/backingruntimefact"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/postgres16protocol"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// Rationale: a successful patch must advance Backup's database authority without
// replacing its data Volume or tools; a changed mount must not be acknowledged.
func TestPatchAdvancesOnlyDatabaseAuthority(t *testing.T) {
	const suffix = "01ARZ3NDEKTSV4RRFFQ69G5FAV"
	tools := "registry.example.test/postgres-tools@sha256:" + strings.Repeat("a", 64)
	directory, err := postgres16protocol.ToolsDirectory(tools)
	if err != nil {
		t.Fatal(err)
	}
	image := "postgres:16-alpine@sha256:" + strings.Repeat("b", 64)
	yaml := "services:\n  postgres:\n    image: " + image + "\n    volumes:\n" +
		"      - {type: volume, source: data, target: /var/lib/postgresql/data}\n" +
		"      - {type: bind, source: " + directory + ", target: /opt/groundplane/postgres16, read_only: true, bind: {create_host_path: false}}\n" +
		"volumes:\n  data: {name: gp-postgres-data, driver: local}\n"
	artifact := &agentpb.ComposeArtifact{ArtifactId: "cfg_" + suffix, OwnerId: "env_" + suffix,
		OwnerKind: agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT, ProjectName: "gp-" + strings.ToLower("env_"+suffix),
		Services: []*agentpb.ComposeService{{ServiceId: "svc_" + suffix, ComposeName: "postgres",
			ExpectedReplicas: 1, ImageReference: image, PostgresToolsImage: tools,
			ExpectedLabels: []*agentpb.LabelPair{
				{Key: "com.groundplane.managed", Value: "true"}, {Key: "com.groundplane.kind", Value: "service"},
				{Key: "com.groundplane.project-id", Value: "prj_" + suffix},
				{Key: "com.groundplane.environment-id", Value: "env_" + suffix},
				{Key: "com.groundplane.service-id", Value: "svc_" + suffix},
				{Key: "com.groundplane.plan-id", Value: "plan_" + suffix},
				{Key: "com.groundplane.render-generation", Value: "1"},
			}}}}
	encode := func(a *agentpb.ComposeArtifact, body string) []byte {
		t.Helper()
		for _, service := range a.Services {
			slices.SortFunc(
				service.ExpectedLabels,
				func(a, b *agentpb.LabelPair) int { return strings.Compare(a.Key, b.Key) },
			)
		}
		a.CanonicalYaml = []byte(body)
		digest := sha256.Sum256(a.CanonicalYaml)
		a.YamlSha256 = digest[:]
		value, err := (proto.MarshalOptions{Deterministic: true}).Marshal(a)
		if err != nil {
			t.Fatal(err)
		}
		return value
	}
	encoded := encode(artifact, yaml)
	digest := sha256.Sum256(encoded)
	now := time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)
	prior := Record{EnvironmentID: artifact.OwnerId, ServiceID: artifact.Services[0].ServiceId,
		Artifact: encoded, LocalImageID: "sha256:" + strings.Repeat("c", 64), ToolsImage: tools,
		CatalogSHA256: strings.Repeat("d", 64), Provisioning: &environmentprojection.BackingRuntimeReceipt{
			ServiceID: artifact.Services[0].ServiceId, ArtifactSHA256: hex.EncodeToString(digest[:]),
			LocalImageID: "sha256:" + strings.Repeat("c", 64), ManagedReleaseSHA256: strings.Repeat("d", 64),
			Adapter: "postgres", RenderGeneration: 1,
			AcknowledgedAt: now,
		}}
	fillNativeBackingReceipt(t, prior.Provisioning, artifact, now)
	if err := Validate(prior); err != nil {
		t.Fatal(err)
	}
	for _, changed := range []string{"none", "data", "tools"} {
		t.Run(changed, func(t *testing.T) {
			next := proto.Clone(artifact).(*agentpb.ComposeArtifact)
			workload := next.Services[0]
			workload.Role = agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_RECREATE_SINGLETON
			workload.ImageReference = "sha256:" + strings.Repeat("f", 64)
			workload.ExpectedLabels = append(workload.ExpectedLabels,
				&agentpb.LabelPair{Key: "com.groundplane.release-id", Value: "dep_" + suffix},
				&agentpb.LabelPair{Key: "com.groundplane.runtime-role", Value: "singleton"})
			body := strings.Replace(yaml, image, workload.ImageReference, 1)
			if changed == "data" {
				body = strings.Replace(body, "gp-postgres-data", "other-data", 1)
			}
			if changed == "tools" {
				workload.PostgresToolsImage = "registry.example.test/postgres-tools@sha256:" + strings.Repeat("f", 64)
			}
			runtime := serviceruntimerecord.Record{EnvironmentID: prior.EnvironmentID,
				Runtime: executionplan.CandidateRuntime{ServiceID: prior.ServiceID, ReleaseID: "dep_" + suffix,
					Target: "singleton", CurrentArtifact: encode(next, body)},
				Source: serviceruntimerecord.Acknowledgement{TaskID: "task_" + suffix,
					WorkItemID: "work_" + suffix, AttemptID: "try_" + suffix,
					Executor: workexecution.Identity{Executor: workexecution.Agent, HostID: "host_" + suffix,
						AgentID: "agt_" + suffix, Generation: 1},
					Input: workexecution.Input{ID: "plan_" + suffix, SHA256: strings.Repeat("e", 64)},
					Checkpoint: workexecution.Checkpoint{AttemptID: "try_" + suffix, StageID: "step_" + suffix,
						Sequence: 1, Evidence: workexecution.Input{ID: "plan_" + suffix, SHA256: strings.Repeat("f", 64)},
						RecordedAt: now}, RenderGeneration: 1, AcknowledgedAt: now}}
			_, admissionErr := ValidatePatchArtifact(prior, next)
			result, err := FromDeployment(prior, runtime)
			if changed != "none" {
				if err == nil || admissionErr == nil {
					t.Fatal("patch acknowledged changed data or tool authority")
				}
				return
			}
			if err != nil || admissionErr != nil {
				t.Fatalf("patch: %v; runtime validation: %v", err, serviceruntimerecord.Validate(runtime))
			}
			value, err := Encode(result)
			if err != nil {
				t.Fatal(err)
			}
			decoded, err := Decode(value)
			if err != nil || decoded.LocalImageID != workload.ImageReference || decoded.ToolsImage != tools ||
				decoded.CatalogSHA256 != prior.CatalogSHA256 || decoded.Provisioning != nil || decoded.Deployment == nil {
				t.Fatalf("database authority did not advance independently: %v", err)
			}
		})
	}
}

func fillNativeBackingReceipt(
	t *testing.T,
	receipt *environmentprojection.BackingRuntimeReceipt,
	artifact *agentpb.ComposeArtifact,
	at time.Time,
) {
	t.Helper()
	const suffix = "01ARZ3NDEKTSV4RRFFQ69G5FAV"
	started := at.Add(-time.Second)
	attempt := workexecution.Attempt{
		ID: "try_" + suffix, WorkItemID: "work_" + suffix, TaskID: "task_" + suffix, Number: 1,
		Executor: workexecution.Identity{Executor: workexecution.Agent, HostID: "host_" + suffix,
			AgentID: "agt_" + suffix, Generation: 1},
		Direction: workexecution.Forward, Input: workexecution.Input{ID: "plan_" + suffix, SHA256: strings.Repeat("e", 64)},
		AssignedAt: started, StartedAt: &started, Deadline: at.Add(time.Minute),
	}
	stageID, err := backingprovision.StageID(attempt.WorkItemID, backingprovision.VerifyRuntime)
	if err != nil {
		t.Fatal(err)
	}
	checkpoint := workexecution.Checkpoint{AttemptID: attempt.ID, Sequence: 2, StageID: stageID,
		Evidence: attempt.Input, RecordedAt: at}
	workload := artifact.Services[0]
	observation := backingruntimefact.Observation{
		ProjectName: artifact.ProjectName, ContainerID: strings.Repeat("a", 64), ObservedAt: at,
		ServiceID: workload.ServiceId, ImageReference: workload.ImageReference, LocalImageID: receipt.LocalImageID,
		State:  agentpb.ObservedContainerState_OBSERVED_CONTAINER_STATE_RUNNING,
		Health: agentpb.ObservedContainerHealth_OBSERVED_CONTAINER_HEALTH_NONE,
		Labels: proto.CloneOf(workload).ExpectedLabels,
	}
	result, err := backingprovision.CompletionEvidence(attempt, checkpoint, observation)
	if err != nil {
		t.Fatal(err)
	}
	attempt.FinishedAt, attempt.Outcome, attempt.Result = &at, workexecution.Succeeded, &result
	receipt.Attempt, receipt.Checkpoint, receipt.Observation = attempt, checkpoint, observation
}
