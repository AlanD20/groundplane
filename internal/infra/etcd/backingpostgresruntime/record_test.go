package backingpostgresruntime

import (
	"crypto/sha256"
	"encoding/hex"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/postgres16protocol"
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
			TaskID: "task_" + suffix, PlanID: "plan_" + suffix, PlanHash: strings.Repeat("e", 64),
			AgentID: "agt_" + suffix, AssignmentID: "asgn_" + suffix, ExecutionEpoch: 1, RenderGeneration: 1,
			AcknowledgedAt: now,
		}}
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
					PlanID: "plan_" + suffix, PlanHash: strings.Repeat("e", 64), StepID: "step_" + suffix,
					AgentID: "agt_" + suffix, AssignmentID: "asgn_" + suffix, ExecutionEpoch: 1,
					RenderGeneration: 1, EffectDigest: strings.Repeat("e", 64), AcknowledgedAt: now}}
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
