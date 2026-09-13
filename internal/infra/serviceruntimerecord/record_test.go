package serviceruntimerecord

import (
	"crypto/sha256"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// Rationale: durable authority must survive Task pruning without trusting a
// mutable source and must reject identity/digest drift inside its copied bytes.
func TestAcknowledgeReleaseValidatesSelfContainedRuntime(t *testing.T) {
	t.Parallel()
	for name, mutate := range map[string]func(*Record, *Observation){
		"valid":                  func(*Record, *Observation) {},
		"wrong release":          func(r *Record, _ *Observation) { r.Runtime.ReleaseID = "dep_01ARZ3NDEKTSV4RRFFQ69G5FAW" },
		"wrong artifact":         func(_ *Record, o *Observation) { o.RecreateArtifactID = "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAW" },
		"wrong target":           func(_ *Record, o *Observation) { o.Target = "blue" },
		"missing strategy proof": func(_ *Record, o *Observation) { o.RecreateArtifactID = "" },
		"both strategy proofs":   func(_ *Record, o *Observation) { o.Proxy = &ProxyObservation{} },
		"missing epoch":          func(r *Record, _ *Observation) { r.Source.ExecutionEpoch = 0 },
		"missing generation":     func(r *Record, _ *Observation) { r.Source.RenderGeneration = 0 },
		"invalid hash":           func(r *Record, _ *Observation) { r.Source.PlanHash = "unsealed" },
		"invalid Task":           func(r *Record, _ *Observation) { r.Source.TaskID = "" },
		"absent source":          func(r *Record, _ *Observation) { r.Runtime.CurrentArtifact = nil },
		"foreign owner":          func(r *Record, _ *Observation) { r.EnvironmentID = "env_01ARZ3NDEKTSV4RRFFQ69G5FAW" },
		"fabricated proxy":       func(r *Record, _ *Observation) { r.Runtime.ProxyGeneration = 1 },
		"workload identity drift": func(r *Record, _ *Observation) {
			changeRecordArtifact(t, r, func(a *agentpb.ComposeArtifact) {
				a.Services[0].ExpectedLabels[0].Value = "dep_01ARZ3NDEKTSV4RRFFQ69G5FAW"
			})
		},
		"YAML digest drift": func(r *Record, _ *Observation) {
			changeRecordArtifact(t, r, func(a *agentpb.ComposeArtifact) { a.CanonicalYaml = []byte("changed") })
		},
	} {
		t.Run(name, func(t *testing.T) {
			record, observation := singletonRecordFixture(t)
			mutate(&record, &observation)
			got, err := AcknowledgeRelease(record.EnvironmentID, record.Runtime, record.Source, observation)
			if name == "valid" {
				if err != nil || Validate(got) != nil || got.Source != record.Source ||
					Key(got.Runtime.ServiceID) != "/v1/runtime/service-acknowledged-runtimes/"+got.Runtime.ServiceID {
					t.Fatalf("valid runtime acknowledgement failed: %v", err)
				}
			} else if err == nil {
				t.Fatal("invalid runtime acknowledgement accepted")
			}
		})
	}
}

func changeRecordArtifact(t *testing.T, record *Record, mutate func(*agentpb.ComposeArtifact)) {
	t.Helper()
	artifact := &agentpb.ComposeArtifact{}
	if err := proto.Unmarshal(record.Runtime.CurrentArtifact, artifact); err != nil {
		t.Fatal(err)
	}
	mutate(artifact)
	var err error
	record.Runtime.CurrentArtifact, err = (proto.MarshalOptions{Deterministic: true}).Marshal(artifact)
	if err != nil {
		t.Fatal(err)
	}
}

func singletonRecordFixture(t *testing.T) (Record, Observation) {
	t.Helper()
	const suffix = "01ARZ3NDEKTSV4RRFFQ69G5FAV"
	releaseID, serviceID, environmentID := "dep_"+suffix, "svc_"+suffix, "env_"+suffix
	yaml := []byte("services: {}\n")
	digest := sha256.Sum256(yaml)
	artifact := &agentpb.ComposeArtifact{
		ArtifactId: "cfg_" + suffix, OwnerId: environmentID, OwnerKind: agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT,
		ProjectName: "gp-" + strings.ToLower(environmentID), CanonicalYaml: yaml, YamlSha256: digest[:],
		Services: []*agentpb.ComposeService{
			{
				ServiceId:        serviceID,
				ComposeName:      "worker",
				Role:             agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_RECREATE_SINGLETON,
				ExpectedReplicas: 1,
				HasHealthcheck:   true,
				ImageReference:   "sha256:" + strings.Repeat("a", 64),
				ExpectedLabels: []*agentpb.LabelPair{
					{Key: "com.groundplane.release-id", Value: releaseID},
					{Key: "com.groundplane.runtime-role", Value: "singleton"},
				},
			},
		},
	}
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(artifact)
	if err != nil {
		t.Fatal(err)
	}
	return Record{EnvironmentID: environmentID,
		Runtime: executionplan.CandidateRuntime{
			ServiceID:       serviceID,
			ReleaseID:       releaseID,
			Target:          "singleton",
			CurrentArtifact: encoded,
		},
		Source: Acknowledgement{
			TaskID:           "task_" + suffix,
			PlanID:           "plan_" + suffix,
			StepID:           "step_" + suffix,
			AgentID:          "agt_" + suffix,
			AssignmentID:     "asgn_" + suffix,
			PlanHash:         strings.Repeat("a", 64),
			EffectDigest:     strings.Repeat("b", 64),
			ExecutionEpoch:   1,
			RenderGeneration: 1,
			AcknowledgedAt:   time.Date(2026, 9, 13, 12, 0, 0, 0, time.UTC),
		},
	}, Observation{ReleaseID: releaseID, Target: "singleton", RecreateArtifactID: artifact.ArtifactId}
}
