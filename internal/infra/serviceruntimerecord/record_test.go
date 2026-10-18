package serviceruntimerecord

import (
	"crypto/sha256"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// TASK-30: retained record validation, not actual Task pruning qualification.
// Rationale: durable authority must survive Task pruning without trusting a
// mutable source and must reject identity/digest drift inside its copied bytes.
// This validates retained authority only, not live completion or storage pruning.
func TestRetainedNativeRuntimeRejectsSourceAndArtifactDrift(t *testing.T) {
	t.Parallel()
	for name, mutate := range map[string]func(*Record){
		"valid":                            func(*Record) {},
		"wrong release":                    func(r *Record) { r.Runtime.ReleaseID = "dep_01ARZ3NDEKTSV4RRFFQ69G5FAW" },
		"wrong serving target":             func(r *Record) { r.Runtime.Target = "blue" },
		"checkpoint from another attempt":  func(r *Record) { r.Source.Checkpoint.AttemptID = ids.New(ids.KindAttempt) },
		"checkpoint after acknowledgement": func(r *Record) { r.Source.Checkpoint.RecordedAt = r.Source.AcknowledgedAt.Add(time.Second) },
		"absent captured artifact":         func(r *Record) { r.Runtime.CurrentArtifact = nil },
		"foreign environment":              func(r *Record) { r.EnvironmentID = "env_01ARZ3NDEKTSV4RRFFQ69G5FAW" },
		"fabricated proxy":                 func(r *Record) { r.Runtime.ProxyGeneration = 1 },
		"workload release drift": func(r *Record) {
			changeRecordArtifact(t, r, func(a *agentpb.ComposeArtifact) {
				a.Services[0].ExpectedLabels[0].Value = "dep_01ARZ3NDEKTSV4RRFFQ69G5FAW"
			})
		},
		"YAML digest drift": func(r *Record) {
			changeRecordArtifact(t, r, func(a *agentpb.ComposeArtifact) { a.CanonicalYaml = []byte("changed") })
		},
	} {
		t.Run(name, func(t *testing.T) {
			record := singletonRecordFixture(t)
			mutate(&record)
			err := Validate(record)
			if name == "valid" && err != nil {
				t.Fatalf("self-contained native runtime rejected: %v", err)
			}
			if name != "valid" && err == nil {
				t.Fatal("runtime authority drift was accepted")
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

func singletonRecordFixture(t *testing.T) Record {
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
			TaskID: "task_" + suffix, WorkItemID: "work_" + suffix, AttemptID: "try_" + suffix,
			Executor: workexecution.Identity{Executor: workexecution.Agent, AgentID: "agt_" + suffix,
				HostID: ids.NewAt(ids.KindHost, time.Date(2026, 9, 13, 12, 0, 0, 0, time.UTC), 1), Generation: 1},
			Input: workexecution.Input{ID: "plan_" + suffix, SHA256: strings.Repeat("a", 64)},
			Checkpoint: workexecution.Checkpoint{AttemptID: "try_" + suffix, Sequence: 1, StageID: "step_" + suffix,
				Evidence: workexecution.Input{
					ID:     ids.NewAt(ids.KindPlan, time.Date(2026, 9, 13, 12, 0, 0, 0, time.UTC), 2),
					SHA256: strings.Repeat("b", 64),
				}, RecordedAt: time.Date(2026, 9, 13, 12, 0, 0, 0, time.UTC)},
			RenderGeneration: 1, AcknowledgedAt: time.Date(2026, 9, 13, 12, 0, 0, 0, time.UTC),
		},
	}
}
