package controller

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"slices"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// QA: ENT-08, SVC-15, JOURNEY-02; local sealed-plan proof, not live recovery.
// Rationale: a successful Entry candidate must prepare a new receipt from the
// selected acknowledged runtime while keeping the stable proxy byte-identical.
// A stopped or unrelated Service has no update.
func TestEntryMutationPreparesOnlySelectedAcknowledgedRuntime(t *testing.T) {
	_, _, input := redeployRestorationInput(t, domain.StrategyBlueGreen)
	render := input.Members[0].Render
	priorRuntime := entryReceiptRuntimeFromAuthority(t, render)
	current := render.Projection
	current.ComposeArtifact = slices.Clone(priorRuntime.CurrentArtifact)
	entry := etcd.EntryRecord{EnvironmentID: current.EnvironmentID,
		Entry: core.EnvEntry{ID: "ev_01ARZ3NDEKTSV4RRFFQ69G5FC1", Kind: core.EntryKindEnv,
			Key: "MODE", Source: core.EntrySource{Kind: core.SourceLiteral}, Exposure: []string{"api"}},
		CurrentValueGenerationID: "cfg_01ARZ3NDEKTSV4RRFFQ69G5FC2"}
	candidate, materials, err := ProjectEnvironmentEntryMutation(current, EnvironmentEntryArtifactMutation{
		RevisionID: "task_01ARZ3NDEKTSV4RRFFQ69G5FC3", ArtifactID: "cfg_01ARZ3NDEKTSV4RRFFQ69G5FC4",
		PlanID: "plan_01ARZ3NDEKTSV4RRFFQ69G5FC5", RenderGeneration: current.RenderGeneration + 1,
		Entries: []etcd.EntryRecord{entry},
	})
	if err != nil {
		t.Fatal(err)
	}
	task := etcd.TaskRecord{ID: candidate.RevisionID, PlanID: "plan_01ARZ3NDEKTSV4RRFFQ69G5FC5",
		Type: etcd.TaskUpdate, Executor: etcd.TaskExecutorAgent, Target: current.EnvironmentID,
		RenderGeneration: int32(candidate.RenderGeneration), TimeoutSeconds: 120}
	for _, material := range materials {
		digest := sha256.Sum256([]byte("MODE=test\n"))
		task.Materializations = append(task.Materializations, etcd.TaskMaterializationRecord{
			StepID: ids.New(ids.KindStep), MaterializationID: ids.New(ids.KindConfig),
			EnvironmentID: current.EnvironmentID, Destination: material.Destination,
			ServiceID: material.ServiceID, ServiceName: material.ServiceName, OutputKind: material.OutputKind,
			UID: material.UID, GID: material.GID, Mode: uint32(material.Mode), Length: 10,
			SHA256: hex.EncodeToString(digest[:]), Source: material.Source,
		})
	}
	runtime := EntryMutationRuntime{Projection: current, EpochRevision: 19,
		RunningServiceIDs: []string{render.ServiceID},
		Sources: []etcd.Versioned[serviceruntimerecord.Record]{{
			Revision: 17, Record: serviceruntimerecord.Record{Runtime: priorRuntime},
		}}}
	task, err = runtime.PrepareTask("/var/lib/groundplane/vol", task, candidate,
		"step_01ARZ3NDEKTSV4RRFFQ69G5FC9")
	if err != nil {
		t.Fatal(err)
	}
	if task.EntryRuntime == nil || len(task.EntryRuntime.Updates) != 1 ||
		task.EntryRuntime.Updates[0].PreviousRevision != 17 ||
		task.EntryRuntime.Updates[0].ServiceID != render.ServiceID {
		t.Fatalf("prepared Entry runtime = %#v", task.EntryRuntime)
	}
	artifact := new(agentpb.ComposeArtifact)
	if proto.Unmarshal(candidate.ComposeArtifact, artifact) != nil {
		t.Fatal("open Entry candidate artifact")
	}
	update := task.EntryRuntime.Updates[0]
	prepared, err := executionplan.PrepareEntryRuntime(
		artifact, priorRuntime, update.CurrentArtifactID, update.RetainedPriorArtifactID,
	)
	if err != nil {
		t.Fatal(err)
	}
	beforeCurrent, afterCurrent := new(agentpb.ComposeArtifact), new(agentpb.ComposeArtifact)
	beforeRetained, afterRetained := new(agentpb.ComposeArtifact), new(agentpb.ComposeArtifact)
	if proto.Unmarshal(priorRuntime.CurrentArtifact, beforeCurrent) != nil ||
		proto.Unmarshal(prepared.CurrentArtifact, afterCurrent) != nil {
		t.Fatal("open prepared Entry runtime")
	}
	after := []*agentpb.ComposeArtifact{afterCurrent}
	if len(priorRuntime.RetainedPriorArtifact) != 0 {
		if proto.Unmarshal(priorRuntime.RetainedPriorArtifact, beforeRetained) != nil ||
			proto.Unmarshal(prepared.RetainedPriorArtifact, afterRetained) != nil {
			t.Fatal("open prepared retained Entry runtime")
		}
		after = append(after, afterRetained)
	} else if len(prepared.RetainedPriorArtifact) != 0 {
		t.Fatal("Entry mutation fabricated a retained runtime")
	}
	for _, artifact := range after {
		if !bytes.Contains(artifact.CanonicalYaml, []byte("secrets/.env."+current.EnvironmentID+".api")) {
			t.Fatal("selected retained workload did not receive the Entry environment")
		}
	}
	proxy := func(artifact *agentpb.ComposeArtifact) *agentpb.ComposeService {
		for _, service := range artifact.Services {
			if service.Role == agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY {
				return service
			}
		}
		return nil
	}
	if !proto.Equal(proxy(beforeCurrent), proxy(afterCurrent)) || proxy(beforeRetained) != nil ||
		proxy(afterRetained) != nil {
		t.Fatal("Entry mutation changed or fabricated the acknowledged stable proxy")
	}
}

func entryReceiptRuntimeFromAuthority(t *testing.T, render etcd.ReleaseRenderInput) executionplan.CandidateRuntime {
	t.Helper()
	artifact := new(agentpb.ComposeArtifact)
	if render.PriorRuntime == nil || proto.Unmarshal(render.PriorRuntime.CurrentArtifact, artifact) != nil {
		t.Fatal("open prior runtime authority")
	}
	runtime := executionplan.CandidateRuntime{ServiceID: render.ServiceID,
		CurrentArtifact:       slices.Clone(render.PriorRuntime.CurrentArtifact),
		RetainedPriorArtifact: slices.Clone(render.PriorRuntime.RetainedPriorArtifact)}
	for _, service := range artifact.Services {
		if service.ServiceId != render.ServiceID {
			continue
		}
		if service.Role == agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY {
			continue
		}
		for _, label := range service.ExpectedLabels {
			if label.Key == "com.groundplane.release-id" {
				runtime.ReleaseID = label.Value
			}
		}
		if service.Role == agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT {
			runtime.Target = service.Slot
		} else {
			runtime.Target = "singleton"
		}
	}
	for _, service := range artifact.Services {
		if service.ServiceId == render.ServiceID &&
			service.Role == agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY {
			var err error
			runtime.ProxyGeneration, err = executionplan.ProxyConfigGeneration(
				service.ProxyConfigJson,
				runtime.ReleaseID,
			)
			if err != nil {
				t.Fatal(err)
			}
			digest := sha256.Sum256(service.ProxyConfigJson)
			runtime.ProxyConfigSHA256 = digest[:]
		}
	}
	return runtime
}
