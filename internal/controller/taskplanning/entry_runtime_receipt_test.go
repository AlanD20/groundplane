package taskplanning

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"slices"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	testtaskmaterialization "github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	composerender "github.com/AlanD20/groundplane/internal/controller/composerender"
	"github.com/AlanD20/groundplane/internal/core"
	testentries "github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runtimeconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// QA: TASK-72, ENT-08, SVC-15, JOURNEY-02; native capture proof, not live recovery.
// Rationale: a successful Entry candidate must prepare a new receipt from the
// selected acknowledged runtime while keeping the stable proxy byte-identical.
// A stopped or unrelated Service has no update.
func TestEntryMutationPreparesOnlySelectedAcknowledgedRuntime(t *testing.T) {
	render, authored, active, retained := nativeRetainedArtifactsFixture(t)
	source := acknowledgedRuntimeSource(t, active, retained)
	priorRuntime := source.Runtime
	current := render.Projection
	baseline, err := RetainBlueprintNativeRuntimeSources(
		authored,
		[]serviceruntimerecord.Record{source},
		[]string{render.ServiceID},
	)
	if err != nil {
		t.Fatal(err)
	}
	current.ComposeArtifact, err = proto.MarshalOptions{Deterministic: true}.Marshal(baseline)
	if err != nil {
		t.Fatal(err)
	}
	entry := testentries.Record{EnvironmentID: current.EnvironmentID,
		Entry: core.EnvEntry{ID: "ev_01ARZ3NDEKTSV4RRFFQ69G5FC1", Kind: core.EntryKindEnv,
			Key: "MODE", Source: core.EntrySource{Kind: core.SourceLiteral}, Exposure: []string{"api"}},
		CurrentValueGenerationID: "cfg_01ARZ3NDEKTSV4RRFFQ69G5FC2"}
	candidate, materials, err := composerender.ProjectEnvironmentEntryMutation(
		current,
		composerender.EnvironmentEntryArtifactMutation{
			RevisionID: "task_01ARZ3NDEKTSV4RRFFQ69G5FC3", ArtifactID: "cfg_01ARZ3NDEKTSV4RRFFQ69G5FC4",
			PlanID: "plan_01ARZ3NDEKTSV4RRFFQ69G5FC5", RenderGeneration: current.RenderGeneration + 1,
			Entries: []testentries.Record{entry},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Now().UTC()
	digest := sha256.Sum256([]byte("MODE=test\n"))
	intent := workexecution.Input{ID: ids.New(ids.KindPlan), SHA256: hex.EncodeToString(digest[:])}
	task := workexecution.Task{
		ID:     candidate.RevisionID,
		Actor:  workexecution.OperatorActor,
		Action: workexecution.ActionUpdate,
		Scope: workexecution.Scope{
			TenantID:      render.TenantID,
			ProjectID:     render.ProjectID,
			EnvironmentID: current.EnvironmentID,
		},
		TargetID:      entry.Entry.ID,
		Intent:        intent,
		ExpectedItems: 1,
		CreatedAt:     at,
		UpdatedAt:     at,
		Deadline:      at.Add(time.Minute),
	}
	item := workexecution.Item{
		ID:        ids.New(ids.KindWorkItem),
		TaskID:    task.ID,
		Operation: workexecution.EntryConfiguration,
		TargetID:  current.EnvironmentID,
		Placement: workexecution.Placement{Executor: workexecution.Agent, HostID: ids.New(ids.KindHost)},
		Phase:     workexecution.Waiting,
		Wait:      &workexecution.Wait{Reason: workexecution.WaitInput},
		Cleanup:   workexecution.CleanupNone,
		CreatedAt: at,
		UpdatedAt: at,
		Deadline:  task.Deadline,
	}
	item.Claims = []workexecution.Claim{
		{ScopeID: current.EnvironmentID},
		{ScopeID: current.EnvironmentID, HostID: item.Placement.HostID},
	}
	files := []testtaskmaterialization.Record{}
	configuration := runtimeconfiguration.WorkCapture{TaskID: task.ID, WorkItemID: item.ID, Placement: item.Placement,
		ReadRevision: 19, Generation: candidate.RenderGeneration,
		Current: runtimeconfiguration.Reference{ID: ids.New(ids.KindConfig), EnvironmentID: current.EnvironmentID,
			Generation: candidate.RenderGeneration, SHA256: intent.SHA256}}
	for _, material := range materials {
		files = append(files, testtaskmaterialization.Record{
			StepID: ids.New(ids.KindStep), MaterializationID: ids.New(ids.KindConfig),
			EnvironmentID: current.EnvironmentID, Destination: material.Destination,
			ServiceID: material.ServiceID, ServiceName: material.ServiceName, OutputKind: material.OutputKind,
			UID: material.UID, GID: material.GID, Mode: uint32(material.Mode), Length: 10,
			SHA256: hex.EncodeToString(digest[:]), Source: material.Source,
		})
		configuration.Destinations = append(configuration.Destinations, material.Destination)
	}
	runtime := EntryMutationRuntime{Projection: current, EpochRevision: 19,
		RunningServiceIDs: []string{render.ServiceID},
		Sources: []testkeyvalue.Versioned[serviceruntimerecord.Record]{{
			Revision: 17, Record: source,
		}}}
	input, capture, err := runtime.PrepareConfigurationWork(EntryConfigurationRequest{Task: task, Item: item,
		OperationID: ids.New(ids.KindOperation), Candidate: candidate, Files: files, Configuration: configuration})
	if err != nil {
		t.Fatal(err)
	}
	if len(capture.Consumers) != 1 || capture.Consumers[0].Revision != 17 ||
		capture.Consumers[0].Previous.Runtime.ServiceID != render.ServiceID ||
		!slices.Equal(input.RunningConsumerIds, []string{render.ServiceID}) {
		t.Fatal("native Entry capture did not select the acknowledged running consumer")
	}
	artifact := new(agentpb.ComposeArtifact)
	if proto.Unmarshal(candidate.ComposeArtifact, artifact) != nil {
		t.Fatal("open Entry candidate artifact")
	}
	update := capture.Consumers[0]
	prepared, err := executionplan.PrepareEntryRuntime(
		artifact, priorRuntime, update.CurrentArtifactID, update.RetainedArtifactID,
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
