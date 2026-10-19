package taskplanning

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/composerender"
	"github.com/AlanD20/groundplane/internal/controller/servicelifecycle"
	"github.com/AlanD20/groundplane/internal/controller/serviceremoval"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	"github.com/AlanD20/groundplane/internal/infra/etcd/servicelifecyclework"
	"github.com/AlanD20/groundplane/internal/infra/etcd/serviceremovalwork"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v3"
)

// SVC-02/03, TASK-41/73: lifecycle commands must retain every captured physical
// member and its original owner across durable reconstruction, not re-render
// mutable desired state or invent a new proxy owner.
func TestNativeLifecycleRetainsBothSlotsAndOriginalProxy(t *testing.T) {
	for _, action := range []workexecution.Action{workexecution.ActionStart, workexecution.ActionStop, workexecution.ActionDestroy} {
		t.Run(string(action), func(t *testing.T) {
			capture, expected := nativeLifecycleCaptureFixture(t, action)
			generation := capture.Runtime.RenderGeneration()
			name := capture.Runtime.Release.Current.Projection.DesiredServices[0].Desired.Name
			runtimeValue, err := releaserender.EncodeServiceLifecycleRenderInput(*capture.Runtime)
			if err != nil {
				t.Fatal(err)
			}
			encoded, err := servicelifecyclework.Encode(capture)
			if err != nil {
				t.Fatal(err)
			}
			first, err := servicelifecycle.BuildWorkInput(capture)
			if err != nil {
				t.Fatal(err)
			}
			capture.Runtime.Release.Current.Projection.NormalizedCompose = []byte("not execution authority")
			capture.Runtime.Release.Current.Projection.DesiredServices[0].Desired.Name = "edited-after-capture"
			decodedRuntime, err := releaserender.DecodeServiceLifecycleRenderInput(runtimeValue)
			clear(runtimeValue)
			if err != nil || decodedRuntime.RenderGeneration() != generation ||
				decodedRuntime.Release.Current.Projection.DesiredServices[0].Desired.Name != name {
				t.Fatalf("captured lifecycle projection aliases later edits or changed generation: %v", err)
			}
			retained, err := servicelifecyclework.Decode(encoded)
			if err != nil {
				t.Fatal(err)
			}
			if retained.Runtime.Release.Current.Projection.DesiredServices[0].Desired.Name != name {
				t.Fatal("captured lifecycle input selected a later desired Service")
			}
			second, err := servicelifecycle.BuildWorkInput(retained)
			if err != nil || !proto.Equal(first, second) {
				t.Fatalf("captured lifecycle changed after reconstruction: %v", err)
			}
			if len(first.Members) != 3 {
				t.Fatal("native lifecycle lost a slot or its stable proxy")
			}
			seen := map[string]bool{}
			for _, member := range first.Members {
				actual := member.Artifact.Services[0]
				if seen[actual.ComposeName] || !proto.Equal(actual, expected[actual.ComposeName]) {
					t.Fatal("native lifecycle changed the captured member or original ownership")
				}
				seen[actual.ComposeName] = true
				if len(member.Ownership.ComposeNames) != 1 || member.Ownership.ComposeNames[0] != actual.ComposeName {
					t.Fatal("native lifecycle widened its selected Compose member")
				}
			}
			want := map[workexecution.Action]agentpb.ServiceLifecycleAction{
				workexecution.ActionStart:   agentpb.ServiceLifecycleAction_SERVICE_LIFECYCLE_ACTION_START,
				workexecution.ActionStop:    agentpb.ServiceLifecycleAction_SERVICE_LIFECYCLE_ACTION_STOP,
				workexecution.ActionDestroy: agentpb.ServiceLifecycleAction_SERVICE_LIFECYCLE_ACTION_DESTROY,
			}[action]
			if first.Action != want || action == workexecution.ActionStop && first.StopGraceSeconds != 30 ||
				action != workexecution.ActionStop && first.StopGraceSeconds != 0 {
				t.Fatal("Start, Stop and runtime removal lost their distinct actions")
			}
			retained.HostID = ids.New(ids.KindHost)
			if _, err := servicelifecycle.BuildWorkInput(retained); err == nil {
				t.Fatal("a different host acquired the captured runtime")
			}
		})
	}
}

// SVC-04/TASK-73: removal must select the same acknowledged physical runtime, including
// the inactive slot and proxy, rather than the unsuffixed authored Service.
func TestNativeServiceRemovalKeepsExactPhysicalSelection(t *testing.T) {
	capture, expected := nativeLifecycleCaptureFixture(t, workexecution.ActionDestroy)
	input := serviceremovalwork.Input{TaskID: capture.TaskID, WorkItemID: capture.WorkItemID, PlanID: capture.PlanID,
		Scope: capture.Scope, ServiceID: capture.ServiceID, ServiceName: "api", ServiceRevision: 1, RuntimeRevision: 1,
		DesiredRevisionID: ids.New(
			ids.KindTask,
		), DesiredSHA256: strings.Repeat("a", 64), BaseRevisionID: ids.New(ids.KindTask),
		HeadRevision: 1, Generation: 7, HostID: capture.HostID, Runtime: capture.Runtime.AcknowledgedRuntime,
		RuntimeSourceRevision: capture.Runtime.AcknowledgedRuntimeRevision, CreatedAt: capture.CreatedAt}
	work, err := serviceremoval.BuildHostInput(input)
	if err != nil {
		t.Fatal(err)
	}
	if work.Action != agentpb.ServiceLifecycleAction_SERVICE_LIFECYCLE_ACTION_DESTROY ||
		len(work.Members) != len(expected) {
		t.Fatal("Service removal lost captured physical members")
	}
	for _, member := range work.Members {
		service := member.Artifact.Services[0]
		if !proto.Equal(service, expected[service.ComposeName]) {
			t.Fatal("Service removal changed captured workload or proxy ownership")
		}
		delete(expected, service.ComposeName)
	}
	if len(expected) != 0 {
		t.Fatal("Service removal omitted a physical member")
	}
	input.HostID = ids.New(ids.KindHost)
	if _, err := serviceremoval.BuildHostInput(input); err == nil {
		t.Fatal("a different host acquired Service removal authority")
	}
}

func nativeLifecycleCaptureFixture(
	t *testing.T,
	action workexecution.Action,
) (servicelifecyclework.Input, map[string]*agentpb.ComposeService) {
	t.Helper()
	current, _, active, inactive := nativeRetainedArtifactsFixture(t)
	retainOlderProxyOwnerFixture(t, active)
	acknowledged := acknowledgedRuntimeSource(t, active, inactive)
	prior := current
	prior.ReleaseID, prior.ArtifactID = "dep_01ARZ3NDEKTSV4RRFFQ69G5FAX", inactive.ArtifactId
	prior.CandidateWorkload = *current.PriorWorkload
	prior.CandidateWorkload.LocalImageID = inactive.Services[0].ImageReference
	prior.CandidateTarget, prior.Slot = current.PriorTarget, current.PriorSlot
	prior.ProxyGeneration = current.PriorProxyGeneration
	prior.PriorArtifactID, prior.PriorWorkload, prior.PriorRuntime = "", nil, nil
	prior.PriorProxyGeneration, prior.PriorProxyDigest = 0, ""
	for _, member := range active.Services {
		if member.Role == agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY {
			current.ProxyConfigDigest = hex.EncodeToString(member.ProxyConfigSha256)
		}
	}
	configuration, err := domain.RenderProxyConfig(prior.ProxyImplementation, prior.ServiceName, prior.ReleaseID,
		prior.CandidateTarget, prior.ProxyGeneration, prior.ProxyPorts)
	if err != nil {
		t.Fatal(err)
	}
	prior.ProxyConfigDigest, current.PriorProxyDigest = hex.EncodeToString(
		configuration.SHA256[:],
	), hex.EncodeToString(
		configuration.SHA256[:],
	)
	// The older Release captured its then-serving proxy too. The inactive
	// slot retained after the switch intentionally contains no second proxy.
	priorServing, err := proto.MarshalOptions{Deterministic: true}.Marshal(nativeRuntimeFixture(t, prior, true))
	if err != nil {
		t.Fatal(err)
	}
	current.PriorRuntime = &releaserender.ServingRuntimeCapture{
		ServiceID:       current.ServiceID,
		CurrentArtifact: priorServing,
	}
	planID := ids.New(ids.KindPlan)
	runtime := &releaserender.ServiceLifecycleRenderInput{PlanID: planID, ServiceID: current.ServiceID,
		TenantID: current.TenantID, TenantSlug: current.TenantSlug, ProjectID: current.ProjectID, ProjectSlug: current.ProjectSlug,
		EnvironmentID: current.EnvironmentID, EnvironmentName: current.EnvironmentName, AuthorizedVolumeDir: current.AuthorizedVolumeDir,
		ArtifactID: active.ArtifactId, AppliedRenderGeneration: current.Projection.RenderGeneration, AppliedProjectionRevision: 20,
		AcknowledgedRuntime: &acknowledged, AcknowledgedRuntimeRevision: 21,
		Release: &releaserender.ServiceLifecycleRelease{
			ServingReleaseID:            current.ReleaseID,
			PriorServingReleaseID:       prior.ReleaseID,
			ProjectionRevision:          22,
			IntentRevision:              23,
			RenderRevision:              24,
			Current:                     current,
			RetainedPrior:               &prior,
			RetainedPriorRenderRevision: 25,
		}}
	if err := releaserender.ValidateReleaseRenderInput(current); err != nil {
		t.Fatalf("current Release fixture: %v", err)
	}
	if err := releaserender.ValidateReleaseRenderInput(prior); err != nil {
		t.Fatalf("retained Release fixture: %v", err)
	}
	if err := releaserender.ValidateServiceLifecycleRenderInput(*runtime); err != nil {
		t.Fatalf("lifecycle fixture: %v", err)
	}
	expected := map[string]*agentpb.ComposeService{}
	for _, artifact := range []*agentpb.ComposeArtifact{active, inactive} {
		for _, member := range artifact.Services {
			expected[member.ComposeName] = proto.CloneOf(member)
		}
	}
	return servicelifecyclework.Input{
		TaskID:          ids.New(ids.KindTask),
		WorkItemID:      ids.New(ids.KindWorkItem),
		PlanID:          planID,
		Action:          action,
		ServiceID:       current.ServiceID,
		ServiceRevision: 1,
		Scope: workexecution.Scope{
			TenantID:      current.TenantID,
			ProjectID:     current.ProjectID,
			EnvironmentID: current.EnvironmentID,
		},
		HostID:    acknowledged.Source.Executor.HostID,
		Runtime:   runtime,
		CreatedAt: time.Now().UTC(),
	}, expected
}

func retainOlderProxyOwnerFixture(t *testing.T, artifact *agentpb.ComposeArtifact) {
	t.Helper()
	var document yaml.Node
	if err := yaml.Unmarshal(artifact.CanonicalYaml, &document); err != nil {
		t.Fatal(err)
	}
	services, err := composerender.ServiceArtifactMapping(document.Content[0])
	if err != nil {
		t.Fatal(err)
	}
	for _, service := range artifact.Services {
		if service.Role != agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY {
			continue
		}
		owner := ids.New(ids.KindPlan)
		for _, label := range service.ExpectedLabels {
			if label.Key == "com.groundplane.plan-id" {
				label.Value = owner
			}
		}
		index := composerender.MappingIndex(services, service.ComposeName)
		if index < 0 {
			t.Fatal("fixture proxy has no Compose definition")
		}
		member := services.Content[index+1]
		labelsIndex := composerender.MappingIndex(member, "labels")
		if labelsIndex < 0 {
			t.Fatal("fixture proxy has no ownership labels")
		}
		labels := member.Content[labelsIndex+1]
		planIndex := composerender.MappingIndex(labels, "com.groundplane.plan-id")
		if planIndex < 0 {
			t.Fatal("fixture proxy has no original plan owner")
		}
		labels.Content[planIndex+1].Value = owner
	}
	artifact.CanonicalYaml, err = yaml.Marshal(&document)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(artifact.CanonicalYaml)
	artifact.YamlSha256 = digest[:]
}
