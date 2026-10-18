package dnsresolver

import (
	"bytes"
	"errors"
	"net/netip"
	"strings"
	"testing"
	"time"

	componentdns "github.com/AlanD20/groundplane-component-sdk/dnsresolver"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/resolverexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/components"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/resolverwork"
	"github.com/AlanD20/groundplane/pkg/errs"
	"google.golang.org/protobuf/proto"
)

// TASK-58: first activation may have no predecessor. Existing runtime identities
// without independent applied state must never become implicit cleanup authority.
func TestPrepareNativeResolverWorkRequiresAppliedPredecessor(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		for _, hasRuntime := range []bool{false, true} {
			t.Run(
				map[bool]string{false: "disabled", true: "enabled"}[enabled]+map[bool]string{false: "/new", true: "/missing-applied"}[hasRuntime],
				func(t *testing.T) {
					planner, _ := nativeResolverPlanner(t)
					component := nativeResolverComponent()
					component.Enabled = enabled
					if hasRuntime {
						component.GeneratedServices = []string{ids.New(ids.KindService)}
					}
					current, err := components.NewRecord(component)
					if err != nil {
						t.Fatal(err)
					}
					desired := component
					desired.Enabled = true
					records, err := resolverwork.NewRepository(nativeResolverReadStore{})
					if err != nil {
						t.Fatal(err)
					}
					request := nativeResolverRequest()
					prepared, err := planner.PrepareWork(
						t.Context(),
						records,
						keyvalue.Versioned[components.Record]{
							Record:       current,
							Revision:     7,
							ReadRevision: 42,
						},
						desired,
						request,
					)
					if hasRuntime {
						if !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
							t.Fatalf(
								"runtime identities without applied configuration were not rejected as a conflict: %v",
								err,
							)
						}
						return
					}
					if err != nil {
						t.Fatal(err)
					}
					if prepared.Input.Previous != nil || !prepared.Input.EnsureRuntime ||
						prepared.Input.Candidate == nil ||
						string(prepared.Input.Candidate.Corefile) != "captured Corefile\n" {
						t.Fatal("first activation invented a predecessor or lost its captured file")
					}
				},
			)
		}
	}
}

// TASK-58: reload changes only file ownership; runtime replacement advances its
// Compose identity while preserving the Service. Host platform, not Controller
// GOARCH, selects the image. Accepted bytes stay independent of planner buffers.
func TestNativeResolverReloadAndReplacementCaptureExactRuntime(t *testing.T) {
	planner, environment := nativeResolverPlanner(t)
	component, request := nativeResolverComponent(), nativeResolverRequest()
	first, ensure, err := planner.prepareNativeConfiguration(component, nativeResolverSelected(t), request, 1, nil)
	if err != nil || !ensure {
		t.Fatalf("first activation: %v", err)
	}
	if string(first.Corefile) != "captured Corefile\n" {
		t.Fatal("planner buffer clearing destroyed captured configuration")
	}
	request.PlanID = ids.New(ids.KindPlan)
	reload, ensure, err := planner.prepareNativeConfiguration(component, nativeResolverSelected(t), request, 2, &first)
	if err != nil || ensure {
		t.Fatalf("reload unnecessarily replaces the runtime: %v", err)
	}
	if !proto.Equal(reload.Runtime, first.Runtime) || reload.RuntimeGeneration != first.RuntimeGeneration ||
		reload.PlanID != first.PlanID || reload.Apply.ExpectedPreviousArtifactId != first.Apply.ArtifactId ||
		!bytes.Equal(reload.Apply.ExpectedPreviousArtifactDigest, first.Apply.ArtifactDigest) {
		t.Fatal("reload changed predecessor runtime ownership or file comparison")
	}
	// A real image change must use a new runtime artifact, not relabel the old one.
	environment.image = "example/new-resolver"
	request.PlanID = ids.New(ids.KindPlan)
	replacement, ensure, err := planner.prepareNativeConfiguration(
		component,
		nativeResolverSelected(t),
		request,
		3,
		&reload,
	)
	if err != nil || !ensure {
		t.Fatalf("changed image did not require replacement: %v", err)
	}
	if replacement.Runtime.ArtifactId == first.Runtime.ArtifactId || replacement.RuntimeGeneration != 3 ||
		replacement.Runtime.Services[0].ServiceId != first.Runtime.Services[0].ServiceId ||
		replacement.Runtime.Services[0].ImageReference == first.Runtime.Services[0].ImageReference {
		t.Fatal("image replacement lost stable Service identity or retained obsolete runtime")
	}
	input := resolverexecution.Input{TaskID: request.TaskID, WorkID: request.WorkID, HostID: request.HostID,
		OperationID: request.OperationID, ComponentID: component.ID, Generation: 3, BaselineSHA256: strings.Repeat("b", 64),
		CreatedAt: request.CreatedAt, Deadline: request.Deadline, Enabled: true, EnsureRuntime: true,
		Candidate: &replacement, Previous: &reload, PreviousEnabled: true, PreviousFileOwner: "mct_" + strings.Repeat("a", 64)}
	raw, reference, err := resolverexecution.Capture(request.PlanID, input)
	if err != nil {
		t.Fatal(err)
	}
	replacement.Corefile[0] = 'X'
	replacement.Runtime.Services[0].ImageReference = "untrusted/new-image:latest"
	decoded, again, err := resolverexecution.Decode(request.PlanID, raw)
	if err != nil || reference != again || string(decoded.Candidate.Corefile) != "captured Corefile\n" ||
		decoded.Candidate.Runtime.Services[0].ImageReference == replacement.Runtime.Services[0].ImageReference {
		t.Fatalf("captured input was recomputed from changed planner data: %v", err)
	}
	request.ImageArchitecture = "arm64"
	arm, _, err := planner.prepareNativeConfiguration(component, nativeResolverSelected(t), request, 4, nil)
	_, expectedImage, supported := testPlatformImage(environment.image).Select("linux", "arm64")
	if err != nil || !supported || arm.Runtime.Services[0].ImageReference != expectedImage {
		t.Fatalf("resolver did not select the captured target-host architecture: %v", err)
	}
	request.ImageArchitecture = "unsupported"
	if _, _, err := planner.prepareNativeConfiguration(component, nativeResolverSelected(t), request, 4, nil); err == nil {
		t.Fatal("unavailable target-host image was accepted")
	}
}

// TASK-58: disabling a retained runtime consumes its exact applied configuration;
// it cannot reconstruct a different container from today's compiled catalog.
func TestNativeResolverDisableUsesRetainedAppliedConfiguration(t *testing.T) {
	planner, environment := nativeResolverPlanner(t)
	component, request := nativeResolverComponent(), nativeResolverRequest()
	configuration, _, err := planner.prepareNativeConfiguration(component, nativeResolverSelected(t), request, 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	component.Enabled = false
	current, err := components.NewRecord(component)
	if err != nil {
		t.Fatal(err)
	}
	baseline, _, err := planner.baselines.GetHostResolverBaseline(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	projection, _, err := planner.projections.GetHostResolutionProjection(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	applied := resolverwork.Applied{
		HostID:           request.HostID,
		ComponentID:      component.ID,
		Generation:       1,
		Desired:          current.Desired,
		Configuration:    &configuration,
		FileOwner:        "mct_" + strings.Repeat("a", 64),
		BaselineSHA256:   baseline.Record.SHA256,
		ProjectionSHA256: projection.Record.InputSHA256,
		ObservedAt:       request.CreatedAt,
	}
	raw, err := resolverwork.EncodeApplied(applied)
	if err != nil {
		t.Fatal(err)
	}
	records, err := resolverwork.NewRepository(
		nativeResolverReadStore{values: map[string][]byte{resolverwork.AppliedKey(request.HostID, component.ID): raw}},
	)
	if err != nil {
		t.Fatal(err)
	}
	environment.image = "example/new-resolver"
	request.Action, request.PlanID = workexecution.ActionStop, ids.New(ids.KindPlan)
	prepared, err := planner.PrepareWork(
		t.Context(),
		records,
		keyvalue.Versioned[components.Record]{Record: current, Revision: 7, ReadRevision: 42},
		component,
		request,
	)
	if err != nil {
		t.Fatal(err)
	}
	if prepared.Input.Enabled || prepared.Input.Candidate != nil || prepared.Input.Previous == nil ||
		!proto.Equal(prepared.Input.Previous.Runtime, configuration.Runtime) ||
		prepared.Input.PreviousFileOwner != applied.FileOwner {
		t.Fatal("disable regenerated cleanup authority instead of retaining the exact predecessor")
	}
}

func nativeResolverComponent() core.Component {
	return core.Component{
		ID:      ids.New(ids.KindComponent),
		Owner:   core.ComponentOwnerPlatform,
		Kind:    core.ComponentKindCoreDNS,
		Enabled: true,
		Config: core.ComponentConfig{
			CoreDNS: &core.CoreDNSComponentConfig{CorefileTemplate: testCorefileTemplate, UpstreamAuto: true},
		},
	}
}

func nativeResolverRequest() WorkRequest {
	at := time.Now().UTC()
	return WorkRequest{TaskID: ids.New(ids.KindTask), WorkID: ids.New(ids.KindWorkItem), PlanID: ids.New(ids.KindPlan),
		OperationID: ids.New(
			ids.KindOperation,
		), HostID: ids.New(ids.KindHost), ImageOS: "linux", ImageArchitecture: "amd64",
		Actor: workexecution.SystemActor, Action: workexecution.ActionUpdate, CreatedAt: at, Deadline: at.Add(time.Minute)}
}

func nativeResolverSelected(t *testing.T) componentdns.ResolverInput {
	t.Helper()
	baseline, err := componentdns.NewResolverBaseline(
		1,
		[]componentdns.ResolverEndpoint{{Address: netip.MustParseAddr("1.1.1.1"), Port: 53}},
	)
	if err != nil {
		t.Fatal(err)
	}
	hosts, err := componentdns.NewHostResolutionProjection(1, [32]byte{1}, nil)
	if err != nil {
		t.Fatal(err)
	}
	return componentdns.ResolverInput{Baseline: baseline, HostResolution: hosts}
}
