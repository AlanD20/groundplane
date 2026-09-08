package app

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"runtime"
	"testing"

	componentsdk "github.com/AlanD20/groundplane-component-sdk/component"
	componentdns "github.com/AlanD20/groundplane-component-sdk/dnsresolver"
	registeredcoredns "github.com/AlanD20/groundplane-registered-components/coredns"
	"github.com/AlanD20/groundplane/internal/agent"
	"github.com/AlanD20/groundplane/internal/infra/docker/dnsresolverobserver"
	"github.com/AlanD20/groundplane/internal/infra/docker/managedconfighelpercontainer"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func TestRegisteredPlannerRejectsUnregisteredValidImage(t *testing.T) {
	catalog, err := newRegisteredActionCatalog()
	if err != nil {
		t.Fatal(err)
	}
	unknown := registeredcoredns.Image
	unknown.Platforms = append([]componentsdk.OCIPlatform(nil), unknown.Platforms...)
	unknown.Repository = "example/unknown-resolver"
	catalog.planners["coredns"] = func(string, componentdns.RenderInput) (componentsdk.EnvironmentPlan, error) {
		return componentsdk.EnvironmentPlan{Services: []componentsdk.ManagedService{{Image: unknown}}}, nil
	}
	if _, err := catalog.Plan("coredns", "svc_test", componentdns.RenderInput{}); err == nil {
		t.Fatal("Plan() accepted an unregistered valid managed image")
	}
}

type componentActionManagedExecutorStub struct{}

func (componentActionManagedExecutorStub) Validate(
	context.Context,
	managedconfighelpercontainer.ValidatorImage,
	[]string,
	[]byte,
) error {
	return nil
}

type componentActionRecoveringManagedExecutor struct {
	calls    int
	response *agentpb.ManagedConfigHelperResponse
}

func (*componentActionRecoveringManagedExecutor) Validate(
	context.Context,
	managedconfighelpercontainer.ValidatorImage,
	[]string,
	[]byte,
) error {
	return nil
}

func (executor *componentActionRecoveringManagedExecutor) Execute(
	context.Context,
	*agentpb.ManagedConfigHelperRequest,
) (*agentpb.ManagedConfigHelperResponse, error) {
	executor.calls++
	if executor.calls == 1 {
		return nil, errors.New("managed-config response was lost")
	}
	return executor.response, nil
}

func (componentActionManagedExecutorStub) Execute(
	context.Context,
	*agentpb.ManagedConfigHelperRequest,
) (*agentpb.ManagedConfigHelperResponse, error) {
	return &agentpb.ManagedConfigHelperResponse{}, nil
}

type componentActionComposeHelperStub struct{}

func (componentActionComposeHelperStub) Execute(
	context.Context,
	*agentpb.ComposeHelperRequest,
) (*agentpb.ComposeHelperResponse, error) {
	return &agentpb.ComposeHelperResponse{}, nil
}

type componentActionObserverStub struct {
	request  dnsresolverobserver.Request
	evidence *agentpb.DNSResolverObservationEvidence
}

func (observer *componentActionObserverStub) Observe(
	_ context.Context,
	request dnsresolverobserver.Request,
) (*agentpb.DNSResolverObservationEvidence, error) {
	observer.request = request
	return observer.evidence, nil
}

// Rationale: serving observation is a catalog action with no transferred
// content, and enable/update candidate checks must reach that same generic
// runtime seam using the artifact digest carried by their action envelope.
func TestRegisteredComponentActionRuntimeObservesDNSResolverWithoutManagedContent(t *testing.T) {
	catalog, err := newRegisteredActionCatalog()
	if err != nil {
		t.Fatal(err)
	}
	definition, _, found := catalog.FindAction("coredns", registeredcoredns.ObserveServingAction)
	if !found {
		t.Fatal("CoreDNS observation action is absent")
	}
	digest := sha256.Sum256([]byte("candidate Corefile"))
	definitionDigest := definition.Digest()
	catalogDigest := catalog.Digest()
	platform, imageReference, selected := registeredcoredns.Image.Select(runtime.GOOS, runtime.GOARCH)
	if !selected {
		t.Fatal("test platform is absent from the CoreDNS catalog")
	}
	for _, mode := range []agentpb.ComponentLifecycleMode{
		agentpb.ComponentLifecycleMode_COMPONENT_LIFECYCLE_MODE_ENABLE,
		agentpb.ComponentLifecycleMode_COMPONENT_LIFECYCLE_MODE_UPDATE,
	} {
		t.Run(mode.String(), func(t *testing.T) {
			observer := &componentActionObserverStub{
				evidence: &agentpb.DNSResolverObservationEvidence{ComponentId: "cmp_exact"},
			}
			actionRuntime, runtimeErr := newRegisteredComponentActionRuntime(
				catalog,
				componentActionManagedExecutorStub{},
				componentActionComposeHelperStub{},
				observer,
			)
			if runtimeErr != nil {
				t.Fatal(runtimeErr)
			}
			step := &agentpb.ExecutionStep{
				StepId: "observe",
				Payload: &agentpb.ExecutionStep_ComponentApply{ComponentApply: &agentpb.ComponentApply{
					ComponentId: "cmp_exact", DefinitionDigest: definitionDigest[:], CatalogDigest: catalogDigest[:],
					ActionId: string(registeredcoredns.ObserveServingAction), ArtifactId: "cfg_exact",
					ArtifactDigest: digest[:], Generation: 7,
				}},
			}
			assignment := agent.Assignment{Plan: &agentpb.ExecutionPlan{
				Operation: agentpb.PlanOperation_PLAN_OPERATION_COMPONENT_APPLY, ComponentLifecycleMode: mode,
				Artifacts: []*agentpb.ComposeArtifact{{
					ProjectName: "groundplane-infra",
					Services: []*agentpb.ComposeService{{
						ServiceId: "svc_exact", ComposeName: registeredcoredns.ServiceName,
						ImageRepository:   registeredcoredns.Image.Repository,
						ImageIndexDigest:  componentActionTestDigest(t, registeredcoredns.Image.IndexDigest),
						ImageConfigDigest: componentActionTestDigest(t, platform.ConfigDigest),
						ImageChildDigest:  componentActionTestDigest(t, platform.ChildDigest),
						ImageReference:    imageReference, ImageOs: platform.OS,
						ImageArchitecture: platform.Architecture, ImageVariant: platform.Variant,
					}},
				}},
			}}
			result, executeErr := actionRuntime.ExecuteComponentAction(
				context.Background(), assignment, step, agent.ManagedConfigPayload{},
			)
			if executeErr != nil {
				t.Fatalf("ExecuteComponentAction() error = %v", executeErr)
			}
			if result == nil || result.DNSResolverObservation != observer.evidence ||
				result.ManagedConfig != nil || observer.request.ComponentID != "cmp_exact" ||
				observer.request.ArtifactSHA256 != digest || observer.request.RenderGeneration != 7 {
				t.Fatalf("observation request = %#v, result = %#v", observer.request, result)
			}
		})
	}
}

func TestRegisteredComponentActionRuntimeRecoversExactManagedConfigTransaction(t *testing.T) {
	candidate := sha256.Sum256([]byte("candidate Corefile"))
	previous := sha256.Sum256([]byte("previous Corefile"))
	request := &agentpb.ManagedConfigHelperRequest{
		Schema: 1, TransactionId: "mct_exact", Operation: agentpb.ManagedConfigOperation_MANAGED_CONFIG_OPERATION_PUBLISH,
		Sha256: candidate[:], ExpectedPreviousSha256: previous[:],
	}
	executor := &componentActionRecoveringManagedExecutor{response: &agentpb.ManagedConfigHelperResponse{
		Schema: 1, TransactionId: request.GetTransactionId(), Operation: request.GetOperation(),
		Disposition: agentpb.ManagedConfigReplayDisposition_MANAGED_CONFIG_REPLAY_DISPOSITION_RECOVERED,
		LiveSha256:  candidate[:], PreviousSha256: previous[:],
	}}
	actionRuntime := &registeredComponentActionRuntime{managedHelper: executor}
	state, err := actionRuntime.executeManagedConfig(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if executor.calls != 2 || !state.Live.Present || state.Live.SHA256 != candidate ||
		!state.Previous.Present || state.Previous.SHA256 != previous {
		t.Fatalf("managed-config recovery calls = %d, state = %#v", executor.calls, state)
	}
}

// Rationale: retry preserves the logical operation and sealed action but is a
// new Task attempt. Its managed-config transaction must therefore differ from
// a compensated predecessor while remaining stable across replay of one Task.
func TestManagedConfigRequestIdentityIsTaskAttemptScoped(t *testing.T) {
	t.Parallel()
	action := &agentpb.ComponentApply{
		ComponentId: "cmp_exact", ArtifactId: "cfg_exact", Generation: 7,
	}
	firstAssignment := agent.Assignment{OperationID: "op_exact", TaskID: "task_first"}
	first := managedConfigRequest(
		firstAssignment,
		action,
		"config/Corefile",
		agentpb.ManagedConfigOperation_MANAGED_CONFIG_OPERATION_PUBLISH,
	)
	replay := managedConfigRequest(
		firstAssignment,
		action,
		"config/Corefile",
		agentpb.ManagedConfigOperation_MANAGED_CONFIG_OPERATION_ROLLBACK,
	)
	retry := managedConfigRequest(
		agent.Assignment{OperationID: "op_exact", TaskID: "task_retry"},
		action,
		"config/Corefile",
		agentpb.ManagedConfigOperation_MANAGED_CONFIG_OPERATION_PUBLISH,
	)
	if first.GetTransactionId() != replay.GetTransactionId() {
		t.Fatalf(
			"one Task attempt changed transaction identity: %q != %q",
			first.GetTransactionId(),
			replay.GetTransactionId(),
		)
	}
	if first.GetTransactionId() == retry.GetTransactionId() {
		t.Fatalf("retry reused compensated transaction identity %q", first.GetTransactionId())
	}
}

func componentActionTestDigest(t *testing.T, value string) []byte {
	t.Helper()
	decoded, err := hex.DecodeString(value)
	if err != nil {
		t.Fatal(err)
	}
	return decoded
}
