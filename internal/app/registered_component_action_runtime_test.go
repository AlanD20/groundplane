package app

import (
	context "context"
	sha256 "crypto/sha256"
	hex "encoding/hex"
	errors "errors"
	runtime "runtime"
	testing "testing"

	registeredcoredns "github.com/AlanD20/groundplane-registered-components/coredns"
	testcomponentaction "github.com/AlanD20/groundplane/internal/agent/componentaction"
	testtaskassignment "github.com/AlanD20/groundplane/internal/agent/taskassignment"
	"github.com/AlanD20/groundplane/internal/app/componentregistration"
	dnsresolverobserver "github.com/AlanD20/groundplane/internal/infra/docker/dnsresolverobserver"
	managedconfighelpercontainer "github.com/AlanD20/groundplane/internal/infra/docker/managedconfighelpercontainer"
	agentpb "github.com/AlanD20/groundplane/proto/agentpb"
)

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
	catalog, err := componentregistration.NewCatalog()
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
			actionRuntime, runtimeErr := testcomponentaction.New(
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
			assignment := testtaskassignment.Assignment{Plan: &agentpb.ExecutionPlan{
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
				context.Background(), assignment, step, testcomponentaction.ManagedConfigPayload{},
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

func componentActionTestDigest(t *testing.T, value string) []byte {
	t.Helper()
	decoded, err := hex.DecodeString(value)
	if err != nil {
		t.Fatal(err)
	}
	return decoded
}
