package taskplanning

import (
	bytes "bytes"
	context "context"
	sha256 "crypto/sha256"
	hex "encoding/hex"
	json "encoding/json"
	slices "slices"
	testing "testing"

	componentsdk "github.com/AlanD20/groundplane-component-sdk/component"
	runner "github.com/AlanD20/groundplane/internal/common/runner"
	serviceproxy "github.com/AlanD20/groundplane/internal/common/serviceproxy"
	testcomponentrender "github.com/AlanD20/groundplane/internal/controller/componentrender"
	composeidentity "github.com/AlanD20/groundplane/internal/controller/composeidentity"
	fixtureowner "github.com/AlanD20/groundplane/internal/controller/composerender"
	core "github.com/AlanD20/groundplane/internal/core"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	composehelper "github.com/AlanD20/groundplane/internal/infra/docker/composehelper"
	etcd "github.com/AlanD20/groundplane/internal/infra/etcd"
	agentpb "github.com/AlanD20/groundplane/proto/agentpb"
	composetypes "github.com/compose-spec/compose-go/v2/types"
	proto "google.golang.org/protobuf/proto"
)

func testSelectedComponentImage(repository string) composeidentity.ComponentImage {
	image, platform, reference := controllerTestOCIPlatform(repository)
	return composeidentity.ComponentImage{
		Repository:  repository,
		IndexDigest: image.IndexDigest,
		Reference:   reference,
		Platform:    platform,
	}
}

func componentTestRegistration(
	t *testing.T,
	kind core.ComponentKind,
	plan testcomponentrender.EnvironmentComponentPlanFunc,
) testcomponentrender.EnvironmentComponentRegistration {
	t.Helper()
	action, err := componentsdk.NewActionDefinition(
		componentsdk.ActionID("activate-config"),
		componentsdk.CapabilityManagedConfig,
		componentsdk.OperationActivate,
	)
	if err != nil {
		t.Fatalf("NewActionDefinition() error = %v", err)
	}
	definition, err := componentsdk.NewDefinition(componentsdk.DefinitionInput{
		Implementation: componentsdk.ImplementationKey(kind),
		ConfigVariant:  componentsdk.ConfigVariant("test"),
		Provides:       []componentsdk.Capability{componentsdk.CapabilityServices},
		OwnerScopes:    []componentsdk.OwnerScope{componentsdk.OwnerScopeEnvironment},
		Actions:        []componentsdk.ActionDefinition{action},
	})
	if err != nil {
		t.Fatalf("NewDefinition() error = %v", err)
	}
	return testcomponentrender.EnvironmentComponentRegistration{
		Kind: kind, Definition: definition,
		CatalogDigest: sha256.Sum256([]byte("test Component catalog:" + string(kind))),
		Plan:          plan,
	}
}

// Rationale: the helper's public selector must validate a real Controller plan
// and select only the acknowledged workload/proxy, even when its rendered
// predecessor declares a dependency on another configured Service.
func assertRestorationStartupScope(t *testing.T, task etcd.TaskRecord, plan *agentpb.ExecutionPlan) {
	t.Helper()
	member := plan.GetCandidateReleaseProcedure().GetMembers()[0]
	for _, addressable := range []bool{false, true} {
		name := "portless"
		if addressable {
			name = "blue-green predecessor"
		}
		t.Run(name, func(t *testing.T) {
			project := &composetypes.Project{Services: composetypes.Services{
				"api": {Image: releaseTestWorkload("example/api:prior").LocalImageID,
					DependsOn: composetypes.DependsOnConfig{
						"configured": {Condition: "service_started", Required: true},
					}},
				"configured": {Image: "example/configured:1"},
			}}
			input := composeRenderTestInput(project)
			input.Identities.Services = []composeidentity.Resource{
				{ID: member.ServiceId, Name: "api"},
				{ID: "svc_01ARZ3NDEKTSV4RRFFQ69G5FAZ", Name: "configured"},
			}
			identity := fixtureowner.ComposeReleaseIdentity{
				ReleaseID: "dep_01ARZ3NDEKTSV4RRFFQ69G5FAV", Image: project.Services["api"].Image,
				Strategy: domain.StrategyRecreate, Target: domain.WorkloadSingleton,
				ServingTarget: domain.WorkloadSingleton, ServingReleaseID: "dep_01ARZ3NDEKTSV4RRFFQ69G5FAV",
				ServingProxyGeneration: 1,
			}
			want := []string{"api"}
			if addressable {
				api := project.Services["api"]
				api.Expose = []string{"8080"}
				project.Services["api"] = api
				identity.Strategy, identity.Target, identity.ServingTarget = domain.StrategyBlueGreen, domain.WorkloadBlue, domain.WorkloadBlue
				identity.ProxyImage = testServiceProxyImage()
				want = []string{"api", "api--blue"}
			}
			input.Releases = map[string]fixtureowner.ComposeReleaseIdentity{member.ServiceId: identity}
			predecessor, err := fixtureowner.RenderCompose(input)
			if err != nil {
				t.Fatal(err)
			}
			encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(predecessor)
			if err != nil {
				t.Fatal(err)
			}
			digest := sha256.Sum256(encoded)
			request := &agentpb.ComposeHelperRequest{
				Schema: composehelper.SchemaVersion, AssignmentId: "asgn_01ARZ3NDEKTSV4RRFFQ69G5FAV",
				TaskId: task.ID, OperationId: task.OperationID, Plan: plan, TimeoutSeconds: 30,
				StepId: member.ServingPredecessor.CompensateStepId,
				RestorationAuthority: &agentpb.ReleaseRestorationAuthority{
					TaskId: task.ID, OperationId: task.OperationID, PlanHash: slices.Clone(plan.PlanHash), AuthoritySha256: make([]byte, 32),
					EnvironmentId: task.Owner.EnvironmentID, CandidateArtifactId: member.CandidateArtifactId,
					Candidates: []*agentpb.ReleaseRestorationCandidate{
						{ServiceId: member.ServiceId, ReleaseId: member.CandidateReleaseId,
							Target: agentpb.ReleaseRestorationTarget_RELEASE_RESTORATION_TARGET_SERVING_PREDECESSOR},
					},
					AppliedPredecessor: &agentpb.ReleaseAppliedPredecessorAuthority{
						KeyRevision: 1, RevisionId: task.ID, RenderGeneration: 7, ComposeArtifact: encoded, ComposeArtifactSha256: digest[:],
					},
				},
			}
			selected, err := composehelper.StartupServices(request)
			if err != nil {
				t.Fatal(err)
			}
			var names []string
			for _, service := range selected {
				names = append(names, service.ComposeName)
			}
			slices.Sort(names)
			if !slices.Equal(names, want) {
				t.Fatalf("sealed restoration startup = %v, want %v", names, want)
			}
			if addressable {
				assertRestorationProxyProbe(t, request, predecessor)
			}
		})
	}
}

// Rationale: exact healthy container identities do not prove the proxy's
// active route. A real rendered prior config must be independently observed.
func assertRestorationProxyProbe(
	t *testing.T,
	request *agentpb.ComposeHelperRequest,
	predecessor *agentpb.ComposeArtifact,
) {
	t.Helper()
	for _, test := range []struct {
		name                                                 string
		compensate, activeMatches, reloadWorks, wantComplete bool
	}{
		{name: "wrong active probe"},
		{name: "matching active probe", activeMatches: true, wantComplete: true},
		{name: "reload observed", compensate: true, reloadWorks: true, wantComplete: true},
		{name: "reload has no effect", compensate: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			assertRestorationProxyScenario(
				t,
				request,
				predecessor,
				test.compensate,
				test.activeMatches,
				test.reloadWorks,
				test.wantComplete,
			)
		})
	}
}

func assertRestorationProxyScenario(
	t *testing.T,
	request *agentpb.ComposeHelperRequest,
	predecessor *agentpb.ComposeArtifact,
	compensate, activeMatches, reloadWorks, wantComplete bool,
) {
	t.Helper()
	request = proto.CloneOf(request)
	request.StepId = request.Plan.CandidateReleaseProcedure.Members[0].ServingPredecessor.ProbeStepId
	if compensate {
		request.StepId = request.Plan.CandidateReleaseProcedure.Members[0].ServingPredecessor.CompensateStepId
	}
	services := make(map[string]*agentpb.ComposeService)
	for _, service := range predecessor.Services {
		if service.Role == agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY {
			services["bbbbbbbbbbbbbbbb"] = service
		} else if labelPairMap(service.ExpectedLabels)[fixtureowner.ComposeLabelReleaseID] != "" {
			services["aaaaaaaaaaaaaaaa"] = service
		}
	}
	proxy := services["bbbbbbbbbbbbbbbb"]
	reads, reloads := 0, 0
	fake := runner.NewFake()
	fake.RunFunc = func(_ context.Context, options runner.RunCmdOpts) (runner.Result, error) {
		if options.Args[0] == "exec" {
			if slices.Equal(options.Args, []string{"exec", "bbbbbbbbbbbbbbbb", "cat", "/proc/1/cmdline"}) {
				return runner.Result{Stdout: []byte(serviceproxy.RunningCommand)}, nil
			}
			if slices.Contains(options.Args, serviceproxy.Activate) {
				if !compensate ||
					!slices.Equal(
						options.Args,
						[]string{"exec", "--interactive", "bbbbbbbbbbbbbbbb", "sh", "-ec", serviceproxy.Activate},
					) ||
					!bytes.Equal(options.Stdin, proxy.ProxyConfigJson) {
					t.Fatalf("unauthorized reload: %v", options.Args)
				}
				reloads++
				activeMatches = reloadWorks
				return runner.Result{}, nil
			}
			if slices.Equal(options.Args, []string{"exec", "bbbbbbbbbbbbbbbb", "cat", serviceproxy.RuntimeConfigPath}) {
				if activeMatches {
					return runner.Result{Stdout: slices.Clone(proxy.ProxyConfigJson)}, nil
				}
				return runner.Result{Stdout: []byte(`{"apps":{}}`)}, nil
			}
			if !slices.Equal(
				options.Args,
				[]string{
					"exec",
					"bbbbbbbbbbbbbbbb",
					"wget",
					"--quiet",
					"--output-document=-",
					"http://127.0.0.1:2019/config/",
				},
			) {
				t.Fatalf("unexpected proxy command: %v", options.Args)
			}
			reads++
			if activeMatches {
				return runner.Result{Stdout: slices.Clone(proxy.ProxyConfigJson)}, nil
			}
			return runner.Result{Stdout: []byte(`{"apps":{}}`)}, nil
		}
		if options.Args[0] != "container" {
			t.Fatalf("read-only restoration probe mutated runtime: %v", options.Args)
		}
		if options.Args[1] == "ls" {
			return runner.Result{Stdout: []byte("aaaaaaaaaaaaaaaa\nbbbbbbbbbbbbbbbb\n")}, nil
		}
		service := services[options.Args[len(options.Args)-1]]
		if service == nil {
			t.Fatalf("unknown container: %v", options.Args)
		}
		switch options.Args[3] {
		case "{{json .Config.Labels}}":
			labels := labelPairMap(service.ExpectedLabels)
			labels["com.docker.compose.project"], labels["com.docker.compose.service"] = predecessor.ProjectName, service.ComposeName
			encoded, err := json.Marshal(labels)
			if err != nil {
				t.Fatal(err)
			}
			return runner.Result{Stdout: encoded}, nil
		case "{{.Config.Image}}":
			return runner.Result{Stdout: []byte(service.ImageReference)}, nil
		case "{{.Config.Image}}\n{{.Image}}":
			return runner.Result{Stdout: []byte(service.ImageReference + "\n" + service.ImageReference)}, nil
		case "{{.Config.Image}}\n{{.Image}}\n{{json .ImageManifestDescriptor}}":
			return runner.Result{
				Stdout: []byte(
					service.ImageReference + "\nsha256:" + hex.EncodeToString(service.ImageConfigDigest) + "\nnull",
				),
			}, nil
		case "{{json .State}}":
			return runner.Result{Stdout: []byte(`{"Running":true,"Health":{"Status":"healthy"}}`)}, nil
		}
		t.Fatalf("unexpected observation: %v", options.Args)
		return runner.Result{}, nil
	}
	response, err := composehelper.Execute(context.Background(), fake, request)
	wantOutcome := agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_FAILED
	if !compensate && !wantComplete {
		wantOutcome = agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_RESTORATION_REQUIRED
	}
	if wantComplete {
		wantOutcome = agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_COMPLETED
	}
	if err != nil || response.GetOutcome() != wantOutcome {
		t.Fatalf("active proxy result = %v, %v, want %v", response, err, wantOutcome)
	}
	if reads == 0 || compensate && (reloads != 1 || reads != 2) || !compensate && reloads != 0 {
		t.Fatalf("active config reads=%d reloads=%d", reads, reloads)
	}
	if wantComplete && !bytes.Equal(response.GetProxyEvidence().GetConfigSha256(), proxy.ProxyConfigSha256) {
		t.Fatal("completed proxy proof differs from observed sealed configuration")
	}
}

func labelPairMap(pairs []*agentpb.LabelPair) map[string]string {
	labels := make(map[string]string, len(pairs))
	for _, pair := range pairs {
		labels[pair.Key] = pair.Value
	}
	return labels
}
