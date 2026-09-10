package composehelper

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	component "github.com/AlanD20/groundplane-component-sdk/component"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

const (
	helperTaskID                 = "task_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	helperOperationID            = "op_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	helperPlanID                 = "plan_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	helperArtifactID             = "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	helperServiceID              = "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	helperComponentID            = "cmp_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	helperStepID                 = "step_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	helperEnvironmentID          = "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	helperTenantID               = "tnt_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	helperProjectID              = "prj_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	componentConfigRelativePath  = "components/caddy/Caddyfile"
	componentConfigContainerPath = "/etc/caddy/Caddyfile"
	componentImageReference      = "docker.io/library/caddy@sha256:5f5c8640aae01df9654968d946d8f1a56c497f1dd5c5cda4cf95ab7c14d58648"
	componentMaterializeStepID   = "step_01ARZ3NDEKTSV4RRFFQ69G5FAW"
	componentComposeApplyStepID  = "step_01ARZ3NDEKTSV4RRFFQ69G5FAX"
)

type componentCatalogFake struct{}

func (componentCatalogFake) ResolveContainerConfigAction(*agentpb.ComponentApply) (ComponentActionRecipe, error) {
	return NewComponentActionRecipe(
		componentConfigRelativePath, componentConfigContainerPath, componentImageReference, component.OCIPlatform{
			OS: "linux", Architecture: "amd64", ChildDigest: strings.Split(componentImageReference, "@sha256:")[1], ConfigDigest: strings.Repeat("b", 64),
		},
		[]string{"caddy", "validate", "--config", componentConfigContainerPath, "--adapter", "caddyfile"},
		[]string{"caddy", "reload", "--config", componentConfigContainerPath, "--adapter", "caddyfile"},
	)
}

// Rationale: helper stdin is one exact bounded frame; concatenated or trailing
// request bytes must never be interpreted as another instruction.
func TestRequestFrameRoundTripsAndRejectsTrailingBytes(t *testing.T) {
	request := validRequest(t)
	framed, err := MarshalRequest(request)
	if err != nil {
		t.Fatalf("MarshalRequest() error = %v", err)
	}
	decoded, err := ReadRequest(context.Background(), bytes.NewReader(framed))
	if err != nil {
		t.Fatalf("ReadRequest() error = %v", err)
	}
	if !reflect.DeepEqual(decoded, request) {
		t.Fatalf("decoded request differs: got=%#v want=%#v", decoded, request)
	}
	framed = append(framed, 0)
	if _, err := ReadRequest(
		context.Background(),
		bytes.NewReader(framed),
	); !errors.Is(
		err,
		errs.New(errs.KindValidationFailed, ""),
	) {
		t.Fatalf("ReadRequest(trailing) error = %v, want validation.failed", err)
	}
}

// Rationale: apply must validate and mutate the same canonical stdin with a
// fixed workdir, explicit project, and no inherited process environment.
func TestExecuteApplyUsesExactComposeProcedure(t *testing.T) {
	fake := runner.NewFake()
	fake.Results[DockerExecutable] = runner.Result{ExitCode: 0}
	request := validRequest(t)
	response, err := Execute(context.Background(), fake, request)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if response.Outcome != agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_COMPLETED ||
		response.Diagnostic != agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_NONE {
		t.Fatalf("response = %#v", response)
	}
	if len(fake.Calls) != 2 {
		t.Fatalf("Runner calls = %d, want config and up", len(fake.Calls))
	}
	prefix := []string{
		"compose", "--project-name", "groundplane-infra",
		"--project-directory", WorkDirectory, "--file", "-",
	}
	wantArgs := [][]string{
		append(append([]string(nil), prefix...), "config", "--quiet", "--no-interpolate"),
		append(append([]string(nil), prefix...), "up", "--detach", "api"),
	}
	wantEnvironment := []string{
		"PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
		"HOME=/nonexistent",
		"DOCKER_HOST=unix:///var/run/docker.sock",
		"COMPOSE_DISABLE_ENV_FILE=1",
		"COMPOSE_PARALLEL_LIMIT=1",
	}
	for index, call := range fake.Calls {
		if call.Name != DockerExecutable || call.Dir != WorkDirectory || !call.ReplaceEnv ||
			!reflect.DeepEqual(call.Env, wantEnvironment) || !reflect.DeepEqual(call.Args, wantArgs[index]) ||
			!bytes.Equal(call.Stdin, request.Plan.Artifacts[0].CanonicalYaml) {
			t.Fatalf("Runner call %d = %#v", index, call)
		}
	}
}

func TestExecuteEmptyFullReconcileUsesComposeDown(t *testing.T) {
	fake := runner.NewFake()
	fake.Results[DockerExecutable] = runner.Result{ExitCode: 0}
	request := validRequest(t)
	request.Plan.PlanHash = nil
	request.Plan.Artifacts[0].Services[0].ExpectedReplicas = 0
	request.Plan.Artifacts[0].CanonicalYaml = []byte(
		"services:\n  api:\n    image: example/api:latest\n    profiles: [configured]\n",
	)
	digest := sha256.Sum256(request.Plan.Artifacts[0].CanonicalYaml)
	request.Plan.Artifacts[0].YamlSha256 = digest[:]
	request.Plan.Steps[0].Payload = &agentpb.ExecutionStep_ComposeApply{ComposeApply: &agentpb.ComposeApply{
		ArtifactId: helperArtifactID, FullReconcile: true,
	}}
	sealed, err := executionplan.Seal(request.Plan)
	if err != nil {
		t.Fatalf("Seal() error = %v", err)
	}
	request.Plan = sealed
	response, err := Execute(context.Background(), fake, request)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if response.Outcome != agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_COMPLETED || len(fake.Calls) != 2 {
		t.Fatalf("response = %#v, Runner calls = %d", response, len(fake.Calls))
	}
	want := []string{
		"compose", "--project-name", "groundplane-infra",
		"--project-directory", WorkDirectory, "--file", "-", "down", "--remove-orphans",
	}
	if !reflect.DeepEqual(fake.Calls[1].Args, want) {
		t.Fatalf("empty reconcile mutation = %#v", fake.Calls[1])
	}
}

func TestExecuteWholeProjectRemovalRemovesNamedVolumes(t *testing.T) {
	fake := runner.NewFake()
	fake.Results[DockerExecutable] = runner.Result{ExitCode: 0}
	request := validRequest(t)
	request.Plan.PlanHash = nil
	request.Plan.Operation = agentpb.PlanOperation_PLAN_OPERATION_REMOVE
	request.Plan.Steps[0].Payload = &agentpb.ExecutionStep_ComposeRemove{ComposeRemove: &agentpb.ComposeRemove{
		ArtifactId: helperArtifactID, WholeProject: true,
	}}
	sealed, err := executionplan.Seal(request.Plan)
	if err != nil {
		t.Fatalf("Seal() error = %v", err)
	}
	request.Plan = sealed
	response, err := Execute(context.Background(), fake, request)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if response.Outcome != agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_COMPLETED || len(fake.Calls) != 1 {
		t.Fatalf("response = %#v, Runner calls = %d", response, len(fake.Calls))
	}
	want := []string{
		"compose", "--project-name", "groundplane-infra",
		"--project-directory", WorkDirectory, "--file", "-", "down", "--remove-orphans", "--volumes",
	}
	if !reflect.DeepEqual(fake.Calls[0].Args, want) {
		t.Fatalf("whole-project removal mutation = %#v", fake.Calls[0])
	}
}

func TestComposeProjectDirectoryUsesAuthorizedEnvironmentVolumeDirectory(t *testing.T) {
	artifact := &agentpb.ComposeArtifact{AuthorizedVolumeDir: "/var/lib/groundplane/vol/tenant/project/environment"}
	if got := composeProjectDirectory(artifact); got != artifact.AuthorizedVolumeDir {
		t.Fatalf("composeProjectDirectory() = %q, want %q", got, artifact.AuthorizedVolumeDir)
	}
	if got := composeProjectDirectory(&agentpb.ComposeArtifact{}); got != WorkDirectory {
		t.Fatalf("composeProjectDirectory(platform) = %q, want %q", got, WorkDirectory)
	}
}

// Rationale: a config rejection is a closed diagnostic and must prevent the
// mutating Compose command without returning raw stderr.
func TestExecuteConfigFailureStopsBeforeMutation(t *testing.T) {
	fake := runner.NewFake()
	fake.RunFunc = func(context.Context, runner.RunCmdOpts) (runner.Result, error) {
		return runner.Result{ExitCode: 15, Stderr: []byte("secret-like raw diagnostic")}, errors.New("exit status 15")
	}
	response, err := Execute(context.Background(), fake, validRequest(t))
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if len(fake.Calls) != 1 || response.ExitCode != 15 ||
		response.Diagnostic != agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_CONFIG_REJECTED {
		t.Fatalf("calls=%d response=%#v", len(fake.Calls), response)
	}
}

// Rationale: health polling requires the observation port and cannot be
// smuggled through the Compose CLI helper before that contract exists.
func TestMarshalRequestRejectsWaitHealthy(t *testing.T) {
	request := validRequest(t)
	request.Plan.PlanHash = nil
	request.Plan.Steps[0].Payload = &agentpb.ExecutionStep_WaitHealthy{WaitHealthy: &agentpb.WaitHealthy{
		ArtifactId: helperArtifactID, ServiceIds: []string{helperServiceID},
	}}
	sealed, err := executionplan.Seal(request.Plan)
	if err != nil {
		t.Fatalf("seal wait plan: %v", err)
	}
	request.Plan = sealed
	if _, err := MarshalRequest(request); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("MarshalRequest(wait healthy) error = %v, want validation.failed", err)
	}
}

// Rationale: the helper must prove the exact catalog-generated configuration
// and managed read-only mount before invoking the catalog-owned validation and
// activation recipe in the uniquely running Component container.
func TestExecuteComponentConfigValidatesBeforeActivation(t *testing.T) {
	request, configPath := validComponentConfigRequest(t)
	fake := componentConfigRunner(t, configPath, 0)
	response, err := ExecuteWithComponentCatalog(context.Background(), fake, request, componentCatalogFake{})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if response.Outcome != agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_COMPLETED ||
		response.Diagnostic != agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_NONE {
		t.Fatalf("response = %#v", response)
	}
	if len(fake.Calls) != 8 {
		t.Fatalf("Runner calls = %d, want lookup, three inspections, two hashes, validate, activate", len(fake.Calls))
	}
	if !reflect.DeepEqual(fake.Calls[5].Args, []string{
		"container", "exec", "0123456789abcdef", "caddy", "validate",
		"--config", componentConfigContainerPath, "--adapter", "caddyfile",
	}) || !reflect.DeepEqual(fake.Calls[7].Args, []string{
		"container", "exec", "0123456789abcdef", "caddy", "reload",
		"--config", componentConfigContainerPath, "--adapter", "caddyfile",
	}) {
		t.Fatalf("Component action calls = %#v", fake.Calls[5:])
	}
}

// Rationale: Blueprint plans may carry a retained prior artifact alongside
// the candidate; the helper must execute against the materialization-bound
// candidate while retaining unique Component ownership checks.
func TestArtifactForRequestBlueprintComponentUsesMaterializedArtifact(t *testing.T) {
	request := blueprintComponentRequestWithRetainedPrior(t)
	request.Plan.Artifacts = []*agentpb.ComposeArtifact{request.Plan.Artifacts[1], request.Plan.Artifacts[0]}
	if err := sealComponentRequest(request); err != nil {
		t.Fatalf("Seal() error = %v", err)
	}

	artifact, err := ArtifactForRequest(request)
	if err != nil {
		t.Fatalf("ArtifactForRequest() error = %v", err)
	}
	if artifact == nil || artifact.ArtifactId != helperArtifactID {
		t.Fatalf("ArtifactForRequest() artifact = %#v, want bound candidate %q", artifact, helperArtifactID)
	}
}

// Rationale: helper-side selection must preserve the execution-plan fences for
// missing or ambiguous chains, selected-service ownership, and ordinary plans.
func TestArtifactForRequestBlueprintComponentRejectsInvalidSelection(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*agentpb.ComposeHelperRequest)
	}{
		{name: "missing materialization", mutate: func(request *agentpb.ComposeHelperRequest) {
			request.Plan.Steps = request.Plan.Steps[1:]
		}},
		{name: "detached chain", mutate: func(request *agentpb.ComposeHelperRequest) {
			request.Plan.Steps[1].PrerequisiteStepId = ""
		}},
		{name: "ambiguous materialization", mutate: func(request *agentpb.ComposeHelperRequest) {
			duplicate := proto.CloneOf(request.Plan.Steps[0])
			duplicate.StepId = "step_01ARZ3NDEKTSV4RRFFQ69FAX"
			request.Plan.Steps = append(request.Plan.Steps, duplicate)
		}},
		{name: "duplicate selected service", mutate: func(request *agentpb.ComposeHelperRequest) {
			duplicate := proto.CloneOf(request.Plan.Artifacts[0].Services[0])
			request.Plan.Artifacts[0].Services = append(request.Plan.Artifacts[0].Services, duplicate)
		}},
		{name: "ordinary global ownership", mutate: func(request *agentpb.ComposeHelperRequest) {
			request.Plan.Operation = agentpb.PlanOperation_PLAN_OPERATION_RECONCILE
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := blueprintComponentRequestWithRetainedPrior(t)
			test.mutate(request)
			if err := sealComponentRequest(request); err != nil {
				return
			}
			if _, err := ArtifactForRequest(request); err == nil {
				t.Fatal("ArtifactForRequest() accepted invalid Component selection")
			}
		})
	}
}

func blueprintComponentRequestWithRetainedPrior(t *testing.T) *agentpb.ComposeHelperRequest {
	t.Helper()
	request, _ := validComponentConfigRequest(t)
	request.Plan.PlanHash = nil
	request.Plan.Operation = agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY
	prior := proto.CloneOf(request.Plan.Artifacts[0])
	prior.ArtifactId = "cfg_01ARZ3NDEKTSV4RRFFQ69G5FBG"
	prior.Services[0].ServiceId = "svc_01ARZ3NDEKTSV4RRFFQ69G5FBG"
	for _, label := range prior.Services[0].ExpectedLabels {
		if label.Key == "com.groundplane.service-id" {
			label.Value = prior.Services[0].ServiceId
		}
	}
	request.Plan.Artifacts = append(request.Plan.Artifacts, prior)
	return request
}

func sealComponentRequest(request *agentpb.ComposeHelperRequest) error {
	sealed, err := executionplan.Seal(request.Plan)
	if err != nil {
		return err
	}
	request.Plan = sealed
	return nil
}

// Rationale: rejected Component configuration must be reported as a bounded
// generic diagnostic and must never reach the activation command.
func TestExecuteComponentConfigRejectionStopsBeforeActivation(t *testing.T) {
	request, configPath := validComponentConfigRequest(t)
	fake := componentConfigRunner(t, configPath, 15)
	response, err := ExecuteWithComponentCatalog(context.Background(), fake, request, componentCatalogFake{})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if len(fake.Calls) != 6 || response.ExitCode != 15 ||
		response.Diagnostic != agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_COMPONENT_CONFIG_REJECTED {
		t.Fatalf("calls=%d response=%#v", len(fake.Calls), response)
	}
}

func TestExecuteComponentConfigRejectsForgedRuntimeAuthorityBeforeActivation(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*runner.FakeRunner)
	}{
		{name: "incomplete labels", mutate: func(fake *runner.FakeRunner) {
			base := fake.RunFunc
			fake.RunFunc = func(ctx context.Context, options runner.RunCmdOpts) (runner.Result, error) {
				if strings.Contains(strings.Join(options.Args, " "), ".Config.Labels") {
					return runner.Result{Stdout: []byte("{}\n")}, nil
				}
				return base(ctx, options)
			}
		}},
		{name: "wrong immutable image", mutate: func(fake *runner.FakeRunner) {
			base := fake.RunFunc
			fake.RunFunc = func(ctx context.Context, options runner.RunCmdOpts) (runner.Result, error) {
				if strings.Contains(strings.Join(options.Args, " "), ".Config.Image") {
					return runner.Result{
						Stdout: []byte(
							"docker.io/library/caddy@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\n",
						),
					}, nil
				}
				return base(ctx, options)
			}
		}},
		{name: "container bytes replaced after validation", mutate: func(fake *runner.FakeRunner) {
			base := fake.RunFunc
			hashes := 0
			fake.RunFunc = func(ctx context.Context, options runner.RunCmdOpts) (runner.Result, error) {
				if strings.Contains(strings.Join(options.Args, " "), " sha256sum ") {
					hashes++
					if hashes == 2 {
						return runner.Result{
							Stdout: []byte(strings.Repeat("a", 64) + "  " + componentConfigContainerPath + "\n"),
						}, nil
					}
				}
				return base(ctx, options)
			}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request, configPath := validComponentConfigRequest(t)
			fake := componentConfigRunner(t, configPath, 0)
			test.mutate(fake)
			response, err := ExecuteWithComponentCatalog(
				context.Background(), fake, request, componentCatalogFake{},
			)
			if err != nil {
				t.Fatalf("ExecuteWithComponentCatalog() error = %v", err)
			}
			if response.GetOutcome() != agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_FAILED {
				t.Fatalf("response = %#v", response)
			}
			for _, call := range fake.Calls {
				if strings.Contains(strings.Join(call.Args, " "), " caddy reload ") {
					t.Fatalf("activation executed with forged runtime authority: %#v", call)
				}
			}
		})
	}
}

func TestRouteRemovalTargetedApplyRefreshesRuntimeBeforeComponentActivation(t *testing.T) {
	request, configPath := validComponentConfigRequest(t)
	request.Plan.PlanHash = nil
	request.Plan.Operation = agentpb.PlanOperation_PLAN_OPERATION_REMOVE
	request.Plan.RenderGeneration = 2
	actionStep := request.Plan.Steps[1]
	actionStep.GetComponentApply().Generation = 2
	for _, label := range request.Plan.Artifacts[0].Services[0].ExpectedLabels {
		if label.GetKey() == "com.groundplane.render-generation" {
			label.Value = "2"
		}
	}
	composeStep := &agentpb.ExecutionStep{
		StepId: componentComposeApplyStepID, TimeoutSeconds: 30,
		PrerequisiteStepId: componentMaterializeStepID,
		Payload: &agentpb.ExecutionStep_ComposeApply{ComposeApply: &agentpb.ComposeApply{
			ArtifactId: helperArtifactID, ServiceIds: []string{helperServiceID},
			ForceRecreate: true, NoDependencies: true,
		}},
	}
	actionStep.PrerequisiteStepId = componentComposeApplyStepID
	request.Plan.Steps = []*agentpb.ExecutionStep{request.Plan.Steps[0], composeStep, actionStep}
	sealed, err := executionplan.Seal(request.Plan)
	if err != nil {
		t.Fatalf("Seal(Route removal plan) error = %v", err)
	}
	request.Plan = sealed

	fake := componentConfigRunner(t, configPath, 0)
	base := fake.RunFunc
	applied := false
	fake.RunFunc = func(ctx context.Context, options runner.RunCmdOpts) (runner.Result, error) {
		joined := strings.Join(options.Args, " ")
		if strings.Contains(joined, "compose ") && strings.Contains(joined, " up ") {
			applied = true
		}
		if strings.Contains(joined, ".Config.Labels") {
			generation := "1"
			if applied {
				generation = "2"
			}
			labels := map[string]string{}
			for _, label := range sealed.Artifacts[0].Services[0].ExpectedLabels {
				labels[label.GetKey()] = label.GetValue()
			}
			labels["com.groundplane.render-generation"] = generation
			encoded, marshalErr := json.Marshal(labels)
			if marshalErr != nil {
				return runner.Result{}, marshalErr
			}
			return runner.Result{Stdout: append(encoded, '\n')}, nil
		}
		return base(ctx, options)
	}
	request.StepId = componentComposeApplyStepID
	if response, executeErr := Execute(context.Background(), fake, request); executeErr != nil ||
		response.GetOutcome() != agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_COMPLETED {
		t.Fatalf("Execute(targeted Caddy apply) = %#v, %v", response, executeErr)
	}
	request.StepId = helperStepID
	if response, executeErr := ExecuteWithComponentCatalog(
		context.Background(), fake, request, componentCatalogFake{},
	); executeErr != nil || response.GetOutcome() != agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_COMPLETED {
		t.Fatalf("ExecuteWithComponentCatalog(activation) = %#v, %v", response, executeErr)
	}
	applyCall := -1
	activateCall := -1
	for index, call := range fake.Calls {
		joined := strings.Join(call.Args, " ")
		if strings.Contains(joined, "compose ") && strings.Contains(joined, " up ") {
			applyCall = index
		}
		if strings.Contains(joined, " caddy reload ") {
			activateCall = index
		}
	}
	if !applied || applyCall < 0 || activateCall <= applyCall {
		t.Fatalf("Route removal call order = apply %d activate %d calls %#v", applyCall, activateCall, fake.Calls)
	}
}

func validComponentConfigRequest(t *testing.T) (*agentpb.ComposeHelperRequest, string) {
	t.Helper()
	root := filepath.Join(t.TempDir(), helperTenantID, helperProjectID, helperEnvironmentID)
	configPath := filepath.Join(root, filepath.FromSlash(componentConfigRelativePath))
	if err := os.MkdirAll(filepath.Dir(configPath), 0o700); err != nil {
		t.Fatalf("MkdirAll() error = %v", err)
	}
	config := []byte("http:// {\n\trespond 404\n}\n")
	if err := os.WriteFile(configPath, config, 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	digest := sha256.Sum256(config)
	definitionDigest := sha256.Sum256([]byte("test Component definition"))
	catalogDigest := sha256.Sum256([]byte("test Component catalog"))
	request := validRequest(t)
	request.Plan.PlanHash = nil
	request.Plan.Operation = agentpb.PlanOperation_PLAN_OPERATION_RECONCILE
	request.Plan.TargetId = helperEnvironmentID
	request.Plan.Artifacts[0].OwnerKind = agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT
	request.Plan.Artifacts[0].OwnerId = helperEnvironmentID
	request.Plan.Artifacts[0].ProjectName = "gp-" + strings.ToLower(helperEnvironmentID)
	request.Plan.Artifacts[0].AuthorizedVolumeDir = root
	request.Plan.Artifacts[0].Services[0].ComposeName = "caddy"
	request.Plan.Artifacts[0].Services[0].ExpectedLabels = []*agentpb.LabelPair{
		{Key: "com.groundplane.component-id", Value: helperComponentID},
		{Key: "com.groundplane.environment-id", Value: helperEnvironmentID},
		{Key: "com.groundplane.kind", Value: "service"},
		{Key: "com.groundplane.managed", Value: "true"},
		{Key: "com.groundplane.plan-id", Value: helperPlanID},
		{Key: "com.groundplane.render-generation", Value: "1"},
		{Key: "com.groundplane.service-id", Value: helperServiceID},
	}
	request.Plan.Artifacts[0].Services[0].OwnerComponentId = helperComponentID
	service := request.Plan.Artifacts[0].Services[0]
	service.ImageReference = componentImageReference
	service.ImageRepository = strings.Split(componentImageReference, "@")[0]
	service.ImageIndexDigest = bytes.Repeat([]byte{0xa}, sha256.Size)
	service.ImageChildDigest, _ = hex.DecodeString(strings.Split(componentImageReference, "@sha256:")[1])
	service.ImageConfigDigest = bytes.Repeat([]byte{0xbb}, sha256.Size)
	service.ImageOs, service.ImageArchitecture = "linux", "amd64"
	service.ExpectedLabels = append(service.ExpectedLabels[:2], append([]*agentpb.LabelPair{
		{Key: "com.groundplane.image-child-digest", Value: "sha256:" + hex.EncodeToString(service.ImageChildDigest)},
		{Key: "com.groundplane.image-config-digest", Value: "sha256:" + hex.EncodeToString(service.ImageConfigDigest)},
		{Key: "com.groundplane.image-index-digest", Value: "sha256:" + hex.EncodeToString(service.ImageIndexDigest)},
		{Key: "com.groundplane.image-platform", Value: "linux/amd64"},
	}, service.ExpectedLabels[2:]...)...)
	request.Plan.Steps = []*agentpb.ExecutionStep{{
		StepId: componentMaterializeStepID, TimeoutSeconds: 30,
		Payload: &agentpb.ExecutionStep_MaterializeFile{MaterializeFile: &agentpb.MaterializeFile{
			ArtifactId: helperArtifactID, MaterializationId: helperArtifactID,
			EnvironmentId: helperEnvironmentID, Destination: componentConfigRelativePath,
			OutputKind: agentpb.MaterializationOutputKind_MATERIALIZATION_OUTPUT_KIND_PLAIN_FILE,
			Mode:       0o444, Length: uint64(len(config)), Sha256: digest[:],
		}},
	}, {
		StepId: helperStepID, TimeoutSeconds: 30, PrerequisiteStepId: componentMaterializeStepID,
		Payload: &agentpb.ExecutionStep_ComponentApply{
			ComponentApply: &agentpb.ComponentApply{
				ComponentId: helperComponentID, DefinitionDigest: definitionDigest[:],
				CatalogDigest: catalogDigest[:], ActionId: "activate-config",
				ArtifactId: helperArtifactID, ArtifactDigest: digest[:], Generation: 1,
			},
		},
	}}
	sealed, err := executionplan.Seal(request.Plan)
	if err != nil {
		t.Fatalf("Seal() error = %v", err)
	}
	request.Plan = sealed
	return request, configPath
}

func componentConfigRunner(t *testing.T, configPath string, validationExit int) *runner.FakeRunner {
	t.Helper()
	labels, err := json.Marshal(map[string]string{
		"com.groundplane.managed": "true", "com.groundplane.kind": "service",
		"com.groundplane.component-id":        helperComponentID,
		"com.groundplane.environment-id":      helperEnvironmentID,
		"com.groundplane.service-id":          helperServiceID,
		"com.groundplane.plan-id":             helperPlanID,
		"com.groundplane.render-generation":   "1",
		"com.groundplane.image-child-digest":  strings.Split(componentImageReference, "@")[1],
		"com.groundplane.image-config-digest": "sha256:" + strings.Repeat("b", 64),
		"com.groundplane.image-index-digest":  "sha256:" + strings.Repeat("0a", 32),
		"com.groundplane.image-platform":      "linux/amd64",
	})
	if err != nil {
		t.Fatalf("Marshal(labels) error = %v", err)
	}
	mounts, err := json.Marshal([]componentConfigMount{{
		Type: "bind", Source: filepath.Dir(configPath), Destination: filepath.Dir(componentConfigContainerPath), RW: false,
	}})
	if err != nil {
		t.Fatalf("Marshal(mounts) error = %v", err)
	}
	fake := runner.NewFake()
	fake.RunFunc = func(_ context.Context, options runner.RunCmdOpts) (runner.Result, error) {
		joined := strings.Join(options.Args, " ")
		switch {
		case strings.Contains(joined, "container ls"):
			return runner.Result{Stdout: []byte("0123456789abcdef\n")}, nil
		case strings.Contains(joined, ".Config.Labels"):
			return runner.Result{Stdout: append(labels, '\n')}, nil
		case strings.Contains(joined, ".Config.Image"):
			return runner.Result{
				Stdout: []byte(componentImageReference + "\nsha256:" + strings.Repeat("b", 64) + "\nnull\n"),
			}, nil
		case strings.Contains(joined, ".Mounts"):
			return runner.Result{Stdout: append(mounts, '\n')}, nil
		case strings.Contains(joined, " sha256sum "):
			digest := sha256.Sum256([]byte("http:// {\n\trespond 404\n}\n"))
			return runner.Result{
				Stdout: []byte(hex.EncodeToString(digest[:]) + "  " + componentConfigContainerPath + "\n"),
			}, nil
		case strings.Contains(joined, " caddy validate ") && validationExit != 0:
			return runner.Result{ExitCode: validationExit}, errors.New("validation failed")
		default:
			return runner.Result{}, nil
		}
	}
	return fake
}

func validRequest(t *testing.T) *agentpb.ComposeHelperRequest {
	t.Helper()
	yaml := []byte("services:\n  api:\n    image: registry.example/api@sha256:" +
		"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef\n")
	yamlHash := sha256.Sum256(yaml)
	plan, err := executionplan.Seal(&agentpb.ExecutionPlan{
		Schema: executionplan.SchemaVersion, PlanId: helperPlanID, RenderGeneration: 1,
		Operation: agentpb.PlanOperation_PLAN_OPERATION_DEPLOY, TargetId: helperServiceID,
		Artifacts: []*agentpb.ComposeArtifact{{
			ArtifactId:  helperArtifactID,
			OwnerKind:   agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_PLATFORM,
			ProjectName: "groundplane-infra", CanonicalYaml: yaml, YamlSha256: yamlHash[:],
			Services: []*agentpb.ComposeService{{
				ServiceId: helperServiceID, ComposeName: "api", ExpectedReplicas: 1, HasHealthcheck: true,
				ExpectedLabels: []*agentpb.LabelPair{
					{Key: "com.groundplane.kind", Value: "service"},
					{Key: "com.groundplane.managed", Value: "true"},
					{Key: "com.groundplane.plan-id", Value: helperPlanID},
					{Key: "com.groundplane.render-generation", Value: "1"},
					{Key: "com.groundplane.service-id", Value: helperServiceID},
				},
			}},
		}},
		Steps: []*agentpb.ExecutionStep{{
			StepId: helperStepID, TimeoutSeconds: 30,
			Payload: &agentpb.ExecutionStep_ComposeApply{ComposeApply: &agentpb.ComposeApply{
				ArtifactId: helperArtifactID, ServiceIds: []string{helperServiceID},
			}},
		}},
	})
	if err != nil {
		t.Fatalf("seal helper plan: %v", err)
	}
	return &agentpb.ComposeHelperRequest{
		Schema: SchemaVersion, AssignmentId: "asgn_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		TaskId: helperTaskID, OperationId: helperOperationID,
		Plan: plan, StepId: helperStepID, TimeoutSeconds: 30,
	}
}
