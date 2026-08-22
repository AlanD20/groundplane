package composehelper

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

const (
	helperTaskID        = "task_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	helperOperationID   = "op_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	helperPlanID        = "plan_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	helperArtifactID    = "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	helperServiceID     = "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	helperStepID        = "step_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	helperEnvironmentID = "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	helperTenantID      = "tnt_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	helperProjectID     = "prj_01ARZ3NDEKTSV4RRFFQ69G5FAV"
)

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
	for index, call := range fake.Calls {
		if call.Name != DockerExecutable || call.Dir != WorkDirectory || !call.ReplaceEnv ||
			!reflect.DeepEqual(call.Env, fixedEnvironment) || !reflect.DeepEqual(call.Args, wantArgs[index]) ||
			!bytes.Equal(call.Stdin, request.Plan.Artifacts[0].CanonicalYaml) {
			t.Fatalf("Runner call %d = %#v", index, call)
		}
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

// Rationale: the helper must prove the exact generated Caddyfile and managed
// read-only mount before invoking fixed validation and reload commands in the
// uniquely running generated Caddy container.
func TestExecuteCaddyConfigValidatesBeforeReload(t *testing.T) {
	request, configPath := validCaddyRequest(t)
	fake := caddyRunner(t, configPath, 0)
	response, err := Execute(context.Background(), fake, request)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if response.Outcome != agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_COMPLETED ||
		response.Diagnostic != agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_NONE {
		t.Fatalf("response = %#v", response)
	}
	if len(fake.Calls) != 5 {
		t.Fatalf("Runner calls = %d, want lookup, two inspections, validate, reload", len(fake.Calls))
	}
	if !reflect.DeepEqual(fake.Calls[3].Args, []string{
		"container", "exec", "0123456789abcdef", "caddy", "validate",
		"--config", caddyfileContainer, "--adapter", "caddyfile",
	}) || !reflect.DeepEqual(fake.Calls[4].Args, []string{
		"container", "exec", "0123456789abcdef", "caddy", "reload",
		"--config", caddyfileContainer, "--adapter", "caddyfile",
	}) {
		t.Fatalf("Caddy calls = %#v", fake.Calls[3:])
	}
}

// Rationale: a rejected Caddyfile must be reported as a bounded diagnostic
// and must never reach the live reload command.
func TestExecuteCaddyConfigRejectionStopsBeforeReload(t *testing.T) {
	request, configPath := validCaddyRequest(t)
	fake := caddyRunner(t, configPath, 15)
	response, err := Execute(context.Background(), fake, request)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if len(fake.Calls) != 4 || response.ExitCode != 15 ||
		response.Diagnostic != agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_CADDY_CONFIG_REJECTED {
		t.Fatalf("calls=%d response=%#v", len(fake.Calls), response)
	}
}

func validCaddyRequest(t *testing.T) (*agentpb.ComposeHelperRequest, string) {
	t.Helper()
	root := filepath.Join(t.TempDir(), helperTenantID, helperProjectID, helperEnvironmentID)
	configPath := filepath.Join(root, filepath.FromSlash(caddyfileRelative))
	if err := os.MkdirAll(filepath.Dir(configPath), 0o700); err != nil {
		t.Fatalf("MkdirAll() error = %v", err)
	}
	config := []byte("http:// {\n\trespond 404\n}\n")
	if err := os.WriteFile(configPath, config, 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	digest := sha256.Sum256(config)
	request := validRequest(t)
	request.Plan.PlanHash = nil
	request.Plan.Operation = agentpb.PlanOperation_PLAN_OPERATION_COMPONENT_APPLY
	request.Plan.TargetId = helperEnvironmentID
	request.Plan.Artifacts[0].OwnerKind = agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT
	request.Plan.Artifacts[0].OwnerId = helperEnvironmentID
	request.Plan.Artifacts[0].ProjectName = "gp-" + strings.ToLower(helperEnvironmentID)
	request.Plan.Artifacts[0].AuthorizedVolumeDir = root
	request.Plan.Artifacts[0].Services[0].ComposeName = "caddy"
	request.Plan.Artifacts[0].Services[0].ExpectedLabels = []*agentpb.LabelPair{
		{Key: "com.groundplane.environment-id", Value: helperEnvironmentID},
		{Key: "com.groundplane.kind", Value: "service"},
		{Key: "com.groundplane.managed", Value: "true"},
		{Key: "com.groundplane.plan-id", Value: helperPlanID},
		{Key: "com.groundplane.render-generation", Value: "1"},
		{Key: "com.groundplane.service-id", Value: helperServiceID},
	}
	request.Plan.Steps[0].Payload = &agentpb.ExecutionStep_CaddyConfigApply{
		CaddyConfigApply: &agentpb.CaddyConfigApply{
			ArtifactId: helperArtifactID, ServiceId: helperServiceID, CaddyfileSha256: digest[:],
		},
	}
	sealed, err := executionplan.Seal(request.Plan)
	if err != nil {
		t.Fatalf("Seal() error = %v", err)
	}
	request.Plan = sealed
	return request, configPath
}

func caddyRunner(t *testing.T, configPath string, validationExit int) *runner.FakeRunner {
	t.Helper()
	labels, err := json.Marshal(map[string]string{
		"com.groundplane.managed": "true", "com.groundplane.kind": "service",
		"com.groundplane.environment-id": helperEnvironmentID,
		"com.groundplane.service-id":     helperServiceID,
	})
	if err != nil {
		t.Fatalf("Marshal(labels) error = %v", err)
	}
	mounts, err := json.Marshal([]caddyMount{{
		Type: "bind", Source: configPath, Destination: caddyfileContainer, RW: false,
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
		case strings.Contains(joined, ".Mounts"):
			return runner.Result{Stdout: append(mounts, '\n')}, nil
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
		Schema: SchemaVersion, TaskId: helperTaskID, OperationId: helperOperationID,
		Plan: plan, StepId: helperStepID, TimeoutSeconds: 30,
	}
}
