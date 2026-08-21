package composehelper

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"reflect"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

const (
	helperTaskID      = "task_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	helperOperationID = "op_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	helperPlanID      = "plan_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	helperArtifactID  = "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	helperServiceID   = "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	helperStepID      = "step_01ARZ3NDEKTSV4RRFFQ69G5FAV"
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
	if _, err := ReadRequest(context.Background(), bytes.NewReader(framed)); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
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
