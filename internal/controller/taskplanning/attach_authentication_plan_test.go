package taskplanning

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/adapters/valkey9"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	testattachments "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testservices "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

var registerAttachAuthenticationValkey sync.Once

func registerAttachAuthenticationAdapter() {
	registerAttachAuthenticationValkey.Do(valkey9.Register)
}

// Rationale: password-only Valkey Attach and detach both need the explicit
// wire mode and default-user secret preserved in the immutable plan.
func TestAttachProcedureTransportsPasswordAuthenticationAndDetachSecret(t *testing.T) {
	registerAttachAuthenticationAdapter()
	record := testattachments.Record{
		ID: ids.New(ids.KindAttach), BackingServiceID: ids.New(ids.KindService),
	}
	identity := AttachPlanIdentity{
		Authentication: core.BackingAuthenticationPassword,
		Role:           "default",
		Password:       []byte("independent-password"),
	}
	for _, taskType := range []testtaskjournal.TaskType{testtaskjournal.TaskAttach, testtaskjournal.TaskDetach} {
		task := etcd.TaskRecord{
			Type: taskType, TimeoutSeconds: 30,
			Steps: []testtaskjournal.TaskStepRecord{{ID: ids.New(ids.KindStep)}},
		}
		var steps []*agentpb.ExecutionStep
		var err error
		if taskType == testtaskjournal.TaskAttach {
			steps, err = BuildAttachProvisionSteps(task, record, "valkey:9", identity)
		} else {
			steps, err = attachProcedureSteps(task, record, "valkey:9", identity)
		}
		if err != nil {
			t.Fatalf("BuildAttachProvisionSteps(%s) error = %v", taskType, err)
		}
		procedure := steps[0].GetAdapterProcedure()
		if procedure.GetAuthentication() != agentpb.BackingAuthentication_BACKING_AUTHENTICATION_PASSWORD ||
			procedure.GetRole() != "default" || string(procedure.GetPassword()) != "independent-password" {
			t.Fatalf("BuildAttachProvisionSteps(%s) procedure = %#v", taskType, procedure)
		}
	}
}

// Rationale: a no-auth backing creates facts and network membership but must
// never manufacture an empty adapter procedure.
func TestAttachProcedureEmitsNoEffectForNoAuthentication(t *testing.T) {
	registerAttachAuthenticationAdapter()
	steps, err := BuildAttachProvisionSteps(
		etcd.TaskRecord{
			Type:           testtaskjournal.TaskAttach,
			TimeoutSeconds: 30,
		},
		testattachments.Record{ID: ids.New(ids.KindAttach), BackingServiceID: ids.New(ids.KindService)},
		"valkey:9",
		AttachPlanIdentity{Authentication: core.BackingAuthenticationNone},
	)
	if err != nil || len(steps) != 0 {
		t.Fatalf("BuildAttachProvisionSteps(none) = %#v, %v", steps, err)
	}
}

// Rationale: replay of a published no-auth Attach must still open and validate
// its encrypted identity while producing only the durable Compose step.
func TestTaskPlanResolverReplaysNoAuthenticationWithoutProcedure(t *testing.T) {
	registerAttachAuthenticationAdapter()
	fixture := newAttachPlanFixture(t, "valkey:9", false)
	setAttachPlanAuthentication(&fixture, core.BackingAuthenticationNone, "", nil)
	fixture.task.Steps = fixture.task.Steps[:1]

	plan, err := fixture.resolver.ResolveExecutionPlan(context.Background(), fixture.task)
	if err != nil {
		t.Fatalf("ResolveExecutionPlan(none) error = %v", err)
	}
	if len(plan.Steps) != 1 || plan.Steps[0].GetComposeApply() == nil ||
		plan.Steps[0].GetAdapterProcedure() != nil || fixture.state.identityCalls != 1 {
		t.Fatalf("ResolveExecutionPlan(none) = %#v, identity calls = %d", plan, fixture.state.identityCalls)
	}
	if _, err := executionplan.Validate(plan); err != nil {
		t.Fatalf("ResolveExecutionPlan(none) returned invalid plan: %v", err)
	}

	fixture.state.identity.Role = "default"
	if _, err := fixture.resolver.ResolveExecutionPlan(context.Background(), fixture.task); err == nil {
		t.Fatal("ResolveExecutionPlan(none with credential material) succeeded")
	}
}

// Rationale: detach replay for password-only authentication needs the secret
// itself because deleting the named ACL user is not available in this mode.
func TestTaskPlanResolverReplaysPasswordDetachWithSecret(t *testing.T) {
	registerAttachAuthenticationAdapter()
	fixture := newAttachPlanFixture(t, "valkey:9", false)
	setAttachPlanAuthentication(
		&fixture, core.BackingAuthenticationPassword, "default", []byte("independent-password"),
	)
	fixture.record.Status = core.AttachDetaching
	fixture.record.Operation = testattachments.AttachOperationDetach
	fixture.state.attaches[fixture.record.ID] = testkeyvalue.Versioned[testattachments.Record]{Record: fixture.record}
	fixture.state.renderInput.NetworkJoins = nil
	fixture.task.Type = testtaskjournal.TaskDetach

	plan, err := fixture.resolver.ResolveExecutionPlan(context.Background(), fixture.task)
	if err != nil {
		t.Fatalf("ResolveExecutionPlan(password detach) error = %v", err)
	}
	if len(plan.Steps) != 2 || plan.Steps[0].GetComposeApply() == nil {
		t.Fatalf("ResolveExecutionPlan(password detach) = %#v", plan)
	}
	detach := plan.Steps[1].GetAdapterProcedure()
	if detach == nil || detach.GetPhase() != agentpb.AdapterProcedurePhase_ADAPTER_PROCEDURE_PHASE_DETACH ||
		detach.GetAuthentication() != agentpb.BackingAuthentication_BACKING_AUTHENTICATION_PASSWORD ||
		detach.GetRole() != "default" || string(detach.GetPassword()) != "independent-password" {
		t.Fatalf("ResolveExecutionPlan(password detach) procedure = %#v", detach)
	}
	if _, err := executionplan.Validate(plan); err != nil {
		t.Fatalf("ResolveExecutionPlan(password detach) returned invalid plan: %v", err)
	}
}

// Rationale: authentication is captured in the durable render input; changing
// the backing Service mode after publication must invalidate replay.
func TestTaskPlanResolverRejectsBackingAuthenticationChangeAfterPublication(t *testing.T) {
	registerAttachAuthenticationAdapter()
	fixture := newAttachPlanFixture(t, "valkey:9", false)
	setAttachPlanAuthentication(&fixture, core.BackingAuthenticationNone, "", nil)
	fixture.task.Steps = fixture.task.Steps[:1]
	fixture.state.service.Record.Desired.Authentication = core.BackingAuthenticationPassword

	_, err := fixture.resolver.ResolveExecutionPlan(context.Background(), fixture.task)
	kind, _ := errs.KindOf(err)
	if kind != errs.KindStateConflict || fixture.state.identityCalls != 0 {
		t.Fatalf(
			"ResolveExecutionPlan(changed authentication) error = %v, identity calls = %d",
			err,
			fixture.state.identityCalls,
		)
	}
}

// Rationale: Blueprint replay must preserve a published no-auth candidate as a
// zero-procedure addition while still validating its encrypted identity shape.
func TestBlueprintAttachPlanReplaysNoAuthenticationWithoutProcedure(t *testing.T) {
	registerAttachAuthenticationAdapter()
	now := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	reader, task := blueprintPlanTestState(t)
	attachID := ids.NewAt(ids.KindAttach, now, 1)
	backingServiceID := ids.NewAt(ids.KindService, now, 2)
	backingEnvironmentID := ids.NewAt(ids.KindEnvironment, now, 3)
	backingNetworkID := ids.NewAt(ids.KindNetwork, now, 4)
	record, err := testattachments.NewPendingAttachRecord(
		attachID, reader.environment.ID, "cache", ids.NewAt(ids.KindProject, now, 5),
		backingEnvironmentID, backingServiceID, backingNetworkID,
		reader.projection.DesiredServices[0].Desired.ID, attachID, nil,
		[]testattachments.FactSetMetadata{{Facts: []testattachments.FactDefinition{{Key: "valkey9_HOST"}}}},
		task.ID, now,
	)
	if err != nil {
		t.Fatalf("NewPendingAttachRecord() error = %v", err)
	}
	intent := testattachments.BlueprintAttachTaskIntent{
		TaskID: task.ID, EnvironmentID: reader.environment.ID, Status: testtaskjournal.TaskStatusPending,
		Candidates: []testattachments.Record{record}, CreatedAt: now,
	}
	state := &attachPlanTestState{
		attaches: map[string]testkeyvalue.Versioned[testattachments.Record]{attachID: {Record: record}},
		service: testkeyvalue.Versioned[testservices.ServiceRecord]{Record: testservices.ServiceRecord{
			EnvironmentID: backingEnvironmentID, BackingNetworkID: backingNetworkID,
			Desired: core.Service{
				ID: backingServiceID, Name: "valkey", Adapter: "valkey:9",
				Authentication: core.BackingAuthenticationNone,
			},
			Runtime: core.ServiceRuntime{
				ServiceID: backingServiceID, RuntimeIntent: core.ServiceRuntimeIntentRunning,
			},
		}},
		identity:        AttachPlanIdentity{Authentication: core.BackingAuthenticationNone},
		blueprintIntent: &intent,
	}
	resolver, err := NewTaskPlanResolverWithAttachments(
		"/var/lib/groundplane/vol", reader, state, state, state, nil,
	)
	if err != nil {
		t.Fatalf("NewTaskPlanResolverWithAttachments() error = %v", err)
	}
	baselineState := *state
	baselineState.blueprintIntent = nil
	baselineResolver, err := NewTaskPlanResolverWithAttachments(
		"/var/lib/groundplane/vol", reader, &baselineState, &baselineState, &baselineState, nil,
	)
	if err != nil {
		t.Fatalf("NewTaskPlanResolverWithAttachments(baseline) error = %v", err)
	}
	baseline, err := baselineResolver.ResolveExecutionPlan(context.Background(), task)
	if err != nil {
		t.Fatalf("ResolveExecutionPlan(baseline) error = %v", err)
	}
	replayed, err := resolver.ResolveExecutionPlan(context.Background(), task)
	if err != nil {
		t.Fatalf("ResolveExecutionPlan(Blueprint none) error = %v", err)
	}
	if string(replayed.PlanHash) != string(baseline.PlanHash) || len(replayed.Steps) != len(baseline.Steps) ||
		state.identityCalls != 1 {
		t.Fatalf("ResolveExecutionPlan(Blueprint none) = %#v, identity calls = %d", replayed, state.identityCalls)
	}
	for _, step := range replayed.Steps {
		if step.GetAdapterProcedure() != nil {
			t.Fatalf("ResolveExecutionPlan(Blueprint none) emitted procedure = %#v", step)
		}
	}
	if _, err := executionplan.Validate(replayed); err != nil {
		t.Fatalf("ResolveExecutionPlan(Blueprint none) returned invalid plan: %v", err)
	}

	state.identity.Password = []byte("must-not-exist")
	if _, err := resolver.ResolveExecutionPlan(context.Background(), task); err == nil {
		t.Fatal("ResolveExecutionPlan(Blueprint none with credential material) succeeded")
	}
}

func setAttachPlanAuthentication(
	fixture *attachPlanFixture,
	authentication core.BackingAuthentication,
	role string,
	password []byte,
) {
	fixture.state.service.Record.Desired.Authentication = authentication
	fixture.state.renderInput.Authentication = authentication
	fixture.state.identity.Authentication = authentication
	fixture.state.identity.Role = role
	fixture.state.identity.Password = append([]byte(nil), password...)
}
