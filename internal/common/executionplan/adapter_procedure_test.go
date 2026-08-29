package executionplan

import (
	"errors"
	"testing"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

const (
	testAttachID         = "att_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	testBackingServiceID = "svc_01ARZ3NDEKTSV4RRFFQ69G5FAW"
)

// Rationale: Attach execution may select only a registered compiled adapter
// and typed identity values; executable SQL or command text has no wire field.
func TestSealAcceptsTypedAdapterProvisionProcedure(t *testing.T) {
	plan := validAdapterProcedurePlan()
	if _, err := Seal(plan); err != nil {
		t.Fatalf("Seal(adapter provision) error = %v", err)
	}
}

// Rationale: the locked adapter identity preserves the exact Compose Service name, whose valid
// ASCII grammar includes uppercase letters, digits, hyphens, dots, and underscores.
func TestSealAcceptsServiceDerivedAdapterIdentity(t *testing.T) {
	plan := validAdapterProcedurePlan()
	procedure := plan.Steps[0].GetAdapterProcedure()
	procedure.Role = "App-Web.v2_5d3f9a"
	procedure.Database = procedure.Role
	if _, err := Seal(plan); err != nil {
		t.Fatalf("Seal(service-derived adapter identity) error = %v", err)
	}
}

// Rationale: a sealed Attach plan must not smuggle Detach behavior or apply
// an adapter procedure to a different durable Attach target.
func TestSealRejectsAdapterProcedureOutsideOperationAndTarget(t *testing.T) {
	tests := map[string]func(*agentpb.ExecutionPlan){
		"phase": func(plan *agentpb.ExecutionPlan) {
			plan.Steps[0].GetAdapterProcedure().Phase =
				agentpb.AdapterProcedurePhase_ADAPTER_PROCEDURE_PHASE_DETACH
			plan.Steps[0].GetAdapterProcedure().Password = nil
			plan.Steps[0].GetAdapterProcedure().Database = ""
		},
		"target": func(plan *agentpb.ExecutionPlan) {
			plan.Steps[0].GetAdapterProcedure().AttachId = "att_01ARZ3NDEKTSV4RRFFQ69G5FAW"
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			plan := validAdapterProcedurePlan()
			mutate(plan)
			if _, err := Seal(plan); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
				t.Fatalf("Seal(invalid adapter procedure) error = %v, want validation.failed", err)
			}
		})
	}
}

func validAdapterProcedurePlan() *agentpb.ExecutionPlan {
	plan := validPlan()
	plan.Operation = agentpb.PlanOperation_PLAN_OPERATION_ATTACH
	plan.TargetId = testAttachID
	plan.Steps = []*agentpb.ExecutionStep{{
		StepId: testStepID, TimeoutSeconds: 30,
		Payload: &agentpb.ExecutionStep_AdapterProcedure{AdapterProcedure: &agentpb.AdapterProcedure{
			AdapterKey: "postgres:16", Phase: agentpb.AdapterProcedurePhase_ADAPTER_PROCEDURE_PHASE_PROVISION,
			AttachId: testAttachID, BackingServiceId: testBackingServiceID,
			Role: "api_5d3f9a", Password: []byte("URL_safe-1"), Database: "api_5d3f9a",
		}},
	}}
	return plan
}
