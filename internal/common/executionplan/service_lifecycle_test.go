package executionplan

import (
	"errors"
	"sort"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func TestSealAllowsOnlyEnumeratedHistoricalServiceLifecycleSource(t *testing.T) {
	plan := validPlan()
	environmentID := "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	lifecyclePlanID := "plan_01ARZ3NDEKTSV4RRFFQ69G5FAW"
	plan.PlanId = lifecyclePlanID
	plan.Operation = agentpb.PlanOperation_PLAN_OPERATION_START
	plan.Artifacts[0].OwnerKind = agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT
	plan.Artifacts[0].OwnerId = environmentID
	plan.Artifacts[0].ProjectName = "gp-" + strings.ToLower(environmentID)
	plan.Artifacts[0].AuthorizedVolumeDir = "/var/lib/groundplane/volumes/tnt_01ARZ3NDEKTSV4RRFFQ69G5FAV/prj_01ARZ3NDEKTSV4RRFFQ69G5FAV/" + environmentID
	labels := plan.Artifacts[0].Services[0].ExpectedLabels
	labels = append(labels, &agentpb.LabelPair{Key: labelEnvironmentID, Value: environmentID})
	sort.Slice(labels, func(i, j int) bool { return labels[i].Key < labels[j].Key })
	plan.Artifacts[0].Services[0].ExpectedLabels = labels
	plan.ServiceLifecycleProcedure = &agentpb.ServiceLifecycleProcedure{Sources: []*agentpb.ServiceLifecycleSource{{
		ArtifactId: testArtifact, SourcePlanId: testPlanID, SourceRenderGeneration: 7,
		ServiceId: testServiceID, ComposeNames: []string{"api"}, StepId: testStepID,
	}}}
	if _, err := Seal(plan); err != nil {
		t.Fatalf("Seal(valid historical lifecycle source) error = %v", err)
	}
	for name, mutate := range map[string]func(*agentpb.ExecutionPlan){
		"extra-name": func(value *agentpb.ExecutionPlan) {
			value.ServiceLifecycleProcedure.Sources[0].ComposeNames = append(value.ServiceLifecycleProcedure.Sources[0].ComposeNames, "other")
		},
		"source-plan": func(value *agentpb.ExecutionPlan) {
			value.ServiceLifecycleProcedure.Sources[0].SourcePlanId = lifecyclePlanID
		},
		"selection": func(value *agentpb.ExecutionPlan) {
			value.Steps[0].GetComposeApply().ServiceIds[0] = "svc_01ARZ3NDEKTSV4RRFFQ69G5FAW"
		},
		"artifact": func(value *agentpb.ExecutionPlan) {
			value.ServiceLifecycleProcedure.Sources[0].ArtifactId = "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAW"
		},
	} {
		t.Run(name, func(t *testing.T) {
			changed := proto.Clone(plan).(*agentpb.ExecutionPlan)
			changed.PlanHash = nil
			mutate(changed)
			if _, err := Seal(changed); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
				t.Fatalf("Seal(tampered lifecycle %s) error = %v", name, err)
			}
		})
	}
}
