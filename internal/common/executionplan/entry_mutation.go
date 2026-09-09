package executionplan

import (
	"slices"
	"sort"
	"strconv"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func validateRuntimeMutationProcedures(plan *agentpb.ExecutionPlan) error {
	if err := validateServiceLifecyclePlan(plan); err != nil {
		return err
	}
	return validateEntryMutationPlan(plan)
}

func validateEntryMutationPlan(plan *agentpb.ExecutionPlan) error {
	procedure := plan.GetEntryMutationProcedure()
	if procedure == nil {
		return nil
	}
	invalid := func() error {
		return errs.New(errs.KindValidationFailed, "Entry mutation procedure exceeds retained runtime authority")
	}
	if plan.Operation != agentpb.PlanOperation_PLAN_OPERATION_RECONCILE ||
		ids.Validate(ids.KindEnvironment, plan.TargetId) != nil ||
		plan.GetCandidateReleaseProcedure() != nil || plan.GetServiceLifecycleProcedure() != nil ||
		plan.GetManagedComponentProcedure() != nil || plan.GetComponentRollbackObservation() != nil ||
		len(plan.Artifacts) != 2 || len(plan.Steps) == 0 ||
		procedure.BaselineArtifactId == procedure.CandidateArtifactId {
		return invalid()
	}
	baseline, candidate := entryMutationArtifacts(plan)
	if baseline == nil || candidate == nil || baseline.OwnerId != plan.TargetId || candidate.OwnerId != plan.TargetId ||
		baseline.OwnerKind != agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT ||
		!entryArtifactRuntimeEqual(baseline, candidate) {
		return invalid()
	}
	for index, step := range plan.Steps {
		if materialization := step.GetMaterializeFile(); materialization != nil {
			if materialization.ArtifactId != candidate.ArtifactId || materialization.EnvironmentId != plan.TargetId {
				return invalid()
			}
			continue
		}
		apply := step.GetComposeApply()
		if index == 0 || index != len(plan.Steps)-1 || apply == nil || apply.ArtifactId != candidate.ArtifactId ||
			apply.FullReconcile || apply.ForceRecreate || !apply.NoDependencies || len(apply.ServiceIds) == 0 {
			return invalid()
		}
		for _, id := range apply.ServiceIds {
			found := false
			for _, service := range candidate.Services {
				if service.GetServiceId() == id &&
					service.GetRole() != agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY {
					found = true
				}
			}
			if !found {
				return invalid()
			}
		}
	}
	return nil
}

func entryArtifactRuntimeEqual(baseline, candidate *agentpb.ComposeArtifact) bool {
	compared := proto.CloneOf(baseline)
	compared.ArtifactId, compared.CanonicalYaml, compared.YamlSha256 =
		candidate.ArtifactId, candidate.CanonicalYaml, candidate.YamlSha256
	return proto.Equal(compared, candidate)
}

func entryMutationArtifacts(plan *agentpb.ExecutionPlan) (*agentpb.ComposeArtifact, *agentpb.ComposeArtifact) {
	var baseline, candidate *agentpb.ComposeArtifact
	for _, artifact := range plan.GetArtifacts() {
		if artifact.GetArtifactId() == plan.GetEntryMutationProcedure().GetBaselineArtifactId() {
			baseline = artifact
		}
		if artifact.GetArtifactId() == plan.GetEntryMutationProcedure().GetCandidateArtifactId() {
			candidate = artifact
		}
	}
	return baseline, candidate
}

func validEntryMutationOwnership(plan *agentpb.ExecutionPlan, labels map[string]string) bool {
	if plan.GetEntryMutationProcedure() == nil || validateEntryMutationPlan(plan) != nil ||
		ids.Validate(ids.KindPlan, labels[labelPlanID]) != nil {
		return false
	}
	generation, err := strconv.ParseUint(labels[labelRenderGen], 10, 64)
	return err == nil && generation > 0 && generation < plan.RenderGeneration &&
		strconv.FormatUint(generation, 10) == labels[labelRenderGen]
}

// EntryMutationServices selects only workload instances of the exposed logical
// Services. Stable proxies are not Entry consumers and cannot be restarted by
// this procedure. The caller must validate the complete sealed plan first.
func EntryMutationServices(plan *agentpb.ExecutionPlan, stepID string) ([]string, error) {
	if plan.GetEntryMutationProcedure() == nil || validateEntryMutationPlan(plan) != nil {
		return nil, errs.New(errs.KindValidationFailed, "Entry mutation procedure is invalid")
	}
	step := plan.Steps[len(plan.Steps)-1]
	apply := step.GetComposeApply()
	if step.StepId != stepID || apply == nil {
		return nil, errs.New(errs.KindValidationFailed, "Entry mutation apply step is absent")
	}
	_, artifact := entryMutationArtifacts(plan)
	var names []string
	for _, service := range artifact.Services {
		if slices.Contains(apply.ServiceIds, service.ServiceId) &&
			service.Role != agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY {
			names = append(names, service.ComposeName)
		}
	}
	sort.Strings(names)
	return names, nil
}
