package agent

import (
	"bytes"
	"crypto/sha256"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// validateAssignmentRestorationWitness verifies the received selection against
// its immutable witness. It neither chooses a target nor queries mutable state.
func validateAssignmentRestorationWitness(authority *agentpb.ReleaseRestorationAuthority) error {
	if executionplan.RejectUnknown(authority) != nil ||
		ids.Validate(ids.KindEnvironment, authority.GetEnvironmentId()) != nil {
		return invalidAssignmentRestorationWitness()
	}
	var artifact *agentpb.ComposeArtifact
	if witness := authority.GetAppliedPredecessor(); witness != nil {
		encoded := witness.GetComposeArtifact()
		if witness.GetKeyRevision() <= 0 || ids.Validate(ids.KindTask, witness.GetRevisionId()) != nil ||
			witness.GetRenderGeneration() == 0 || len(encoded) == 0 || len(encoded) > executionplan.MaximumPlanBytes {
			return invalidAssignmentRestorationWitness()
		}
		digest := sha256.Sum256(encoded)
		if !bytes.Equal(digest[:], witness.GetComposeArtifactSha256()) {
			return invalidAssignmentRestorationWitness()
		}
		artifact = &agentpb.ComposeArtifact{}
		if proto.Unmarshal(encoded, artifact) != nil || executionplan.RejectUnknown(artifact) != nil {
			return invalidAssignmentRestorationWitness()
		}
		canonical, err := (proto.MarshalOptions{Deterministic: true}).Marshal(artifact)
		if err != nil || !bytes.Equal(canonical, encoded) ||
			artifact.GetOwnerKind() != agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT ||
			artifact.GetOwnerId() != authority.GetEnvironmentId() {
			return invalidAssignmentRestorationWitness()
		}
	}
	identities := make([]executionplan.CandidateServiceIdentity, len(authority.GetCandidates()))
	for index, member := range authority.GetCandidates() {
		identities[index] = executionplan.CandidateServiceIdentity{
			ServiceID: member.GetServiceId(), ReleaseID: member.GetReleaseId(),
		}
	}
	var selected []executionplan.CandidateRestorationSelection
	var err error
	if len(authority.GetNativePredecessors()) != 0 {
		if len(authority.GetNativePredecessors()) != len(authority.GetCandidates()) {
			return invalidAssignmentRestorationWitness()
		}
		witnesses := make([]executionplan.NativePredecessorWitness, len(authority.GetNativePredecessors()))
		for index, witness := range authority.GetNativePredecessors() {
			if witness == nil {
				return invalidAssignmentRestorationWitness()
			}
			witnesses[index] = executionplan.NativePredecessorWitness{
				ServiceID: witness.GetServiceId(), CurrentArtifact: witness.GetCurrentArtifact(),
				RetainedPriorArtifact: witness.GetRetainedPriorArtifact(),
			}
		}
		selected, err = executionplan.SelectNativeRestorationMembers(
			authority.GetEnvironmentId(),
			identities,
			witnesses,
		)
	} else {
		selected, err = executionplan.SelectBlueprintRestorationMembers(identities, artifact)
	}
	if err != nil || len(selected) != len(authority.GetCandidates()) {
		return invalidAssignmentRestorationWitness()
	}
	for index, selection := range selected {
		member := authority.GetCandidates()[index]
		if member.GetServiceId() != selection.ServiceID || member.GetReleaseId() != selection.ReleaseID ||
			member.GetTarget() != selection.Target {
			return invalidAssignmentRestorationWitness()
		}
	}
	return nil
}

func invalidAssignmentRestorationWitness() error {
	return errs.New(
		errs.KindInternal,
		"agent: applied restoration witness is invalid or diverges from selected members",
	)
}

// validateNativePlanReferences closes the Blueprint plan's explicit native
// artifact references against the separately carried immutable witness. The
// applied predecessor is intentionally not consulted here.
func validateNativePlanReferences(plan *agentpb.ExecutionPlan, authority *agentpb.ReleaseRestorationAuthority) error {
	if plan == nil ||
		len(authority.GetNativePredecessors()) != len(plan.GetCandidateReleaseProcedure().GetMembers()) {
		return invalidAssignmentRestorationWitness()
	}
	byService := make(map[string]*agentpb.ReleaseNativePredecessorAuthority, len(authority.GetNativePredecessors()))
	for _, witness := range authority.GetNativePredecessors() {
		if witness == nil || witness.GetServiceId() == "" || byService[witness.GetServiceId()] != nil {
			return invalidAssignmentRestorationWitness()
		}
		byService[witness.GetServiceId()] = witness
	}
	for _, member := range plan.GetCandidateReleaseProcedure().GetMembers() {
		witness := byService[member.GetServiceId()]
		if witness == nil {
			return invalidAssignmentRestorationWitness()
		}
		prior := member.GetServingPredecessor()
		if len(witness.GetCurrentArtifact()) == 0 {
			if prior.GetPriorArtifactId() != "" || prior.GetPriorReleaseId() != "" || prior.GetPriorTarget() != "" ||
				prior.GetRetainedPriorArtifactId() != "" {
				return invalidAssignmentRestorationWitness()
			}
			continue
		}
		if prior.GetPriorArtifactId() == "" || prior.GetPriorReleaseId() == "" || prior.GetPriorTarget() == "" ||
			prior.GetPriorArtifactId() == member.GetCandidateArtifactId() {
			return invalidAssignmentRestorationWitness()
		}
		if executionplan.ValidateNativePredecessorWitness(
			authority.GetEnvironmentId(), member.GetServiceId(),
			witness.GetCurrentArtifact(), witness.GetRetainedPriorArtifact(),
		) != nil {
			return invalidAssignmentRestorationWitness()
		}
		for _, encoded := range [][]byte{witness.GetCurrentArtifact(), witness.GetRetainedPriorArtifact()} {
			if len(encoded) == 0 {
				continue
			}
			matched := false
			for _, artifact := range plan.GetArtifacts() {
				candidate, err := (proto.MarshalOptions{Deterministic: true}).Marshal(artifact)
				if err == nil && bytes.Equal(candidate, encoded) {
					matched = true
					break
				}
			}
			if !matched {
				return invalidAssignmentRestorationWitness()
			}
		}
		artifact := new(agentpb.ComposeArtifact)
		if (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(witness.GetCurrentArtifact(), artifact) != nil {
			return invalidAssignmentRestorationWitness()
		}
		if artifact.GetArtifactId() != prior.GetPriorArtifactId() {
			return invalidAssignmentRestorationWitness()
		}
		found := false
		for _, service := range artifact.GetServices() {
			if service.GetServiceId() != member.GetServiceId() ||
				service.GetRole() == agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY {
				continue
			}
			target := service.GetSlot()
			if target == "" {
				target = "singleton"
			}
			if found || restorationReleaseLabel(service) != prior.GetPriorReleaseId() ||
				target != prior.GetPriorTarget() {
				return invalidAssignmentRestorationWitness()
			}
			found = true
		}
		if !found {
			return invalidAssignmentRestorationWitness()
		}
		if len(witness.GetRetainedPriorArtifact()) == 0 {
			if prior.GetRetainedPriorArtifactId() != "" {
				return invalidAssignmentRestorationWitness()
			}
		} else {
			retained := new(agentpb.ComposeArtifact)
			if (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(witness.GetRetainedPriorArtifact(), retained) != nil ||
				retained.GetArtifactId() != prior.GetRetainedPriorArtifactId() {
				return invalidAssignmentRestorationWitness()
			}
		}
	}
	return nil
}

func hasServingPredecessorAuthority(authority *agentpb.ReleaseRestorationAuthority, serviceID string) bool {
	if authority == nil {
		return false
	}
	if len(authority.GetNativePredecessors()) != 0 {
		for _, witness := range authority.GetNativePredecessors() {
			if witness.GetServiceId() == serviceID {
				return len(witness.GetCurrentArtifact()) != 0
			}
		}
		return false
	}
	return authority.GetAppliedPredecessor() != nil
}

func validateCandidateReleaseAssignmentAuthority(assignment Assignment, plan *agentpb.ExecutionPlan) error {
	procedure := plan.GetCandidateReleaseProcedure()
	if procedure == nil {
		if assignment.RestorationAuthority != nil || assignment.ReleaseRecoveryDirective != nil ||
			len(assignment.ReleaseRecoveryRecordSHA256) != 0 {
			return errs.New(errs.KindInternal, "agent: non-release assignment carries restoration authority")
		}
		return nil
	}
	authority := assignment.RestorationAuthority
	if authority == nil || len(authority.GetPlanHash()) != 32 ||
		!bytes.Equal(authority.GetPlanHash(), plan.GetPlanHash()) ||
		len(authority.GetAuthoritySha256()) != 32 ||
		authority.GetTaskId() != assignment.TaskID ||
		authority.GetOperationId() != assignment.OperationID ||
		len(authority.GetCandidates()) != len(procedure.GetMembers()) {
		return errs.New(errs.KindInternal, "agent: candidate Release restoration authority is invalid")
	}
	if len(authority.GetNativePredecessors()) != len(procedure.GetMembers()) {
		return errs.New(errs.KindInternal, "agent: native predecessor authority is incomplete")
	}
	if err := validateAssignmentRestorationWitness(authority); err != nil {
		return err
	}
	if err := validateNativePlanReferences(plan, authority); err != nil {
		return err
	}
	selectedStepIDs := make([]string, 0, len(procedure.GetMembers())*2)
	compensateStepIDs := make([]string, 0, len(procedure.GetMembers()))
	for index, member := range procedure.GetMembers() {
		candidate := authority.GetCandidates()[index]
		if candidate.GetServiceId() != member.GetServiceId() ||
			candidate.GetReleaseId() != member.GetCandidateReleaseId() ||
			authority.GetCandidateArtifactId() != member.GetCandidateArtifactId() {
			return errs.New(errs.KindInternal, "agent: candidate Release restoration members diverge")
		}
		var probeID, compensateID string
		switch candidate.GetTarget() {
		case agentpb.ReleaseRestorationTarget_RELEASE_RESTORATION_TARGET_SERVING_PREDECESSOR:
			selected := member.GetServingPredecessor()
			if selected == nil || !hasServingPredecessorAuthority(authority, member.GetServiceId()) {
				return errs.New(errs.KindInternal, "agent: serving predecessor authority is incomplete")
			}
			probeID, compensateID = selected.GetProbeStepId(), selected.GetCompensateStepId()
		case agentpb.ReleaseRestorationTarget_RELEASE_RESTORATION_TARGET_CANDIDATE_ABSENCE:
			selected := member.GetCandidateAbsence()
			if selected == nil {
				return errs.New(errs.KindInternal, "agent: candidate absence authority is incomplete")
			}
			probeID, compensateID = selected.GetProbeStepId(), selected.GetCompensateStepId()
		default:
			return errs.New(errs.KindInternal, "agent: candidate Release restoration target is invalid")
		}
		selectedStepIDs = append(selectedStepIDs, probeID)
		compensateStepIDs = append(compensateStepIDs, compensateID)
	}
	for index := len(compensateStepIDs) - 1; index >= 0; index-- {
		selectedStepIDs = append(selectedStepIDs, compensateStepIDs[index])
	}
	if assignment.ExecutionMode != agentpb.TaskExecutionMode_TASK_EXECUTION_MODE_RECOVERY_ONLY {
		return nil
	}
	directive := assignment.ReleaseRecoveryDirective
	if directive.GetCursor() > uint32(len(directive.GetStepIds())) ||
		len(directive.GetStepIds()) != len(selectedStepIDs) {
		return errs.New(errs.KindInternal, "agent: candidate Release recovery cursor is invalid")
	}
	for index := range selectedStepIDs {
		if directive.GetStepIds()[index] != selectedStepIDs[index] {
			return errs.New(errs.KindInternal, "agent: candidate Release recovery procedure diverges")
		}
	}
	applicableIndex := 0
	for _, compensationStepID := range selectedStepIDs[len(procedure.GetMembers()):] {
		if applicableIndex < len(directive.GetApplicableCompensationStepIds()) &&
			directive.GetApplicableCompensationStepIds()[applicableIndex] == compensationStepID {
			applicableIndex++
		}
	}
	if applicableIndex != len(directive.GetApplicableCompensationStepIds()) {
		return errs.New(errs.KindInternal, "agent: recovery compensation obligation is not canonical")
	}
	switch directive.GetPhase() {
	case agentpb.ReleaseRecoveryPhase_RELEASE_RECOVERY_PHASE_PROBE:
		if int(directive.GetCursor()) >= len(procedure.GetMembers()) {
			return errs.New(errs.KindInternal, "agent: recovery probe phase cursor is invalid")
		}
	case agentpb.ReleaseRecoveryPhase_RELEASE_RECOVERY_PHASE_COMPENSATE:
		if int(directive.GetCursor()) < len(procedure.GetMembers()) ||
			int(directive.GetCursor()) >= len(selectedStepIDs) {
			return errs.New(errs.KindInternal, "agent: recovery compensation phase cursor is invalid")
		}
	case agentpb.ReleaseRecoveryPhase_RELEASE_RECOVERY_PHASE_PROVEN:
		if int(directive.GetCursor()) != len(selectedStepIDs) {
			return errs.New(errs.KindInternal, "agent: recovery proven phase cursor is invalid")
		}
	default:
		return errs.New(errs.KindInternal, "agent: recovery assignment phase is invalid")
	}
	return nil
}
