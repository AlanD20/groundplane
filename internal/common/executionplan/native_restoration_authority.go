package executionplan

import (
	"bytes"
	"crypto/sha256"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// ValidateNativeRestorationAuthority checks received recovery authority against an
// already validated plan. The nullable applied artifact is checked independently;
// only the exact per-Service native artifacts select and authorize restoration.
func ValidateNativeRestorationAuthority(
	plan *agentpb.ExecutionPlan,
	authority *agentpb.ReleaseRestorationAuthority,
) error {
	members := plan.GetCandidateReleaseProcedure().GetMembers()
	if authority == nil || len(members) == 0 || len(members) != len(authority.GetCandidates()) ||
		len(authority.GetPlanHash()) != sha256.Size || !bytes.Equal(authority.GetPlanHash(), plan.GetPlanHash()) ||
		len(authority.GetAuthoritySha256()) != sha256.Size ||
		ids.Validate(
			ids.KindTask,
			authority.GetTaskId(),
		) != nil || ids.Validate(ids.KindOperation, authority.GetOperationId()) != nil {
		return invalidNativeRestorationAuthority()
	}
	if err := validateRestorationMemberSelection(authority); err != nil {
		return err
	}
	for index, member := range members {
		candidate := authority.GetCandidates()[index]
		if candidate.GetServiceId() != member.GetServiceId() ||
			candidate.GetReleaseId() != member.GetCandidateReleaseId() ||
			authority.GetCandidateArtifactId() != member.GetCandidateArtifactId() {
			return invalidNativeRestorationAuthority()
		}
	}
	return validateNativePlanReferences(plan, authority)
}

// validateRestorationMemberSelection verifies the received selection against
// its immutable witness. It neither chooses a target nor queries mutable state.
func validateRestorationMemberSelection(authority *agentpb.ReleaseRestorationAuthority) error {
	if RejectUnknown(authority) != nil ||
		ids.Validate(ids.KindEnvironment, authority.GetEnvironmentId()) != nil {
		return invalidNativeRestorationAuthority()
	}
	if witness := authority.GetAppliedPredecessor(); witness != nil {
		encoded := witness.GetComposeArtifact()
		if witness.GetKeyRevision() <= 0 || ids.Validate(ids.KindTask, witness.GetRevisionId()) != nil ||
			witness.GetRenderGeneration() == 0 || len(encoded) == 0 || len(encoded) > MaximumPlanBytes {
			return invalidNativeRestorationAuthority()
		}
		digest := sha256.Sum256(encoded)
		if !bytes.Equal(digest[:], witness.GetComposeArtifactSha256()) {
			return invalidNativeRestorationAuthority()
		}
		artifact := &agentpb.ComposeArtifact{}
		if proto.Unmarshal(encoded, artifact) != nil || RejectUnknown(artifact) != nil {
			return invalidNativeRestorationAuthority()
		}
		canonical, err := (proto.MarshalOptions{Deterministic: true}).Marshal(artifact)
		if err != nil || !bytes.Equal(canonical, encoded) ||
			artifact.GetOwnerKind() != agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT ||
			artifact.GetOwnerId() != authority.GetEnvironmentId() {
			return invalidNativeRestorationAuthority()
		}
	}
	identities := make([]CandidateServiceIdentity, len(authority.GetCandidates()))
	for index, member := range authority.GetCandidates() {
		identities[index] = CandidateServiceIdentity{
			ServiceID: member.GetServiceId(), ReleaseID: member.GetReleaseId(),
		}
	}
	if len(authority.GetNativePredecessors()) != len(authority.GetCandidates()) {
		return invalidNativeRestorationAuthority()
	}
	witnesses := make([]NativePredecessorWitness, len(authority.GetNativePredecessors()))
	for index, witness := range authority.GetNativePredecessors() {
		if witness == nil {
			return invalidNativeRestorationAuthority()
		}
		witnesses[index] = NativePredecessorWitness{
			ServiceID: witness.GetServiceId(), CurrentArtifact: witness.GetCurrentArtifact(),
			RetainedPriorArtifact: witness.GetRetainedPriorArtifact(),
		}
	}
	selected, err := SelectNativeRestorationMembers(authority.GetEnvironmentId(), identities, witnesses)
	if err != nil || len(selected) != len(authority.GetCandidates()) {
		return invalidNativeRestorationAuthority()
	}
	for index, selection := range selected {
		member := authority.GetCandidates()[index]
		if member.GetServiceId() != selection.ServiceID || member.GetReleaseId() != selection.ReleaseID ||
			member.GetTarget() != selection.Target {
			return invalidNativeRestorationAuthority()
		}
	}
	return nil
}

func invalidNativeRestorationAuthority() error {
	return errs.New(
		errs.KindValidationFailed,
		"native restoration authority is invalid or diverges from selected members",
	)
}

// validateNativePlanReferences closes the Blueprint plan's explicit native
// artifact references against the separately carried immutable witness. The
// applied predecessor is intentionally not consulted here.
func validateNativePlanReferences(plan *agentpb.ExecutionPlan, authority *agentpb.ReleaseRestorationAuthority) error {
	if plan == nil ||
		len(authority.GetNativePredecessors()) != len(plan.GetCandidateReleaseProcedure().GetMembers()) {
		return invalidNativeRestorationAuthority()
	}
	byService := make(map[string]*agentpb.ReleaseNativePredecessorAuthority, len(authority.GetNativePredecessors()))
	for _, witness := range authority.GetNativePredecessors() {
		if witness == nil || witness.GetServiceId() == "" || byService[witness.GetServiceId()] != nil {
			return invalidNativeRestorationAuthority()
		}
		byService[witness.GetServiceId()] = witness
	}
	for _, member := range plan.GetCandidateReleaseProcedure().GetMembers() {
		witness := byService[member.GetServiceId()]
		if witness == nil {
			return invalidNativeRestorationAuthority()
		}
		prior := member.GetServingPredecessor()
		if len(witness.GetCurrentArtifact()) == 0 {
			if prior.GetPriorArtifactId() != "" || prior.GetPriorReleaseId() != "" || prior.GetPriorTarget() != "" ||
				prior.GetRetainedPriorArtifactId() != "" {
				return invalidNativeRestorationAuthority()
			}
			continue
		}
		if prior.GetPriorArtifactId() == "" || prior.GetPriorReleaseId() == "" || prior.GetPriorTarget() == "" ||
			prior.GetPriorArtifactId() == member.GetCandidateArtifactId() {
			return invalidNativeRestorationAuthority()
		}
		if ValidateNativePredecessorWitness(
			authority.GetEnvironmentId(), member.GetServiceId(),
			witness.GetCurrentArtifact(), witness.GetRetainedPriorArtifact(),
		) != nil {
			return invalidNativeRestorationAuthority()
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
				return invalidNativeRestorationAuthority()
			}
		}
		artifact := &agentpb.ComposeArtifact{}
		if (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(witness.GetCurrentArtifact(), artifact) != nil {
			return invalidNativeRestorationAuthority()
		}
		if artifact.GetArtifactId() != prior.GetPriorArtifactId() {
			return invalidNativeRestorationAuthority()
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
			if found || nativePredecessorReleaseID(service) != prior.GetPriorReleaseId() ||
				target != prior.GetPriorTarget() {
				return invalidNativeRestorationAuthority()
			}
			found = true
		}
		if !found {
			return invalidNativeRestorationAuthority()
		}
		if len(witness.GetRetainedPriorArtifact()) == 0 {
			if prior.GetRetainedPriorArtifactId() != "" {
				return invalidNativeRestorationAuthority()
			}
		} else {
			retained := &agentpb.ComposeArtifact{}
			if (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(witness.GetRetainedPriorArtifact(), retained) != nil ||
				retained.GetArtifactId() != prior.GetRetainedPriorArtifactId() {
				return invalidNativeRestorationAuthority()
			}
		}
	}
	return nil
}
