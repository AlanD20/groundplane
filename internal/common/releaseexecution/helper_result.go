package releaseexecution

import (
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/rollingrelease"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// ValidateHelperResult is shared by transport consumers and durable evidence
// publication. Completion must describe this exact action, never an unrelated
// successful helper command or an earlier proxy configuration.
func ValidateHelperResult(input Input, progress Progress, response *agentpb.ReleaseHelperResponse) error {
	if progress.Validate(input) != nil || !progress.InFlight || progress.Phase != Rollout || response == nil ||
		executionplan.RejectUnknown(
			response,
		) != nil || !response.Succeeded || response.ExitCode != 0 || response.Inspection && response.MutationAttempted {
		return errs.New(errs.KindStateConflict, "Release helper result is not a successful active action")
	}
	startup := progress.Action == StartCandidate ||
		progress.Rolling != nil && progress.Rolling.Phase == rollingrelease.StartCandidate
	removal := progress.Action == RemovePrevious || progress.Action == RetireSwitchedPrevious ||
		progress.Rolling != nil &&
			(progress.Rolling.Phase == rollingrelease.RemovePredecessor || progress.Rolling.Phase == rollingrelease.RemoveRetained)
	switching := progress.Action == SwitchTraffic ||
		progress.Rolling != nil && progress.Rolling.Phase == rollingrelease.SwitchTraffic
	verifying := progress.Action == VerifyServing
	if !startup && !removal && !switching && !verifying ||
		startup && (!response.MutationAttempted && !response.Inspection || response.AbsenceObserved) ||
		removal && !response.AbsenceObserved ||
		verifying && (response.MutationAttempted || response.AbsenceObserved) ||
		switching && (response.AbsenceObserved || input.Artifacts.Proxy == nil && response.MutationAttempted) {
		return errs.New(errs.KindStateConflict, "Release helper result does not prove the selected action")
	}
	if (switching || verifying) && input.Artifacts.Proxy != nil {
		return progress.ValidateTrafficEvidence(input, response.ProxyEvidence)
	}
	if response.ProxyEvidence != nil {
		return errs.New(errs.KindStateConflict, "Release helper returned unrelated proxy evidence")
	}
	return nil
}
