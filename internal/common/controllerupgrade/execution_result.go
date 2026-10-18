package controllerupgrade

import (
	"github.com/AlanD20/groundplane/internal/common/taskprogress"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ExecutionResult is operation evidence, not a generic Task status. An absent
// journal proves no activation was published only when the owner sets Settled.
// An unresolved result keeps the release claim while freeing worker capacity.
type ExecutionResult struct {
	Outcome workexecution.Outcome `json:"outcome"`
	Settled bool                  `json:"settled"`
	Journal *Journal              `json:"journal,omitempty"`
	Failure *taskprogress.Failure `json:"failure,omitempty"`
}

func (result ExecutionResult) Validate(expected Journal) error {
	if err := expected.Validate(); err != nil {
		return err
	}
	if result.Failure == nil && result.Outcome != workexecution.Succeeded || result.Failure != nil && result.Outcome == workexecution.Succeeded {
		return errs.New(errs.KindValidationFailed, "controller activation failure evidence is missing or contradictory")
	}
	if err := result.Failure.Validate(); err != nil {
		return err
	}
	if result.Outcome != workexecution.Succeeded && result.Outcome != workexecution.Failed && result.Outcome != workexecution.Cancelled {
		return errs.New(errs.KindValidationFailed, "controller activation result outcome is invalid")
	}
	if result.Journal == nil {
		if result.Outcome == workexecution.Succeeded {
			return errs.New(errs.KindValidationFailed, "controller activation success requires its healthy journal")
		}
		return nil
	}
	journal := result.Journal
	if err := journal.Validate(); err != nil {
		return err
	}
	if !journal.SameOperation(expected) || result.Settled && !journal.Phase.Settled() || !result.Settled && result.Outcome == workexecution.Succeeded {
		return errs.New(errs.KindValidationFailed, "controller activation result differs from its captured handoff")
	}
	if result.Outcome == workexecution.Succeeded && journal.Phase != PhaseHealthy {
		return errs.New(errs.KindValidationFailed, "controller activation success has no qualification proof")
	}
	return nil
}
