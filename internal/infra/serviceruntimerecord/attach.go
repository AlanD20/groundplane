package serviceruntimerecord

import (
	"bytes"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// AttachPreparation binds selected workload updates to one sealed network
// mutation. Source revisions are compared at publication and acknowledgement.
// An empty Updates list explicitly means validation-only execution.
type AttachPreparation struct {
	EnvironmentID    string         `json:"environment_id"`
	PlanID           string         `json:"plan_id"`
	PlanHash         string         `json:"plan_hash"`
	StepID           string         `json:"step_id"`
	RenderGeneration uint64         `json:"render_generation"`
	Updates          []AttachUpdate `json:"updates"`
}

type AttachUpdate struct {
	PreviousRevision int64                          `json:"previous_revision"`
	Runtime          executionplan.CandidateRuntime `json:"runtime"`
}

func ValidateAttachPreparation(prepared AttachPreparation) error {
	if ids.Validate(ids.KindEnvironment, prepared.EnvironmentID) != nil ||
		ids.Validate(ids.KindPlan, prepared.PlanID) != nil ||
		ids.Validate(ids.KindStep, prepared.StepID) != nil ||
		!validDigest(prepared.PlanHash) || prepared.RenderGeneration == 0 || len(prepared.Updates) > 1 {
		return errs.New(errs.KindValidationFailed, "Attach runtime preparation identity is invalid")
	}
	for _, update := range prepared.Updates {
		if update.PreviousRevision <= 0 ||
			ids.Validate(ids.KindDeployment, update.Runtime.ReleaseID) != nil {
			return errs.New(errs.KindValidationFailed, "Attach runtime predecessor is invalid")
		}
		if err := executionplan.ValidateNativePredecessorWitness(prepared.EnvironmentID, update.Runtime.ServiceID,
			update.Runtime.CurrentArtifact, update.Runtime.RetainedPriorArtifact); err != nil {
			return err
		}
		if err := validateRuntimeIdentity(update.Runtime); err != nil {
			return err
		}
	}
	return nil
}

// AcknowledgeAttach advances an existing record, never creates one from
// historical Release input. The compiler preserves unselected proxy bytes;
// these checks additionally bind the prior identity and retained slot at commit.
func AcknowledgeAttach(previous Record, prepared AttachPreparation, source Acknowledgement) (Record, error) {
	if err := Validate(previous); err != nil {
		return Record{}, err
	}
	if err := ValidateAttachPreparation(prepared); err != nil {
		return Record{}, err
	}
	if len(prepared.Updates) != 1 || previous.EnvironmentID != prepared.EnvironmentID ||
		source.PlanID != prepared.PlanID || source.PlanHash != prepared.PlanHash ||
		source.StepID != prepared.StepID || source.RenderGeneration != prepared.RenderGeneration {
		return Record{}, errs.New(errs.KindStateConflict, "Attach runtime acknowledgement differs from preparation")
	}
	next := prepared.Updates[0].Runtime
	prior := previous.Runtime
	if next.ServiceID != prior.ServiceID || next.ReleaseID != prior.ReleaseID || next.Target != prior.Target ||
		next.ProxyGeneration != prior.ProxyGeneration || !bytes.Equal(next.ProxyConfigSHA256, prior.ProxyConfigSHA256) ||
		!bytes.Equal(next.RetainedPriorArtifact, prior.RetainedPriorArtifact) {
		return Record{}, errs.New(errs.KindStateConflict, "Attach runtime acknowledgement changed unselected authority")
	}
	record := Record{EnvironmentID: prepared.EnvironmentID, Runtime: next, Source: source}
	if previous.Configuration != nil {
		configuration := *previous.Configuration
		record.Configuration = &configuration
	}
	return record, Validate(record)
}
