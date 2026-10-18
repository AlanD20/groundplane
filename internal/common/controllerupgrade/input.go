package controllerupgrade

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/imageref"
	"github.com/AlanD20/groundplane/pkg/errs"
	"math"
)

// Input captures one verified native release and its exact predecessors. Paths,
// commands and a newer release selection cannot be supplied by the executor.
type Input struct {
	Release            Digest            `json:"release"`
	Manifest           Manifest          `json:"manifest"`
	PreviousController Digest            `json:"previous_controller"`
	Agent              *AgentPredecessor `json:"agent"`
}

func (input Input) ValidateRecord() error {
	if !input.Release.Valid() || !input.PreviousController.Valid() || input.Manifest.ControllerSHA256 == input.PreviousController {
		return errs.New(errs.KindValidationFailed, "controller update identities are invalid or unchanged")
	}
	if err := (Release{Release: input.Release, Manifest: input.Manifest}).ValidateRecord(); err != nil {
		return err
	}
	if input.Agent != nil && (ids.Validate(ids.KindAgent, input.Agent.ID) != nil || !imageref.IsDigestPinned(input.Agent.Image) ||
		len(input.Agent.Image) > 1024 || input.Agent.Generation == 0 || input.Agent.Generation > math.MaxUint64-4) {
		return errs.New(errs.KindValidationFailed, "controller activation Agent predecessor is invalid")
	}
	return nil
}

func (input Input) Validate() error {
	if err := input.ValidateRecord(); err != nil {
		return err
	}
	return input.Manifest.Validate()
}
