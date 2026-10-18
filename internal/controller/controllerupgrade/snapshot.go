package controllerupgrade

import (
	"context"

	upgrade "github.com/AlanD20/groundplane/internal/common/controllerupgrade"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ControllerUpdateSnapshot combines the running inode, verified staging and
// an independent accepted-update projection. Diagnostic Task retention does not
// erase installed software, captured release identity or its accepted outcome.
func (service *Service) ControllerUpdateSnapshot(ctx context.Context) (apiTypes.ControllerUpdateState, error) {
	if ctx == nil {
		return apiTypes.ControllerUpdateState{}, errs.New(
			errs.KindInternal,
			"Controller update snapshot context is required",
		)
	}
	state, journal, hasJournal := service.releaseSnapshot(ctx)
	latest, found, err := service.records.Current(ctx, 0)
	if err != nil {
		return unavailableSnapshot(ctx, state, "Controller update history is unavailable.")
	}
	if !found {
		return state, ctx.Err()
	}
	current := latest.Record
	status := "queued"
	if current.Started {
		status = "running"
	}
	if current.Outcome != "" {
		status = string(current.Outcome)
	}
	if current.Outcome != "" && !current.Settled {
		status = "needs_attention"
	}
	state.LastUpdate = &apiTypes.ControllerUpdateSummary{TaskID: current.Capture.TaskID, Release: string(current.Capture.Input.Release),
		Status: status, CreatedAt: current.Capture.CreatedAt}
	if hasJournal && journal.TaskID == current.Capture.TaskID {
		state.LastUpdate.Phase = string(journal.Phase)
	}
	return state, ctx.Err()
}

func (service *Service) releaseSnapshot(ctx context.Context) (apiTypes.ControllerUpdateState, upgrade.Journal, bool) {
	state := apiTypes.ControllerUpdateState{RunningSHA256: string(service.process)}
	if service.catalog == nil {
		return state, upgrade.Journal{}, false
	}
	journal, hasJournal, err := service.catalog.Current(ctx)
	if err != nil || (hasJournal && journal.Validate() != nil) {
		state.Error = "Native release state is unavailable or invalid."
		hasJournal = false
	}
	candidate, found, err := service.catalog.Candidate(ctx)
	if err != nil || (found && candidate.Validate() != nil) {
		state.Error = "Staged Controller release is unavailable or invalid."
	} else if found {
		state.Candidate = releaseResponse(candidate)
	}
	if state.Error == "" {
		installed, installedErr := service.catalog.Installed(ctx)
		switch {
		case hasJournal && !journal.Phase.Settled():
			state.Error = "Native Controller update recovery is active."
		case installedErr != nil || installed != service.process:
			state.Error = "Installed Controller recovery identity is unavailable."
		case service.unit.VerifyBootstrap(ctx) != nil:
			state.Error = "Guarded Controller bootstrap is unavailable."
		default:
			state.Available = true
		}
	}
	return state, journal, hasJournal
}

func unavailableSnapshot(
	ctx context.Context,
	state apiTypes.ControllerUpdateState,
	message string,
) (apiTypes.ControllerUpdateState, error) {
	if err := ctx.Err(); err != nil {
		return apiTypes.ControllerUpdateState{}, err
	}
	state.Available, state.Error = false, message
	return state, nil
}

func releaseResponse(release upgrade.Release) *apiTypes.ControllerRelease {
	manifest := release.Manifest
	response := &apiTypes.ControllerRelease{
		Release:           string(release.Release),
		ControllerSHA256:  string(manifest.ControllerSHA256),
		ControllerVersion: manifest.ControllerVersion,
		AgentImage:        manifest.AgentImage,
		StorageEpoch:      manifest.StorageEpoch,
		ChannelSchema:     manifest.ChannelSchema,
	}
	if manifest.ControllerSource.Known() {
		source := manifest.ControllerSource
		response.ControllerSource = &source
	}
	return response
}
