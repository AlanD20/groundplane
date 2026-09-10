package controllerupgrade

import (
	"context"
	"time"

	upgrade "github.com/AlanD20/groundplane/internal/common/controllerupgrade"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ControllerUpdateSnapshot combines the running inode, verified staging and
// the latest durable Task. The activation phase is supplementary, never an
// invented Task result; pre-activation failures therefore remain visible.
func (service *Service) ControllerUpdateSnapshot(ctx context.Context) (apiTypes.ControllerUpdateState, error) {
	if ctx == nil {
		return apiTypes.ControllerUpdateState{}, errs.New(
			errs.KindInternal,
			"Controller update snapshot context is required",
		)
	}
	state, journal, hasJournal := service.releaseSnapshot(ctx)
	latest, found, err := service.tasks.LatestControllerUpdate(ctx)
	if err != nil {
		return unavailableSnapshot(ctx, state, "Controller update history is unavailable.")
	}
	if !found {
		return state, ctx.Err()
	}
	task := latest.Record
	input, err := DecodeTask(task, task.CreatedAt, task.CreatedAt.Add(upgrade.TaskTimeoutSeconds*time.Second))
	if err != nil {
		return unavailableSnapshot(ctx, state, "Controller update history is invalid.")
	}
	state.LastUpdate = &apiTypes.ControllerUpdateSummary{TaskID: task.ID, Release: string(input.Release),
		Status: string(task.Status), CreatedAt: task.CreatedAt}
	if hasJournal && journal.TaskID == task.ID {
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
	return &apiTypes.ControllerRelease{
		Release:           string(release.Release),
		ControllerSHA256:  string(manifest.ControllerSHA256),
		ControllerVersion: manifest.ControllerVersion,
		AgentImage:        manifest.AgentImage,
		StorageEpoch:      manifest.StorageEpoch,
		ChannelSchema:     manifest.ChannelSchema,
	}
}
