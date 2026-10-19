package materialization

import (
	"context"
	"errors"
	"io"

	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// RunReleaseFile uses the descriptor-confined materializer directly, never a
// synthetic generic plan. The source owner has authenticated the captured file
// metadata; this boundary also binds it to the active attempt and configuration.
func (runtime *Runtime) RunReleaseFile(
	ctx context.Context,
	input releaseexecution.Input,
	report releaseexecution.CheckpointReport,
	header entrymaterialization.Header,
	source io.ReadCloser,
	verifyOnly bool,
) (err error) {
	if source == nil {
		return errs.New(errs.KindInternal, "Release configuration source is missing")
	}
	defer func() { err = errors.Join(err, source.Close()) }()
	if runtime == nil || runtime.helper == nil || ctx == nil || report.Ack == nil ||
		report.ValidateEnvelope(input.Attempt) != nil ||
		report.Progress.Validate(input) != nil ||
		!report.Progress.InFlight ||
		report.Progress.Phase != releaseexecution.Configuration {
		return errs.New(errs.KindStateConflict, "Release file has no acknowledged configuration boundary")
	}
	stage, err := report.Progress.StageID(input)
	if err != nil || stage != report.StageID || header.StepID() != stage || header.TaskID() != input.Attempt.TaskID ||
		header.EnvironmentID() != input.Release.EnvironmentId || header.Generation() != input.Release.Configuration.Generation {
		return errs.New(errs.KindStateConflict, "Release file metadata differs from its captured configuration")
	}
	selected := false
	for _, destination := range input.Release.ConfigurationDestinations {
		if destination == header.Destination() {
			selected = true
			break
		}
	}
	if !selected || input.Artifacts.Candidate == nil {
		return errs.New(errs.KindStateConflict, "Release file is outside the captured destination set")
	}
	return runtime.runFile(
		ctx,
		input.Artifacts.Candidate.AuthorizedVolumeDir,
		Payload{Header: header, Source: source},
		verifyOnly,
	)
}
