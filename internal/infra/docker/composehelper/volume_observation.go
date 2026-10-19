package composehelper

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/environmentpath"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/internal/common/volumeexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/docker/environmentdirectoryhelper"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// ObserveVolumeWork runs inside the host helper after its execution owner has
// accounted for outstanding helpers. It reads only the captured Volume, using
// the helper's configured root rather than trusting an input absolute path.
func ObserveVolumeWork(ctx context.Context, taskRunner runner.Runner, volumeRoot string,
	attempt workexecution.Attempt, input *agentpb.VolumeWorkInput,
) (volumeexecution.Observation, error) {
	if ctx == nil {
		return volumeexecution.Observation{}, errs.New(errs.KindInternal, "Volume observation requires a context")
	}
	if _, bounded := ctx.Deadline(); !bounded {
		return volumeexecution.Observation{}, errs.New(
			errs.KindValidationFailed,
			"Volume observation requires a deadline",
		)
	}
	// Observation serves both forward execution and read-only recovery. Its
	// caller has opened the corresponding helper envelope; bind these bytes to
	// the attempt without treating recovery as a new forward grant.
	owned, raw, reference, err := executionplan.CaptureVolumeWorkInput(attempt.Input.ID, input)
	clear(raw)
	if err != nil {
		return volumeexecution.Observation{}, err
	}
	if attempt.Validate() != nil || attempt.StartedAt == nil || attempt.FinishedAt != nil ||
		attempt.Executor.Executor != workexecution.Agent || reference != attempt.Input {
		return volumeexecution.Observation{}, errs.New(
			errs.KindStateConflict,
			"Volume observation requires a started attempt",
		)
	}
	scope, err := environmentpath.Parse(volumeRoot, owned.Candidate.AuthorizedVolumeDir)
	if err != nil || scope.EnvironmentID != owned.EnvironmentId {
		return volumeexecution.Observation{}, errs.New(
			errs.KindValidationFailed,
			"Volume observation directory is outside its configured owner",
		)
	}
	directory, err := (environmentdirectoryhelper.Creator{}).InspectManagedVolume(ctx,
		environmentdirectoryhelper.ManagedVolumeInspectRequest{
			VolumeRoot: volumeRoot, VolumeDir: owned.Candidate.AuthorizedVolumeDir,
			VolumeID: owned.VolumeId, ComposeKey: owned.ComposeKey,
		})
	if err != nil {
		return volumeexecution.Observation{}, err
	}
	docker, err := ObserveVolumeDocker(ctx, taskRunner, owned)
	if err != nil {
		return volumeexecution.Observation{}, err
	}
	if err := ctx.Err(); err != nil {
		return volumeexecution.Observation{}, err
	}
	return volumeexecution.Observation{
		VolumeID: owned.VolumeId, Input: attempt.Input, DirectoryReady: directory.Ready,
		DirectoryAbsent: directory.Absent, VolumeReady: docker.Ready, VolumeAbsent: docker.Absent,
		ConsumersDetached: docker.ConsumersDetached, At: time.Now().UTC(),
	}, nil
}
