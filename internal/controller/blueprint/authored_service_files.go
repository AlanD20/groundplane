package blueprint

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/composerender"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Compose resolves every declared env_file while loading the candidate, even
// when up selects one Service. Prepare the sealed input files before any native
// rollout; file acknowledgement and recovery are bound to the native work input.
func (service *Service) prepareAuthoredServiceFiles(
	ctx context.Context,
	work workqueue.View,
	projection projectionrecord.EnvironmentComposeProjection,
	input authoredParentInput,
	artifact *agentpb.ComposeArtifact,
	allocate func(ids.Kind, string) string,
) ([]taskmaterialization.Record, error) {
	if work.Task.Task.Validate() != nil || work.Item.Item.Validate() != nil ||
		work.Item.Item.TaskID != work.Task.Task.ID || work.Item.Item.Operation != workexecution.Release ||
		work.Task.Task.Scope.EnvironmentID != projection.EnvironmentID ||
		projection.RevisionID != work.Task.Task.ID || artifact == nil ||
		artifact.GetOwnerId() != projection.EnvironmentID || allocate == nil {
		return nil, errs.New(errs.KindValidationFailed, "Service file preparation requires its native Blueprint work")
	}
	project, err := composerender.LoadNormalizedEnvironmentProject(ctx, projection)
	if err != nil {
		return nil, err
	}
	identities, err := authoredOwnedIdentitySnapshot(ctx, input.desired, input.identities)
	if err != nil {
		return nil, err
	}
	entries, err := composerender.ProjectEnvironmentEntries(
		project, projection.EnvironmentID, artifact.GetAuthorizedVolumeDir(),
		identities.Services, projection.Entries,
	)
	if err != nil {
		return nil, err
	}
	return service.environmentFileSources(
		ctx, projection.EnvironmentID, work.Task.Task.Scope.ProjectID, projection.RevisionID,
		projection.RenderGeneration, allocate, projection.RuntimeFiles,
		composerender.EnvironmentComponentComposeProjection{}, entries.Materializations, projection.Entries,
	)
}
