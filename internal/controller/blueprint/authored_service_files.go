package blueprint

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/composerender"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Compose resolves every declared env_file while loading the candidate, even
// when up selects one Service. Prepare the sealed input files before any native
// rollout step; configuration acknowledgement and recovery remain Task-owned.
func (service *Service) prepareAuthoredServiceFiles(
	ctx context.Context,
	child etcd.TaskRecord,
	projection projectionrecord.EnvironmentComposeProjection,
	input authoredParentInput,
	artifact *agentpb.ComposeArtifact,
	allocate func(ids.Kind, string) string,
) (etcd.TaskRecord, []*agentpb.ExecutionStep, error) {
	project, err := composerender.LoadNormalizedEnvironmentProject(ctx, projection)
	if err != nil {
		return etcd.TaskRecord{}, nil, err
	}
	identities, err := authoredOwnedIdentitySnapshot(ctx, input.desired, input.identities)
	if err != nil {
		return etcd.TaskRecord{}, nil, err
	}
	entries, err := composerender.ProjectEnvironmentEntries(
		project, projection.EnvironmentID, artifact.GetAuthorizedVolumeDir(),
		identities.Services, projection.Entries,
	)
	if err != nil {
		return etcd.TaskRecord{}, nil, err
	}
	materializations, steps, err := service.environmentComponentMaterializations(
		ctx, projection.EnvironmentID, child.Owner.ProjectID, projection.RevisionID,
		artifact.GetArtifactId(), projection.RenderGeneration, allocate, projection.RuntimeFiles,
		composerender.EnvironmentComponentComposeProjection{}, entries.Materializations, projection.Entries,
	)
	if err != nil {
		return etcd.TaskRecord{}, nil, err
	}
	child.Materializations = materializations
	for _, step := range steps {
		step.TimeoutSeconds = uint32(child.TimeoutSeconds)
	}
	return child, steps, nil
}
