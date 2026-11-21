package operations

import (
	"math"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/composerender"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// A saved definition remains renderable for an explicit Deploy, but never
// acknowledges configuration files or rewrites the serving runtime.
func entryDefinitionProjection(source etcd.EntryMutationSource, records []entries.Record,
	taskID string,
) (*environmentprojection.EnvironmentComposeProjection, error) {
	if source.Runtime == nil {
		return nil, nil
	}
	generation := source.Desired.Record.RenderGeneration + 1
	if generation == 0 || generation > math.MaxInt32 {
		return nil, errs.New(errs.KindStateConflict, "Entry desired render generation is exhausted")
	}
	candidate, _, err := composerender.ProjectEnvironmentEntryMutation(source.Runtime.Record,
		composerender.EnvironmentEntryArtifactMutation{
			RevisionID: taskID, ArtifactID: entryStableIDFromRevision(ids.KindConfig, taskID),
			PlanID: entryStableIDFromRevision(ids.KindPlan, taskID), RenderGeneration: generation, Entries: records,
		})
	if err != nil {
		return nil, err
	}
	return &candidate, nil
}
