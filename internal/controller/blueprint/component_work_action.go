package blueprint

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	"github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/core"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runtimeconfiguration"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (service *Service) prepareComponentWorkAction(ctx context.Context, placement componentWorkPlacement,
	rendered componentWorkRender, files []taskmaterialization.Record, capture runtimeconfiguration.WorkCapture,
) (*agentpb.ComponentApply, error) {
	if rendered.Artifact == nil {
		return nil, nil
	}
	var prior runtimeconfiguration.Snapshot
	var err error
	if capture.Prior != nil {
		prior, err = service.repository.LoadComponentWorkConfiguration(ctx, *capture.Prior)
		if err != nil {
			return nil, err
		}
	}
	predecessorKind := core.ComponentKind("")
	current := placement.Selection.Current
	if current.RuntimePresent && current.Runtime.Record.Component.Desired.Enabled {
		predecessorKind = current.Runtime.Record.Component.Desired.Kind
	}
	return service.configuration.PrepareComponentWorkAction(ctx, taskplanning.EnvironmentManagedConfigApplyInput{
		RevisionID:       placement.Selection.Source.desired.Record.RevisionID,
		RenderGeneration: capture.Generation, Components: []componentrecord.Record{placement.Candidate},
		ComponentCatalog: service.componentCatalog, Materializations: files, Artifact: rendered.Artifact,
	}, capture, prior, predecessorKind)
}
