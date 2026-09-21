package blueprint

import (
	"context"
	composeidentity "github.com/AlanD20/groundplane/internal/controller/composeidentity"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"

	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/pkg/errs"
	"math"
)

// applyBaseline binds one attempt to its hierarchy and prior desired state.
type applyBaseline struct {
	environment          etcdstore.Versioned[hierarchyrecord.EnvironmentRecord]
	project              etcdstore.Versioned[hierarchyrecord.ProjectRecord]
	tenant               etcdstore.Versioned[hierarchyrecord.TenantRecord]
	taskOwner            taskjournal.TaskOwner
	previousProjection   etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]
	hasProjection        bool
	expectedHeadRevision int64
	previous             composeidentity.Snapshot
	generation           uint64
}

func (service *Service) loadApplyBaseline(ctx context.Context, environmentID, expectedRevision string) (applyBaseline, error) {
	environment, err := service.repository.GetEnvironment(ctx, environmentID)
	if err != nil {
		return applyBaseline{}, err
	}
	project, err := service.repository.GetProject(ctx, environment.Record.ProjectID)
	if err != nil {
		return applyBaseline{}, err
	}
	tenant, err := service.repository.GetTenant(ctx, project.Record.TenantID)
	if err != nil {
		return applyBaseline{}, err
	}
	taskOwner, err := taskjournal.EnvironmentTaskOwner(project.Record, environment.Record)
	if err != nil {
		return applyBaseline{}, err
	}
	if environment.Record.ProjectID != project.Record.ID || project.Record.TenantID != tenant.Record.ID ||
		project.Record.Kind != hierarchyrecord.ProjectKindTenant {
		return applyBaseline{}, errs.New(errs.KindInternal, "Environment hierarchy is inconsistent")
	}

	head, hasHead, err := service.repository.GetEnvironmentBlueprintHead(ctx, environmentID)
	if err != nil {
		return applyBaseline{}, err
	}
	previousProjection, hasProjection, err := service.repository.GetEnvironmentComposeProjection(ctx, environmentID)
	if err != nil {
		return applyBaseline{}, err
	}
	expectedHeadRevision, previous, generation, err := environmentBlueprintState(
		environmentID, head, hasHead, previousProjection, hasProjection,
	)
	if err != nil {
		return applyBaseline{}, err
	}
	if expectedRevision != "" && expectedRevision != environmentBlueprintRevision(head, hasHead) {
		return applyBaseline{}, errs.New(
			errs.KindStateConflict,
			"Environment Blueprint changed after the authoring revision was loaded",
		)
	}
	if generation > math.MaxInt32 {
		return applyBaseline{}, errs.New(errs.KindInternal, "Environment render generation is exhausted")
	}

	return applyBaseline{environment: environment, project: project, tenant: tenant, taskOwner: taskOwner, previousProjection: previousProjection, hasProjection: hasProjection, expectedHeadRevision: expectedHeadRevision, previous: previous, generation: generation}, nil
}
