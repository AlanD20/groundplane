package configurationrecovery

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runtimeconfiguration"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// UnchangedComponentFile selects observation evidence from the exact snapshot
// pinned before this Task. It grants no write authority and rejects any path
// the Task will materialize, including a removal of that path.
func (sources *Sources) UnchangedComponentFile(ctx context.Context, task etcd.TaskRecord,
	componentID, destination string,
) (taskmaterialization.Record, error) {
	if ids.Validate(ids.KindComponent, componentID) != nil {
		return taskmaterialization.Record{}, errs.New(errs.KindStateConflict, "retained Component identity is invalid")
	}
	previous, err := sources.priorSnapshot(ctx, task)
	if err != nil {
		return taskmaterialization.Record{}, err
	}
	if previous == nil {
		return taskmaterialization.Record{}, errs.New(
			errs.KindStateConflict,
			"Component predecessor configuration is absent",
		)
	}
	for _, change := range task.Materializations {
		if change.Destination == destination {
			return taskmaterialization.Record{}, errs.New(
				errs.KindStateConflict,
				"retained Component path is written by this Task",
			)
		}
	}
	for _, file := range previous.Files {
		if file.Destination != destination {
			continue
		}
		if file.EnvironmentID != task.Owner.EnvironmentID || file.OutputKind != taskmaterialization.OutputPlainFile ||
			file.ServiceID != "" || file.ServiceName != "" || file.Source.Kind != taskmaterialization.SourceComponentFile ||
			file.Source.ComponentFile == nil || file.Source.ComponentFile.ComponentID != componentID {
			return taskmaterialization.Record{}, errs.New(
				errs.KindStateConflict,
				"retained configuration is not this Component's plain file",
			)
		}
		return taskmaterialization.Clone([]taskmaterialization.Record{file})[0], nil
	}
	return taskmaterialization.Record{}, errs.New(
		errs.KindStateConflict,
		"Component predecessor file is missing from its retained snapshot",
	)
}

func (sources *Sources) priorSnapshot(
	ctx context.Context,
	task etcd.TaskRecord,
) (*runtimeconfiguration.Snapshot, error) {
	if ctx == nil || sources == nil || sources.snapshots == nil || task.Configuration == nil ||
		task.CreatedAt.IsZero() || ids.Validate(ids.KindTask, task.ID) != nil ||
		ids.Validate(ids.KindPlan, task.PlanID) != nil || task.RenderGeneration <= 0 {
		return nil, errs.New(errs.KindStateConflict, "Task configuration recovery authority is missing")
	}
	configuration := task.Configuration
	if runtimeconfiguration.ValidateReference(configuration.Current) != nil ||
		configuration.Current.EnvironmentID != task.Owner.EnvironmentID ||
		configuration.Current.Generation > uint64(task.RenderGeneration) ||
		(configuration.Prior == nil) != (configuration.PriorRevision == 0) || configuration.PriorRevision < 0 {
		return nil, errs.New(errs.KindStateConflict, "Task configuration recovery authority is inconsistent")
	}
	if configuration.Prior == nil {
		return nil, nil
	}
	if runtimeconfiguration.ValidateReference(*configuration.Prior) != nil ||
		configuration.Prior.EnvironmentID != task.Owner.EnvironmentID ||
		configuration.Prior.Generation > uint64(task.RenderGeneration) {
		return nil, errs.New(errs.KindStateConflict, "Task predecessor configuration reference is inconsistent")
	}
	retained, err := sources.snapshots.LoadRetained(ctx, *configuration.Prior)
	if err != nil {
		return nil, err
	}
	return &retained, nil
}
