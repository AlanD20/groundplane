package entrywork

import (
	"context"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/entryexecution"
	"github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runtimeconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

func (repository *InputRepository) LoadRecoveryConfiguration(
	ctx context.Context,
	view workqueue.ResumeView,
) (entryexecution.FileInput, []taskmaterialization.Record, error) {
	var empty entryexecution.FileInput
	if !view.ActiveAttempt || !view.CurrentExecutor || view.Attempt.Attempt.FinishedAt != nil ||
		view.Attempt.Attempt.StartedAt == nil ||
		view.Work.Task.Task.FinishedAt != nil ||
		view.Work.Task.Task.CollectionStartedAt != nil {
		return empty, nil, conflict("Entry configuration restoration requires an active recovery attempt")
	}
	if _, err := repository.ExecutionSecretConditions(ctx, view.Work); err != nil {
		return empty, nil, err
	}
	recovery, err := repository.LoadRecoveryInput(ctx, view.Work, view.Attempt.Attempt)
	if err != nil {
		return empty, nil, err
	}
	_, capture, _, err := repository.Load(ctx, view.Work)
	if err != nil {
		return empty, nil, err
	}
	sources, err := runtimeconfiguration.New(repository.store)
	if err != nil {
		return empty, nil, err
	}
	current, err := sources.Load(ctx, capture.Configuration.Current, view.Work.ReadRevision)
	if err != nil {
		return empty, nil, err
	}
	var prior *runtimeconfiguration.Snapshot
	if capture.Configuration.Prior != nil {
		loaded, err := sources.Load(ctx, *capture.Configuration.Prior, view.Work.ReadRevision)
		if err != nil {
			return empty, nil, err
		}
		prior = &loaded
	}
	byDestination := make(map[string]taskmaterialization.Record, len(current.Files))
	for _, file := range current.Files {
		byDestination[file.Destination] = file
	}
	changes := make([]taskmaterialization.Record, 0, len(recovery.Input.Files))
	for _, descriptor := range recovery.Input.Files {
		file, ok := byDestination[descriptor.Destination]
		if !ok {
			return empty, nil, conflict("Entry restoration has no captured forward file")
		}
		changes = append(changes, file)
	}
	selected, err := runtimeconfiguration.SelectRestorations(
		recovery.Input.EnvironmentId,
		recovery.Input.RenderGeneration,
		prior,
		changes,
	)
	if err != nil {
		return empty, nil, err
	}
	files := make([]taskmaterialization.Record, 0, len(selected))
	for _, file := range selected {
		files = append(files, file.Prior)
	}
	slices.SortFunc(files, func(a, b taskmaterialization.Record) int {
		if a.Destination < b.Destination {
			return -1
		}
		if a.Destination > b.Destination {
			return 1
		}
		return 0
	})
	return entryexecution.FileInput{
		Attempt:   view.Attempt.Attempt,
		Procedure: recovery.Input,
		Recovery:  &recovery.Source,
		Progress:  recovery.Boundary,
	}, files, nil
}
