package entrywork

import (
	"context"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/entryexecution"
	"github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runtimeconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

func (repository *InputRepository) LoadActiveConfiguration(
	ctx context.Context,
	view workqueue.ResumeView,
) (entryexecution.FileInput, []taskmaterialization.Record, error) {
	var empty entryexecution.FileInput
	attempt := view.Attempt.Attempt
	if !view.ActiveAttempt || !view.CurrentExecutor || attempt.Direction != workexecution.Forward ||
		attempt.FinishedAt != nil ||
		attempt.StartedAt == nil ||
		view.Checkpoint.Record == nil ||
		view.Work.Task.Task.FinishedAt != nil ||
		view.Work.Task.Task.CollectionStartedAt != nil ||
		view.Work.Task.Task.Stop != nil {
		return empty, nil, conflict("Entry configuration requires an active file stage")
	}
	input, capture, _, err := repository.Load(ctx, view.Work)
	if err != nil {
		return empty, nil, err
	}
	if input == nil || input.Configuration == nil {
		return empty, nil, conflict("Entry configuration captured configuration is unavailable")
	}
	if _, err := repository.ExecutionSecretConditions(ctx, view.Work); err != nil {
		return empty, nil, err
	}
	key := ReportKey(attempt.ID, view.Checkpoint.Record.Sequence)
	read, err := repository.read(ctx, []string{key}, view.Work.ReadRevision)
	if err != nil {
		return empty, nil, err
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil {
		return empty, nil, conflict("Entry configuration write checkpoint is unavailable")
	}
	progress, err := entryexecution.DecodeProgress(attempt, input, read.Values[0].Value)
	if err != nil {
		return empty, nil, err
	}
	checkpoint, err := progress.Checkpoint(attempt, input)
	if err != nil || checkpoint != *view.Checkpoint.Record || !progress.InFlight ||
		(progress.Stage != entryexecution.WriteConfiguration && progress.Stage != entryexecution.VerifyConfiguration) {
		return empty, nil, conflict("Entry configuration does not match its acknowledged file stage")
	}
	sources, err := runtimeconfiguration.New(repository.store)
	if err != nil {
		return empty, nil, err
	}
	reference := capture.Configuration.Current
	if reference.ID != input.Configuration.ConfigurationId || reference.EnvironmentID != input.EnvironmentId ||
		reference.Generation != input.Configuration.Generation || reference.SHA256 != hex.EncodeToString(input.Configuration.Sha256) {
		return empty, nil, conflict("Entry configuration file source differs from captured configuration")
	}
	snapshot, err := sources.Load(ctx, reference, view.Work.ReadRevision)
	if err != nil {
		return empty, nil, err
	}
	byDestination := make(map[string]taskmaterialization.Record, len(snapshot.Files))
	for _, file := range snapshot.Files {
		byDestination[file.Destination] = file
	}
	files := make([]taskmaterialization.Record, 0, len(input.Files))
	for _, descriptor := range input.Files {
		file, ok := byDestination[descriptor.Destination]
		if !ok {
			return empty, nil, conflict("Entry configuration captured file is unavailable")
		}
		files = append(files, file)
	}
	return entryexecution.FileInput{Attempt: attempt, Procedure: input, Progress: progress}, files, nil
}
