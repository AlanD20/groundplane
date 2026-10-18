package entrywork

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/entryexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func (repository *InputRepository) PrepareEntryCheckpoint(ctx context.Context, view workqueue.ResumeView,
	input *agentpb.EntryConfigurationWorkInput, progress entryexecution.Progress, at time.Time,
) (workqueue.Change, error) {
	captured, capture, guards, err := repository.Load(ctx, view.Work)
	if err != nil {
		return workqueue.Change{}, err
	}
	if !proto.Equal(input, captured) {
		return workqueue.Change{}, conflict("Entry report differs from published input")
	}
	if progress.InFlight && (view.Checkpoint.Record == nil || progress.Sequence > view.Checkpoint.Record.Sequence) {
		current, desired, err := repository.ReadDesiredMatch(ctx, view.Work)
		if err != nil {
			return workqueue.Change{}, err
		}
		if !current {
			return workqueue.Change{}, conflict("Entry configuration was replaced; another stage cannot start")
		}
		guards = append(guards, desired...)
		secrets, err := repository.ExecutionSecretConditions(ctx, view.Work)
		if err != nil {
			return workqueue.Change{}, err
		}
		consumers, err := repository.ConsumerConditions(ctx, capture, view.Work.ReadRevision)
		if err != nil {
			return workqueue.Change{}, err
		}
		guards = append(guards, secrets...)
		guards = append(guards, consumers...)
	}
	change, err := PrepareCheckpoint(ctx, repository.store, view, captured, progress, at)
	if err != nil {
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, guards...)
	return change, nil
}
