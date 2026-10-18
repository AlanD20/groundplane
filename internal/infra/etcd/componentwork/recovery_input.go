package componentwork

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/componentexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type RecoveryInput struct {
	Input      *agentpb.ComponentWorkInput
	Source     componentexecution.Recovery
	Boundary   componentexecution.Progress
	Conditions []keyvalue.Condition
}

func (repository *InputRepository) LoadRecoveryInput(ctx context.Context, view workqueue.View, attempt workexecution.Attempt) (RecoveryInput, error) {
	input, _, guards, err := repository.Load(ctx, view)
	if err != nil {
		return RecoveryInput{}, err
	}
	saved, err := repository.LoadRecovery(ctx, view)
	if err != nil {
		return RecoveryInput{}, err
	}
	if err := saved.Source.ValidateFor(attempt, input); err != nil {
		return RecoveryInput{}, err
	}
	checkpoint := saved.Source.Failure.Checkpoint
	if checkpoint == nil {
		return RecoveryInput{}, conflict("Component recovery has no interrupted checkpoint")
	}
	key := ReportKey(saved.Source.Forward.ID, checkpoint.Sequence)
	read, err := repository.read(ctx, []string{key}, view.ReadRevision)
	if err != nil {
		return RecoveryInput{}, err
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil {
		return RecoveryInput{}, conflict("Component recovery checkpoint report is unavailable")
	}
	boundary, err := componentexecution.DecodeProgress(saved.Source.Forward, input, read.Values[0].Value)
	if err != nil {
		return RecoveryInput{}, err
	}
	expected, err := boundary.Checkpoint(saved.Source.Forward, input)
	if err != nil || expected != *checkpoint {
		return RecoveryInput{}, conflict("Component recovery checkpoint differs from the captured failure")
	}
	guards = append(guards, keyvalue.Condition{Key: RecoveryKey(view.Item.Item.ID), ModRevision: saved.Revision},
		keyvalue.Condition{Key: key, ModRevision: read.Values[0].ModRevision})
	return RecoveryInput{Input: input, Source: saved.Source, Boundary: boundary, Conditions: guards}, nil
}
