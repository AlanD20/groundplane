package attachprovision

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/attachexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintattachoutputs"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

type CompletedInput struct {
	Source     CapturedInput
	Progress   attachexecution.ProvisionProgress
	Facts      attachments.EncryptedFacts
	Conditions []keyvalue.Condition
}

func (completed *CompletedInput) Clear() {
	if completed != nil {
		completed.Source.Clear()
		clear(completed.Facts.Ciphertext)
		*completed = CompletedInput{}
	}
}

// LoadCompleted requires both command proof and immutable fact output. It does
// not itself settle a binding; the returned comparisons join that transaction.
func (repository *InputRepository) LoadCompleted(ctx context.Context, view workqueue.ResumeView,
	input attachexecution.ProvisionInput, at time.Time,
) (_ CompletedInput, resultErr error) {
	attempt := view.Attempt.Attempt
	if !view.ActiveAttempt || attempt.FinishedAt != nil || view.Work.Task.Task.CollectionStartedAt != nil ||
		view.Work.Task.Task.FinishedAt != nil || at.IsZero() || at.Location() != time.UTC {
		return CompletedInput{}, checkpointConflict("Attach completion no longer owns active execution")
	}
	authority, err := input.Authority()
	if err != nil {
		return CompletedInput{}, err
	}
	progress, guards, err := repository.LoadProgress(ctx, view, input)
	if err != nil {
		return CompletedInput{}, err
	}
	if progress == nil || !progress.Complete(attempt, authority) || at.Before(progress.UpdatedAt) {
		return CompletedInput{}, checkpointConflict("Attach completion requires every captured command to succeed")
	}
	source, err := repository.LoadCaptured(ctx, view.Work)
	if err != nil {
		return CompletedInput{}, err
	}
	completed := CompletedInput{Source: source, Progress: *progress,
		Conditions: append(append([]keyvalue.Condition(nil), source.Conditions...), guards...)}
	defer func() {
		if resultErr != nil {
			completed.Clear()
		}
	}()
	capture := source.Generation.Record
	expected, err := capture.ProvisionAuthority()
	if err != nil {
		return CompletedInput{}, err
	}
	if capture.SettlesInController() || authority != expected {
		return CompletedInput{}, checkpointConflict("Attach completion differs from the captured provisioning target")
	}
	completed.Conditions = append(completed.Conditions,
		keyvalue.Condition{Key: workqueue.AttemptKey(attempt.ID), ModRevision: view.Attempt.Revision})
	if !authority.Custom {
		completed.Facts = capture.GeneratedInputs
		completed.Facts.Ciphertext = append([]byte(nil), capture.GeneratedInputs.Ciphertext...)
		return completed, nil
	}
	key := blueprintattachoutputs.Key(capture.AttachID, attempt.ID)
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: view.Work.ReadRevision})
	if read != nil {
		defer keyvalue.ClearValues(read.Values)
	}
	if err != nil {
		return CompletedInput{}, err
	}
	if read == nil || read.ReadRevision != view.Work.ReadRevision || len(read.Values) != 1 || read.Values[0] == nil ||
		read.Values[0].Key != key || read.Values[0].ModRevision <= 0 || read.Values[0].ModRevision > view.Work.ReadRevision {
		return CompletedInput{}, checkpointConflict("Custom Attach completion has no durable hook facts")
	}
	output, err := blueprintattachoutputs.Decode(read.Values[0].Value)
	if err != nil {
		return CompletedInput{}, err
	}
	defer blueprintattachoutputs.Clear(&output)
	if output.EnvironmentID != capture.EnvironmentID || output.TaskID != attempt.TaskID || output.WorkItemID != attempt.WorkItemID ||
		output.AttemptID != attempt.ID || output.Executor != attempt.Executor || output.Input != attempt.Input ||
		output.AttachID != capture.AttachID || output.OperationID != capture.OperationID ||
		output.CreatedAt.Before(*attempt.StartedAt) || output.CreatedAt.After(progress.LastResult.FinishedAt) {
		return CompletedInput{}, checkpointConflict("Custom Attach facts differ from the completed execution")
	}
	completed.Facts = output.Facts
	completed.Facts.Ciphertext = append([]byte(nil), output.Facts.Ciphertext...)
	completed.Conditions = append(completed.Conditions, keyvalue.Condition{Key: key, ModRevision: read.Values[0].ModRevision})
	return completed, nil
}
