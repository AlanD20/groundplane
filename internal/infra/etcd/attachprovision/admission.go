package attachprovision

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// ReadDesiredMatch orders new provisioning permission against Blueprint
// publication. Already-authorized results use the captured binding instead.
func (repository *InputRepository) ReadDesiredMatch(
	ctx context.Context,
	view workqueue.View,
) (bool, []keyvalue.Condition, error) {
	source, err := repository.LoadCaptured(ctx, view)
	if err != nil {
		return false, nil, err
	}
	defer source.Clear()
	key := blueprints.EnvironmentBlueprintHeadKey(source.Generation.Record.EnvironmentID)
	read, err := repository.store.GetMany(
		ctx,
		keyvalue.GetManyRequest{Keys: []string{key}, Revision: view.ReadRevision},
	)
	if read != nil {
		defer keyvalue.ClearValues(read.Values)
	}
	if err != nil {
		return false, nil, err
	}
	if read == nil || read.ReadRevision != view.ReadRevision || len(read.Values) != 1 {
		return false, nil, checkpointConflict("Attach desired-state read is incomplete")
	}
	value := read.Values[0]
	guards := append(source.Conditions, keyvalue.Condition{Key: key, ModRevision: keyvalue.RevisionOf(value)})
	if value == nil {
		return false, guards, nil
	}
	if value.Key != key || value.ModRevision <= 0 || value.ModRevision > view.ReadRevision {
		return false, nil, checkpointConflict("Attach desired-state revision is invalid")
	}
	head, err := idempotency.DecodeTaskReference(value.Value)
	if err != nil {
		return false, nil, err
	}
	if view.Task.Task.Action == workexecution.ActionAttach {
		// A direct resource edit does not own the Environment's Apply head.
		// Compare its authored binding, while preserving the captured procedure
		// and encrypted inputs; unrelated desired edits do not supersede it.
		capture := source.Generation.Record
		desired, found, err := blueprints.ReadDesiredInputRevisionAt(
			ctx,
			repository.store,
			capture.EnvironmentID,
			head,
			view.ReadRevision,
		)
		if err != nil {
			return false, nil, err
		}
		if !found || desired.ReadRevision != view.ReadRevision {
			return false, nil, checkpointConflict("Attach desired input is unavailable")
		}
		guards = append(
			guards,
			keyvalue.Condition{
				Key:         blueprints.EnvironmentBlueprintRootKey(capture.EnvironmentID, head),
				ModRevision: desired.Revision,
			},
		)
		spec, found := desired.Record.Input.Attachments[capture.AttachName]
		if !found {
			return false, guards, nil
		}
		fingerprint, err := core.AttachmentSpecFingerprint(spec)
		if err != nil {
			return false, nil, err
		}
		if fingerprint != capture.AuthoredSpecSHA256 {
			return false, guards, nil
		}
	} else if head != source.Generation.Record.RevisionID {
		return false, guards, nil
	}
	current, runtimeGuards, err := repository.consumerAdmission(ctx, view, source.Generation.Record)
	if err != nil {
		return false, nil, err
	}
	return current, append(guards, runtimeGuards...), nil
}
