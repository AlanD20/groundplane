package etcd

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/desiredauthoring"
	"github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	"github.com/AlanD20/groundplane/internal/infra/etcd/entrywork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentfence"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runtimeconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type EntryMetadataPublication struct {
	Source     EntryMutationSource
	Records    []entries.Record
	Projection *environmentprojection.EnvironmentComposeProjection
	Changed    []string
	Accepted   workqueue.TaskSnapshot
	Task       workexecution.Task
	Item       workexecution.Item
	Marker     idempotency.IdempotencyMarker
	At         time.Time
}

// PrepareEntryDefinitionWork saves authored values without touching running
// workloads or their acknowledged configuration. Removal additionally requires
// no affected runtime or file. The desired head and epoch fence publication.
func (repository *EnvironmentBlueprintRepository) PrepareEntryDefinitionWork(
	ctx context.Context, request EntryMetadataPublication,
) (EntryWorkCapture, error) {
	source, task := request.Source, request.Task
	if source.Desired.ReadRevision <= 0 || task.Validate() != nil ||
		task.Action == workexecution.ActionRemove && source.Runtime != nil ||
		request.Item.Validate() != nil || request.Item.Placement.Executor != workexecution.Controller ||
		task.Scope.EnvironmentID != source.Desired.Record.EnvironmentID || request.Item.TaskID != task.ID ||
		request.Marker.TaskID != task.ID || task.Intent.SHA256 != request.Marker.Intent.CiphertextDigest ||
		request.At.IsZero() || request.At.Before(task.CreatedAt) || len(request.Changed) == 0 ||
		(task.TargetID != task.Scope.EnvironmentID && (len(request.Changed) != 1 || request.Changed[0] != task.TargetID)) {
		return EntryWorkCapture{}, errs.New(
			errs.KindValidationFailed,
			"Entry metadata publication has invalid authority",
		)
	}
	authority, err := repository.readEntryMutationSourceAt(ctx, task.Scope.EnvironmentID, source.Desired.ReadRevision)
	if err != nil {
		return EntryWorkCapture{}, err
	}
	if task.Action == workexecution.ActionRemove && authority.Runtime != nil ||
		authority.Desired.Revision != source.Desired.Revision ||
		authority.Desired.Record.RevisionID != source.Desired.Record.RevisionID {
		return EntryWorkCapture{}, errs.New(errs.KindStateConflict, "Entry metadata source changed")
	}
	source, request.Source = authority, authority
	if err := repository.validateEntryMetadataRecords(ctx, request); err != nil {
		return EntryWorkCapture{}, err
	}
	fence, err := environmentfence.LoadOrdinary(
		ctx,
		repository.store,
		task.Scope.EnvironmentID,
		source.Desired.ReadRevision,
	)
	if err != nil {
		return EntryWorkCapture{}, err
	}
	configuration, runtimeGuards, err := repository.entryMetadataHostProof(ctx, source, request.Changed, task.Action)
	if err != nil {
		return EntryWorkCapture{}, err
	}
	publication, err := desiredauthoring.PrepareEntryDefinitions(ctx, repository.store,
		task.Scope.EnvironmentID, request.Marker, source.Desired.Revision, request.Records, request.Projection)
	if err != nil {
		return EntryWorkCapture{}, err
	}
	defer keyvalue.ClearMutationValues(publication.Mutations)
	ready, change, err := entrywork.PrepareMetadataPublication(task, request.Item, configuration,
		request.At)
	if err != nil {
		return EntryWorkCapture{}, err
	}
	defer keyvalue.ClearMutationValues(change.Mutations)
	change.Conditions = append(change.Conditions, publication.Conditions...)
	change.Conditions = append(change.Conditions, runtimeGuards...)
	change.Conditions, err = environmentfence.AppendConditions(change.Conditions, fence)
	if err != nil {
		return EntryWorkCapture{}, err
	}
	change.Conditions = append(change.Conditions, keyvalue.Condition{
		Key: runtimeconfiguration.HeadKey(task.Scope.EnvironmentID), ModRevision: configuration,
	})
	for _, mutation := range publication.Mutations {
		mutation.Value = append([]byte(nil), mutation.Value...)
		change.Mutations = append(change.Mutations, mutation)
	}
	if request.Accepted.Revision <= 0 || !request.Accepted.Task.PendingPreparation() ||
		request.Accepted.Task.ID != task.ID {
		return EntryWorkCapture{}, errs.New(errs.KindStateConflict, "Entry metadata lost its accepted Task")
	}
	change.Conditions = append(
		change.Conditions,
		keyvalue.Condition{Key: workqueue.TaskKey(task.ID), ModRevision: request.Accepted.Revision},
	)
	if task.Action == workexecution.ActionRemove {
		removal, err := repository.prepareEntryMetadataRemoval(ctx, request)
		if err != nil {
			return EntryWorkCapture{}, err
		}
		change.Conditions = append(change.Conditions, removal.Conditions...)
		change.Mutations = append(change.Mutations, removal.Mutations...)
	}
	epoch, err := fence.EpochRewriteMutation()
	if err != nil {
		return EntryWorkCapture{}, err
	}
	change.Mutations = append(change.Mutations, epoch)
	change.Mutations = keyvalue.CloneMutations(change.Mutations)
	decision := *ready.Result
	return EntryWorkCapture{Task: task, Item: request.Item, Decision: &decision, Change: change}, nil
}

func (repository *EnvironmentBlueprintRepository) entryMetadataHostProof(ctx context.Context,
	source EntryMutationSource, changed []string, action workexecution.Action,
) (int64, []keyvalue.Condition, error) {
	files, err := runtimeconfiguration.New(repository.store)
	if err != nil {
		return 0, nil, err
	}
	snapshot, head, err := files.ReadAcknowledged(ctx, source.Desired.Record.EnvironmentID, source.Desired.ReadRevision)
	if err != nil {
		return 0, nil, err
	}
	// Saving a new selection leaves the entire acknowledged snapshot and every
	// running consumer intact. Only an explicit deployment activates it.
	if action != workexecution.ActionRemove {
		return head, nil, nil
	}
	// Captured files can belong to a Service omitted by a later failed Apply.
	// Its runtime must also remain absent before deferring file materialization.
	serviceIDs := make([]string, 0, len(source.Owned.Services)+len(snapshot.Files))
	seen := make(map[string]bool)
	for _, service := range source.Owned.Services {
		if !seen[service.ID] {
			serviceIDs = append(serviceIDs, service.ID)
			seen[service.ID] = true
		}
	}
	for _, file := range snapshot.Files {
		if file.ServiceID == "" {
			continue
		}
		if ids.Validate(ids.KindService, file.ServiceID) != nil {
			return 0, nil, errs.New(errs.KindInternal, "Captured configuration has an invalid consumer")
		}
		if !seen[file.ServiceID] {
			serviceIDs = append(serviceIDs, file.ServiceID)
			seen[file.ServiceID] = true
		}
	}
	var guards []keyvalue.Condition
	for start := 0; start < len(serviceIDs); start += 64 {
		end := min(start+64, len(serviceIDs))
		keys := make([]string, 0, end-start)
		for _, id := range serviceIDs[start:end] {
			keys = append(keys, serviceruntimerecord.Key(id))
		}
		read, err := repository.store.GetMany(
			ctx,
			keyvalue.GetManyRequest{Keys: keys, Revision: source.Desired.ReadRevision},
		)
		if err != nil {
			return 0, nil, err
		}
		if read == nil || read.ReadRevision != source.Desired.ReadRevision || len(read.Values) != len(keys) {
			return 0, nil, errs.New(errs.KindInternal, "Entry runtime observation is incomplete")
		}
		present := false
		for _, value := range read.Values {
			present = present || value != nil
		}
		keyvalue.ClearValues(read.Values)
		for _, key := range keys {
			guards = append(guards, keyvalue.Condition{Key: key})
		}
		if present {
			return 0, nil, errs.New(
				errs.KindStateConflict,
				"Entry change affects acknowledged runtime; finish preparing the Environment configuration",
			)
		}
	}
	// Removal needs Agent-owned cleanup before forgetting an applied file.
	targets := make(map[string]bool, len(changed))
	for _, id := range changed {
		targets[id] = true
	}
	for _, file := range snapshot.Files {
		if file.Source.EntryValue != nil && targets[file.Source.EntryValue.EntryID] {
			return 0, nil, entryDefinitionConflict()
		}
		if generated := file.Source.GeneratedEnvironment; generated != nil {
			for _, value := range generated.Values {
				if targets[value.Value.EntryID] {
					return 0, nil, entryDefinitionConflict()
				}
			}
		}
	}
	return head, guards, nil
}
