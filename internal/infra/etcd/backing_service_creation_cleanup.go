package etcd

import (
	"context"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const backingCreationCleanupCursor = "/v1/staging/backing-service-cleanup-cursor"

// CleanupBackingServiceCreationStages releases private encrypted bootstrap
// seeds after their pure accepted Task has failed or its history was collected.
// Active preparation and every published native intent retain their draft.
func (repository *HierarchyRepository) CleanupBackingServiceCreationStages(
	ctx context.Context,
	at time.Time,
	limit int,
) (int, error) {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return 0, err
	}
	if !blueprints.ValidBlueprintRecordTime(at) || limit < 1 || limit > 128 {
		return 0, errs.New(errs.KindValidationFailed, "Backing creation cleanup request is invalid")
	}
	cursor, err := repository.store.Get(ctx, backingCreationCleanupCursor)
	if err != nil {
		return 0, err
	}
	var after string
	var cursorRevision int64
	if cursor == nil {
		return 0, errs.New(errs.KindInternal, "Backing creation cleanup cursor read is incomplete")
	}
	if cursor.Entry != nil {
		after, err = recordcodec.Decode[string](cursor.Entry.Value, "backing_creation_cleanup_cursor")
		clear(cursor.Entry.Value)
		if err != nil {
			return 0, err
		}
		if !strings.HasPrefix(after, backingServiceCreationStagePrefix) || cursor.Entry.ModRevision <= 0 {
			return 0, errs.New(errs.KindInternal, "Backing creation cleanup cursor is invalid")
		}
		cursorRevision = cursor.Entry.ModRevision
	}
	page, err := repository.store.Range(
		ctx,
		keyvalue.RangeRequest{Prefix: backingServiceCreationStagePrefix, Limit: int64(limit), StartExclusive: after},
	)
	if err != nil {
		return 0, err
	}
	if page == nil || page.ReadRevision <= 0 || len(page.Values) > limit {
		return 0, errs.New(errs.KindInternal, "Backing creation cleanup page is incomplete")
	}
	processed := 0
	for _, entry := range page.Values {
		stage, err := decodeBackingServiceCreationStage(entry.Value)
		clear(entry.Value)
		if err != nil {
			return processed, err
		}
		clear(stage.BootstrapSeed.Ciphertext)
		key, err := backingServiceCreationStageKey(stage.Locator)
		if err != nil || key != entry.Key || entry.ModRevision <= 0 {
			return processed, errs.New(errs.KindInternal, "Backing creation cleanup stage identity is invalid")
		}
		keys := []string{workqueue.TaskKey(stage.TaskID), blueprints.EnvironmentBlueprintHeadKey(stage.EnvironmentID)}
		read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys})
		if err != nil {
			return processed, err
		}
		if read == nil || len(read.Values) != 2 {
			return processed, errs.New(errs.KindInternal, "Backing creation cleanup authority is incomplete")
		}
		conditions := []keyvalue.Condition{
			{Key: entry.Key, ModRevision: entry.ModRevision},
			{Key: keys[0]},
			{Key: keys[1]},
		}
		eligible := read.Values[1] == nil
		if read.Values[0] != nil {
			task, decodeErr := workqueue.DecodeTask(read.Values[0].Value)
			if decodeErr != nil {
				keyvalue.ClearValues(read.Values)
				return processed, decodeErr
			}
			eligible = eligible && task.ID == stage.TaskID && task.AcceptedIntent != nil && task.ExpectedItems == 0 &&
				task.Progress.Total() == 0 &&
				task.FinishedAt != nil &&
				task.Scope.ProjectID == stage.ProjectID &&
				task.Scope.EnvironmentID == stage.EnvironmentID
			conditions[1].ModRevision = read.Values[0].ModRevision
		} else {
			eligible = eligible && !at.Before(stage.CreatedAt.Add(24*time.Hour))
		}
		keyvalue.ClearValues(read.Values)
		if !eligible {
			continue
		}
		result, err := repository.store.Transact(
			ctx,
			conditions,
			[]keyvalue.Mutation{{Type: keyvalue.MutationDelete, Key: entry.Key}},
		)
		keyvalue.ClearValues(result.FailureReads)
		if err != nil {
			return processed, err
		}
		if result.Succeeded {
			processed++
		}
	}
	mutation := keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: backingCreationCleanupCursor}
	if page.More && len(page.Values) != 0 {
		value, err := recordcodec.Encode("backing_creation_cleanup_cursor", page.Values[len(page.Values)-1].Key)
		if err != nil {
			return processed, err
		}
		defer clear(value)
		mutation.Type, mutation.Value = keyvalue.MutationPut, value
	}
	result, err := repository.store.Transact(
		ctx,
		[]keyvalue.Condition{{Key: backingCreationCleanupCursor, ModRevision: cursorRevision}},
		[]keyvalue.Mutation{mutation},
	)
	keyvalue.ClearValues(result.FailureReads)
	return processed, err
}
