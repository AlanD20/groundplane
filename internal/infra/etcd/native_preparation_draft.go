package etcd

import (
	"bytes"
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"time"
)

const nativePreparationDraftPrefix = "/v1/private/native-preparation-drafts/"

func nativePreparationDraftKey(taskID string) string { return nativePreparationDraftPrefix + taskID }

// nativePreparationDraft is custody for inert native chunks, not executable
// authority. It precedes every chunk and survives loss of the compiler process.
type nativePreparationDraft struct {
	TaskID   string                                    `json:"task_id"`
	Intent   workexecution.Input                       `json:"intent"`
	Kind     string                                    `json:"kind"`
	WorkID   string                                    `json:"work_id"`
	Cleaned  uint32                                    `json:"cleaned"`
	Detaches []string                                  `json:"detaches,omitempty"`
	Claim    blueprints.EnvironmentBlueprintStageClaim `json:"claim"`
}

func (draft nativePreparationDraft) validate() error {
	if err := blueprints.ValidateEnvironmentBlueprintStageClaim(draft.Claim); err != nil {
		return err
	}
	if draft.WorkID != ids.DeriveAt(ids.KindWorkItem, draft.Claim.CreatedAt, draft.TaskID, draft.Kind) {
		return errs.New(errs.KindStateConflict, "native draft work identity differs from its Task")
	}
	if ids.Validate(ids.KindTask, draft.TaskID) != nil || draft.Intent.Validate() != nil ||
		ids.Validate(ids.KindWorkItem, draft.WorkID) != nil ||
		draft.Claim.TaskID != draft.TaskID ||
		draft.Claim.TaskLocator == nil ||
		len(draft.Detaches) >= int(workexecution.MaximumTaskItems) ||
		draft.Kind != "volume" && draft.Kind != "network" ||
		draft.Kind == "volume" && len(draft.Detaches) != 0 ||
		draft.Cleaned > uint32(len(draft.Detaches)) {
		return errs.New(errs.KindStateConflict, "native preparation draft has invalid custody")
	}
	seen := map[string]bool{}
	for _, id := range draft.Detaches {
		if ids.Validate(ids.KindWorkItem, id) != nil || id == draft.WorkID || seen[id] {
			return errs.New(errs.KindStateConflict, "native preparation draft repeats an invalid work item")
		}
		seen[id] = true
	}
	return nil
}

func (repository *EnvironmentBlueprintRepository) stageNativePreparationDraft(
	ctx context.Context,
	task workexecution.Task,
	kind, workID string,
	detaches []string,
	claim blueprints.EnvironmentBlueprintStageClaim,
	guards []keyvalue.Condition,
) error {
	if task.AcceptedIntent == nil {
		return errs.New(errs.KindStateConflict, "native preparation requires an accepted Task")
	}
	// Existing describes the lookup result, not immutable custody identity.
	claim.Existing = false
	draft := nativePreparationDraft{
		TaskID:   task.ID,
		Intent:   *task.AcceptedIntent,
		Kind:     kind,
		WorkID:   workID,
		Detaches: append([]string(nil), detaches...),
		Claim:    claim,
	}
	if err := draft.validate(); err != nil {
		return err
	}
	raw, err := recordcodec.Encode("native-preparation-draft", draft)
	if err != nil {
		return err
	}
	defer clear(raw)
	if len(raw) > 256<<10 {
		return errs.New(errs.KindValidationFailed, "native preparation draft exceeds its storage bound")
	}
	key := nativePreparationDraftKey(task.ID)
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}})
	if err != nil {
		return err
	}
	if read == nil || len(read.Values) != 1 {
		return errs.New(errs.KindStateConflict, "native preparation draft read is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] != nil {
		if bytes.Equal(read.Values[0].Value, raw) {
			return nil
		}
		return errs.New(errs.KindStateConflict, "native preparation already captured different inputs")
	}
	conditions := append([]keyvalue.Condition(nil), guards...)
	conditions = append(conditions, keyvalue.Condition{Key: key})
	result, err := repository.store.Transact(
		ctx,
		conditions,
		[]keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: key, Value: raw}},
	)
	keyvalue.ClearValues(result.FailureReads)
	if err != nil {
		return err
	}
	if !result.Succeeded {
		return errs.New(errs.KindStateConflict, "native preparation source changed before draft capture")
	}
	return nil
}

func (repository *EnvironmentBlueprintRepository) readNativePreparationDraft(
	ctx context.Context,
	taskID string,
) (nativePreparationDraft, int64, error) {
	read, err := repository.store.GetMany(
		ctx,
		keyvalue.GetManyRequest{Keys: []string{nativePreparationDraftKey(taskID)}},
	)
	if err != nil {
		return nativePreparationDraft{}, 0, err
	}
	if read == nil || len(read.Values) != 1 {
		return nativePreparationDraft{}, 0, errs.New(
			errs.KindStateConflict,
			"native preparation draft read is incomplete",
		)
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil {
		return nativePreparationDraft{}, 0, nil
	}
	draft, err := recordcodec.Decode[nativePreparationDraft](read.Values[0].Value, "native-preparation-draft")
	if err != nil {
		return draft, 0, err
	}
	if err := draft.validate(); err != nil {
		return draft, 0, err
	}
	if draft.TaskID != taskID {
		return draft, 0, errs.New(errs.KindStateConflict, "native preparation draft belongs to another Task")
	}
	return draft, read.Values[0].ModRevision, nil
}

// FinishNativePreparationDraft retires custody only after promotion, or after
// the owner has explicitly abandoned all inert input and desired-state drafts.
func (repository *EnvironmentBlueprintRepository) FinishNativePreparationDraft(
	ctx context.Context,
	taskID string,
	promoted bool,
) (bool, error) {
	queue, err := workqueue.NewRepository(repository.store)
	if err != nil {
		return false, err
	}
	current, _, err := queue.ReadTask(ctx, taskID, 0)
	if err != nil {
		return false, err
	}
	draft, revision, err := repository.readNativePreparationDraft(ctx, taskID)
	if err != nil {
		return false, err
	}
	if revision == 0 {
		return true, nil
	}
	if current.Task.Scope.EnvironmentID != draft.Claim.EnvironmentID ||
		!current.Task.CreatedAt.Equal(draft.Claim.CreatedAt) ||
		current.Task.AcceptedIntent == nil ||
		*current.Task.AcceptedIntent != draft.Intent ||
		promoted != (current.Task.Intent != (workexecution.Input{})) {
		return false, errs.New(errs.KindStateConflict, "native preparation cleanup no longer owns its Task")
	}
	mutations := []keyvalue.Mutation{{Type: keyvalue.MutationDelete, Key: nativePreparationDraftKey(taskID)}}
	if !promoted {
		raw, err := workqueue.EncodeTask(current.Task)
		if err != nil {
			return false, err
		}
		defer clear(raw)
		mutations = append(
			mutations,
			keyvalue.Mutation{Type: keyvalue.MutationPut, Key: workqueue.TaskKey(taskID), Value: raw},
		)
	}
	result, err := queue.Commit(
		ctx,
		workqueue.Change{
			Conditions: []keyvalue.Condition{
				{Key: workqueue.TaskKey(taskID), ModRevision: current.Revision},
				{Key: nativePreparationDraftKey(taskID), ModRevision: revision},
			},
			Mutations: mutations,
		},
	)
	keyvalue.ClearValues(result.FailureReads)
	return result.Succeeded, err
}

// CleanupNativePreparationDraft handles promoted custody and terminal pure Tasks.
// Promoted native input is never removed by this path; normal native retention
// owns it. A returned claim remains protected until its owner finishes cleanup.
func (repository *EnvironmentBlueprintRepository) CleanupNativePreparationDraft(
	ctx context.Context,
	accepted workqueue.TaskSnapshot,
	at time.Time,
) (blueprints.EnvironmentBlueprintStageClaim, bool, error) {
	queue, err := workqueue.NewRepository(repository.store)
	if err != nil {
		return blueprints.EnvironmentBlueprintStageClaim{}, false, err
	}
	current, _, err := queue.ReadTask(ctx, accepted.Task.ID, 0)
	if err != nil {
		return blueprints.EnvironmentBlueprintStageClaim{}, false, err
	}
	draft, revision, err := repository.readNativePreparationDraft(ctx, accepted.Task.ID)
	if err != nil {
		return blueprints.EnvironmentBlueprintStageClaim{}, false, err
	}
	if revision == 0 {
		return blueprints.EnvironmentBlueprintStageClaim{}, true, nil
	}
	if current.Task.Scope.EnvironmentID != draft.Claim.EnvironmentID ||
		!current.Task.CreatedAt.Equal(draft.Claim.CreatedAt) ||
		current.Task.AcceptedIntent == nil ||
		accepted.Task.AcceptedIntent == nil ||
		*current.Task.AcceptedIntent != draft.Intent ||
		*accepted.Task.AcceptedIntent != draft.Intent {
		return blueprints.EnvironmentBlueprintStageClaim{}, false, errs.New(
			errs.KindStateConflict,
			"native draft differs from its cleanup Task",
		)
	}
	if current.Task.Intent != (workexecution.Input{}) {
		done, err := repository.FinishNativePreparationDraft(ctx, current.Task.ID, true)
		return blueprints.EnvironmentBlueprintStageClaim{}, done, err
	}
	if current.Task.FinishedAt == nil || at.Before(*current.Task.FinishedAt) || current.Task.ExpectedItems != 0 {
		return blueprints.EnvironmentBlueprintStageClaim{}, false, nil
	}
	var done bool
	if draft.Kind == "volume" {
		done, err = repository.AbandonVolumeWorkInput(ctx, current, draft.WorkID)
	} else {
		done, err = repository.AbandonNetworkWorkInput(ctx, current, draft.WorkID, draft.Detaches)
	}
	if err != nil || !done {
		return blueprints.EnvironmentBlueprintStageClaim{}, done, err
	}
	return draft.Claim, true, nil
}
