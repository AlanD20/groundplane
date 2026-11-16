package taskcredentialrefs

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Finish abandons only a submission that did not become an accepted Task. An
// unknown successful acceptance is protected by the Task comparison, not guessed
// from the submitting request's transport result.
func (repository *Repository) Finish(ctx context.Context, taskID string, applied bool) error {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return err
	}
	if applied {
		return nil
	}
	for range 66 {
		done, progress, err := repository.collectOne(ctx, taskID, time.Now().UTC(), true)
		if err != nil || done || !progress {
			return err
		}
	}
	return errs.New(errs.KindInternal, "pending credential cleanup exceeded its bounded membership")
}

// Collect scans a bounded fair page, independently of history retention. Large
// reference sets retire eight memberships per commit; Tasks retain no plaintext.
func (repository *Repository) Collect(ctx context.Context, after string, at time.Time) (string, error) {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return after, err
	}
	page, err := repository.store.Range(
		ctx,
		keyvalue.RangeRequest{Prefix: rootPrefix, StartExclusive: after, Limit: 16},
	)
	if err != nil {
		return after, err
	}
	if page == nil || page.ReadRevision <= 0 || len(page.Values) > 16 || page.More && len(page.Values) == 0 {
		return after, corrupt()
	}
	defer keyvalue.ClearRangeValues(page.Values)
	next := ""
	if page.More {
		next = page.Values[len(page.Values)-1].Key
	}
	var failures []error
	for _, entry := range page.Values {
		id := strings.TrimPrefix(entry.Key, rootPrefix)
		if _, _, err := repository.collectOne(ctx, id, at, false); err != nil {
			failures = append(failures, err)
		}
	}
	if len(failures) != 0 {
		return next, errs.Wrap(errs.KindInternal, errors.Join(failures...))
	}
	return next, nil
}

// PurgeFinishedForSecret avoids making operators wait for periodic maintenance
// after a Task settles. Active and in-progress submissions still block removal.
func (repository *Repository) PurgeFinishedForSecret(ctx context.Context, secretID string) error {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return err
	}
	for range 16 {
		page, err := repository.store.Range(ctx, keyvalue.RangeRequest{Prefix: SecretPrefix(secretID), Limit: 1})
		if err != nil {
			return err
		}
		if page == nil || page.ReadRevision <= 0 || len(page.Values) > 1 || page.More && len(page.Values) == 0 {
			return corrupt()
		}
		if len(page.Values) == 0 {
			return nil
		}
		id := string(page.Values[0].Value)
		if ids.Validate(ids.KindTask, id) != nil || page.Values[0].Key != SecretPrefix(secretID)+id {
			keyvalue.ClearRangeValues(page.Values)
			return corrupt()
		}
		keyvalue.ClearRangeValues(page.Values)
		done := false
		for range 66 {
			var progress bool
			done, progress, err = repository.collectOne(ctx, id, time.Now().UTC(), false)
			if err != nil {
				return err
			}
			if done || !progress {
				break
			}
		}
		if !done {
			return errs.New(
				errs.KindResourceInUse,
				"This credential is selected by Task "+id+"; finish or cancel that Task before deleting it",
			)
		}
	}
	return errs.New(
		errs.KindResourceInUse,
		"Completed credential references are being collected; retry deletion shortly",
	)
}

func (repository *Repository) collectOne(
	ctx context.Context,
	taskID string,
	at time.Time,
	abandon bool,
) (bool, bool, error) {
	if ids.Validate(ids.KindTask, taskID) != nil {
		return false, false, corrupt()
	}
	keys := []string{Key(taskID), taskKey(taskID)}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys})
	if err != nil {
		return false, false, err
	}
	if read == nil || read.ReadRevision <= 0 || len(read.Values) != 2 {
		return false, false, corrupt()
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil {
		return true, false, nil
	}
	value, err := decode(read.Values[0].Value)
	if err != nil || value.TaskID != taskID || read.Values[0].Key != keys[0] {
		return false, false, corrupt()
	}
	if current := read.Values[1]; current != nil {
		task, err := recordcodec.Decode[workexecution.Task](current.Value, "execution_task")
		if err != nil || task.Validate() != nil || task.ID != taskID || task.AcceptedIntent == nil ||
			*task.AcceptedIntent != value.Intent {
			return false, false, corrupt()
		}
		if task.FinishedAt == nil {
			return false, false, nil
		}
	} else if !value.Collecting && !abandon && at.Before(value.ExpiresAt) {
		return false, false, nil
	}
	value.Collecting = true
	end := min(value.Next+8, value.Published)
	mutations := make([]keyvalue.Mutation, 0, 9)
	for _, secretID := range value.SecretIDs[value.Next:end] {
		mutations = append(
			mutations,
			keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: SecretPrefix(secretID) + taskID},
		)
	}
	value.Next = end
	if value.Next == value.Published {
		mutations = append(mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: keys[0]})
	} else {
		raw, err := encode(value)
		if err != nil {
			return false, false, err
		}
		mutations = append(mutations, keyvalue.Mutation{Type: keyvalue.MutationPut, Key: keys[0], Value: raw})
	}
	defer keyvalue.ClearMutationValues(mutations)
	result, err := repository.store.Transact(ctx, []keyvalue.Condition{
		{Key: keys[0], ModRevision: read.Values[0].ModRevision},
		{Key: keys[1], ModRevision: keyvalue.RevisionOf(read.Values[1])},
	}, mutations)
	keyvalue.ClearValues(result.FailureReads)
	return result.Succeeded && value.Next == value.Published, result.Succeeded, err
}
