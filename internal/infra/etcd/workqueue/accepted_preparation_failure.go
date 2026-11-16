package workqueue

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"time"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type AcceptedPreparationFailure struct {
	TaskID string    `json:"task_id"`
	Code   string    `json:"code"`
	Detail string    `json:"detail"`
	At     time.Time `json:"at"`
}

func acceptedPreparationFailureKey(taskID string) string { return root + "accepted-failures/" + taskID }

func (failure AcceptedPreparationFailure) validate() error {
	if ids.Validate(ids.KindTask, failure.TaskID) != nil || len(failure.Code) == 0 || len(failure.Code) > 128 ||
		len(failure.Detail) == 0 || len(failure.Detail) > 4096 || !utf8.ValidString(failure.Detail) ||
		!recordcodec.IsCanonicalUTC(failure.At) {
		return invalid("accepted Task failure evidence is invalid")
	}
	return nil
}

// PrepareAcceptedPreparationFailure joins sanitized operator evidence with a
// no-work Task rejection. Raw provider errors and private causes never enter it.
func PrepareAcceptedPreparationFailure(
	task TaskSnapshot,
	code, detail string,
	at time.Time,
) (Change, workexecution.Input, error) {
	failure := AcceptedPreparationFailure{TaskID: task.Task.ID, Code: code, Detail: detail, At: at}
	if !task.Task.PendingPreparation() || task.Revision <= 0 || failure.validate() != nil ||
		at.Before(task.Task.UpdatedAt) {
		return Change{}, workexecution.Input{}, invalid("accepted Task failure requires unprepared intent")
	}
	raw, err := json.Marshal(failure)
	if err != nil {
		return Change{}, workexecution.Input{}, errs.Wrap(errs.KindInternal, err)
	}
	digest := sha256.Sum256(raw)
	clear(raw)
	reference := workexecution.Input{
		ID:     ids.DeriveAt(ids.KindPlan, task.Task.CreatedAt, task.Task.ID, "accepted-preparation-failure"),
		SHA256: hex.EncodeToString(digest[:]),
	}
	value, err := encode("execution_accepted_failure", failure)
	if err != nil {
		return Change{}, workexecution.Input{}, err
	}
	key := acceptedPreparationFailureKey(task.Task.ID)
	return Change{
		Conditions: []keyvalue.Condition{{Key: TaskKey(task.Task.ID), ModRevision: task.Revision}, {Key: key}},
		Mutations:  []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: key, Value: value}},
	}, reference, nil
}

func (repository *Repository) ReadAcceptedPreparationFailure(ctx context.Context, taskID string,
	revision int64,
) (*AcceptedPreparationFailure, error) {
	read, err := repository.read(ctx, []string{acceptedPreparationFailureKey(taskID)}, revision)
	if err != nil {
		return nil, err
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil {
		return nil, nil
	}
	failure, err := recordcodec.Decode[AcceptedPreparationFailure](read.Values[0].Value, "execution_accepted_failure")
	if err != nil || failure.validate() != nil || failure.TaskID != taskID {
		return nil, corrupt()
	}
	return &failure, nil
}
