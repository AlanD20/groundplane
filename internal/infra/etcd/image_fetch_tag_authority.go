package etcd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/imagefetch"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	imageTagSelectionPrefix = "/v1/runtime/image-tag-selections/"
	imageTagCommitPrefix    = "/v1/runtime/image-tag-commits/"
	imageTagReceiptPrefix   = "/v1/evidence/image-tag-commits/"
	imageTagSelected        = "selected"
	imageTagCommitting      = "committing"
	imageTagCommitted       = "committed"
)

type imageTagAuthority struct {
	Schema      int       `json:"schema"`
	State       string    `json:"state"`
	Tag         string    `json:"tag"`
	PlanID      string    `json:"plan_id"`
	PlanHash    string    `json:"plan_hash"`
	Image       string    `json:"image"`
	TaskID      string    `json:"task_id,omitempty"`
	SelectedAt  time.Time `json:"selected_at,omitempty"`
	CommittedAt time.Time `json:"committed_at,omitempty"`
}

// CreateImageFetchTask publishes an explicit tag selection in the same
// transaction as its Task and idempotency marker. Transaction order is the
// accepted-selection order. An unresolved Docker tag commit blocks replacement
// until its exact plan proves or abandons the effect.
func (repository *TaskRepository) CreateImageFetchTask(
	ctx context.Context,
	task TaskRecord,
	marker idempotency.IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	authority, tagged, err := imageTagAuthorityForTask(task)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if !tagged {
		return repository.CreateTask(ctx, task, marker)
	}
	value, err := encodeImageTagAuthority(authority)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(value)
	selectionKey, commitKey := imageTagKeys(authority.Tag)
	classifyTask := classifyTaskCreateConflict(task.OperationID)
	classify := func(revision int64, values []*keyvalue.KeyValue) error {
		if len(values) != 5 {
			return errs.New(errs.KindInternal, "image fetch creation evidence is incomplete")
		}
		if values[4] != nil {
			return errs.New(
				errs.KindStateConflict,
				"image tag commit is unresolved; retry its Fetch Task before selecting this tag again",
			)
		}
		return classifyTask(revision, values[:4])
	}
	return repository.createTask(
		ctx,
		task,
		marker,
		[]keyvalue.Condition{{Key: commitKey}},
		[]keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: selectionKey, Value: value}},
		classify,
	)
}

// ImageTagCommitCompleted lets a resumed Task trust its immutable receipt
// without requiring the registry content to remain available after the exact
// Docker tag effect was already verified.
func (repository *TaskRepository) ImageTagCommitCompleted(
	ctx context.Context,
	task TaskRecord,
) (bool, error) {
	authority, tagged, err := imageTagAuthorityForTask(task)
	if err != nil {
		return false, err
	}
	if !tagged || task.Status != taskjournal.TaskStatusRunning {
		return false, errs.New(errs.KindValidationFailed, "image tag receipt requires a running tagged Fetch Task")
	}
	read, err := repository.store.Get(ctx, imageTagReceiptKey(authority.PlanID))
	if err != nil {
		return false, err
	}
	if read == nil {
		return false, errs.New(errs.KindInternal, "image tag receipt read is missing")
	}
	if read.Entry == nil {
		return false, nil
	}
	receipt, err := decodeImageTagAuthority(read.Entry.Value, imageTagCommitted)
	if err != nil {
		return false, err
	}
	if !sameImageTagAuthority(*receipt, authority) {
		return false, corruptImageTagAuthority()
	}
	return true, nil
}

// BeginImageTagCommit returns true when this accepted selection already has a
// durable completion receipt. Otherwise it owns (or resumes) the per-tag fence.
func (repository *TaskRepository) BeginImageTagCommit(
	ctx context.Context,
	task TaskRecord,
) (bool, error) {
	authority, tagged, err := imageTagAuthorityForTask(task)
	if err != nil {
		return false, err
	}
	if !tagged || task.Status != taskjournal.TaskStatusRunning {
		return false, errs.New(errs.KindValidationFailed, "image tag commit requires a running tagged Fetch Task")
	}
	selectionKey, commitKey := imageTagKeys(authority.Tag)
	receiptKey := imageTagReceiptKey(authority.PlanID)
	for range 3 {
		view, err := repository.readImageTagAuthority(ctx, selectionKey, commitKey, receiptKey)
		if err != nil {
			return false, err
		}
		if view.receipt != nil {
			if !sameImageTagAuthority(*view.receipt, authority) {
				return false, corruptImageTagAuthority()
			}
			return true, nil
		}
		if view.selection == nil || !sameImageTagAuthority(*view.selection, authority) {
			return false, errs.New(errs.KindStateConflict, "image tag selection was superseded before commit")
		}
		if view.commit != nil {
			if !sameImageTagAuthority(*view.commit, authority) {
				return false, errs.New(
					errs.KindStateConflict,
					"another image tag commit must finish or be retried first",
				)
			}
			return false, nil
		}
		commit := authority
		commit.State, commit.TaskID, commit.SelectedAt = imageTagCommitting, "", time.Time{}
		value, err := encodeImageTagAuthority(commit)
		if err != nil {
			return false, err
		}
		result, err := repository.store.Transact(
			ctx,
			[]keyvalue.Condition{
				{Key: selectionKey, ModRevision: view.selectionRevision},
				{Key: commitKey},
				{Key: receiptKey},
			},
			[]keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: commitKey, Value: value}},
		)
		clear(value)
		if err != nil {
			return false, err
		}
		if result.Succeeded {
			return false, nil
		}
	}
	return false, errs.New(errs.KindStateConflict, "image tag authority changed while starting commit")
}

// CompleteImageTagCommit stores proof before releasing the acceptance fence.
// A crash or storage failure leaves the fence for the same plan's retry to
// inspect and settle; a newer selection cannot race the Docker write.
func (repository *TaskRepository) CompleteImageTagCommit(
	ctx context.Context,
	task TaskRecord,
	committedAt time.Time,
) error {
	authority, tagged, err := imageTagAuthorityForTask(task)
	if err != nil {
		return err
	}
	if !tagged || task.Status != taskjournal.TaskStatusRunning ||
		recordcodec.ValidateTimestamp("image tag committed_at", committedAt) != nil {
		return errs.New(errs.KindValidationFailed, "image tag completion authority is invalid")
	}
	selectionKey, commitKey := imageTagKeys(authority.Tag)
	receiptKey := imageTagReceiptKey(authority.PlanID)
	for range 3 {
		view, err := repository.readImageTagAuthority(ctx, selectionKey, commitKey, receiptKey)
		if err != nil {
			return err
		}
		if view.receipt != nil {
			if !sameImageTagAuthority(*view.receipt, authority) {
				return corruptImageTagAuthority()
			}
			return nil
		}
		if view.selection == nil || view.commit == nil ||
			!sameImageTagAuthority(*view.selection, authority) || !sameImageTagAuthority(*view.commit, authority) {
			return errs.New(errs.KindStateConflict, "image tag completion lost its durable authority")
		}
		receipt := authority
		receipt.State, receipt.TaskID, receipt.SelectedAt, receipt.CommittedAt =
			imageTagCommitted, task.ID, time.Time{}, committedAt
		value, err := encodeImageTagAuthority(receipt)
		if err != nil {
			return err
		}
		result, err := repository.store.Transact(
			ctx,
			[]keyvalue.Condition{
				{Key: selectionKey, ModRevision: view.selectionRevision},
				{Key: commitKey, ModRevision: view.commitRevision},
				{Key: receiptKey},
			},
			[]keyvalue.Mutation{
				{Type: keyvalue.MutationPut, Key: receiptKey, Value: value},
				{Type: keyvalue.MutationDelete, Key: commitKey},
			},
		)
		clear(value)
		if err != nil {
			return err
		}
		if result.Succeeded {
			return nil
		}
	}
	return errs.New(errs.KindStateConflict, "image tag authority changed while recording completion")
}

// AbandonImageTagCommit releases only an observed-unchanged Docker outcome.
// Unknown outcomes deliberately retain the fence for exact recovery.
func (repository *TaskRepository) AbandonImageTagCommit(ctx context.Context, task TaskRecord) error {
	authority, tagged, err := imageTagAuthorityForTask(task)
	if err != nil {
		return err
	}
	if !tagged || task.Status != taskjournal.TaskStatusRunning {
		return errs.New(errs.KindValidationFailed, "image tag abandonment authority is invalid")
	}
	selectionKey, commitKey := imageTagKeys(authority.Tag)
	receiptKey := imageTagReceiptKey(authority.PlanID)
	for range 3 {
		view, err := repository.readImageTagAuthority(ctx, selectionKey, commitKey, receiptKey)
		if err != nil {
			return err
		}
		if view.receipt != nil {
			return errs.New(errs.KindStateConflict, "completed image tag commit cannot be abandoned")
		}
		if view.commit == nil {
			return nil
		}
		if view.selection == nil || !sameImageTagAuthority(*view.selection, authority) ||
			!sameImageTagAuthority(*view.commit, authority) {
			return errs.New(errs.KindStateConflict, "image tag abandonment lost its durable authority")
		}
		result, err := repository.store.Transact(
			ctx,
			[]keyvalue.Condition{
				{Key: selectionKey, ModRevision: view.selectionRevision},
				{Key: commitKey, ModRevision: view.commitRevision},
				{Key: receiptKey},
			},
			[]keyvalue.Mutation{{Type: keyvalue.MutationDelete, Key: commitKey}},
		)
		if err != nil {
			return err
		}
		if result.Succeeded {
			return nil
		}
	}
	return errs.New(errs.KindStateConflict, "image tag authority changed while abandoning commit")
}

type imageTagAuthorityView struct {
	selection         *imageTagAuthority
	selectionRevision int64
	commit            *imageTagAuthority
	commitRevision    int64
	receipt           *imageTagAuthority
}

func (repository *TaskRepository) readImageTagAuthority(
	ctx context.Context,
	selectionKey, commitKey, receiptKey string,
) (imageTagAuthorityView, error) {
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{
		Keys: []string{selectionKey, commitKey, receiptKey},
	})
	if err != nil {
		return imageTagAuthorityView{}, err
	}
	if read == nil || read.ReadRevision <= 0 || len(read.Values) != 3 {
		return imageTagAuthorityView{}, errs.New(errs.KindInternal, "image tag authority read is incomplete")
	}
	view := imageTagAuthorityView{
		selectionRevision: keyvalue.RevisionOf(read.Values[0]),
		commitRevision:    keyvalue.RevisionOf(read.Values[1]),
	}
	if read.Values[0] != nil {
		view.selection, err = decodeImageTagAuthority(read.Values[0].Value, imageTagSelected)
	}
	if err == nil && read.Values[1] != nil {
		view.commit, err = decodeImageTagAuthority(read.Values[1].Value, imageTagCommitting)
	}
	if err == nil && read.Values[2] != nil {
		view.receipt, err = decodeImageTagAuthority(read.Values[2].Value, imageTagCommitted)
	}
	return view, err
}

func imageTagAuthorityForTask(task TaskRecord) (imageTagAuthority, bool, error) {
	if err := ValidateTaskRecord(task); err != nil {
		return imageTagAuthority{}, false, err
	}
	if task.Type != taskjournal.TaskFetch ||
		task.Params[taskjournal.TaskResourceKindParam] != taskjournal.TaskResourceImage {
		return imageTagAuthority{}, false, errs.New(errs.KindValidationFailed, "Task is not an image Fetch")
	}
	plan, err := imagefetch.Decode(task.Params[taskjournal.TaskImageFetchInputParam], task.PlanHash)
	if err != nil {
		return imageTagAuthority{}, false, err
	}
	tag, err := plan.TagReference()
	if err != nil {
		return imageTagAuthority{}, false, err
	}
	if tag == "" {
		return imageTagAuthority{}, false, nil
	}
	return imageTagAuthority{
		Schema: 1, State: imageTagSelected, Tag: tag, PlanID: task.PlanID, PlanHash: task.PlanHash,
		Image: plan.Reference(), TaskID: task.ID, SelectedAt: task.CreatedAt,
	}, true, nil
}

func imageTagKeys(tag string) (string, string) {
	digest := sha256.Sum256([]byte(tag))
	suffix := hex.EncodeToString(digest[:])
	return imageTagSelectionPrefix + suffix, imageTagCommitPrefix + suffix
}

func imageTagReceiptKey(planID string) string { return imageTagReceiptPrefix + planID }

func encodeImageTagAuthority(authority imageTagAuthority) ([]byte, error) {
	if validateImageTagAuthority(authority, authority.State) != nil {
		return nil, corruptImageTagAuthority()
	}
	value, err := json.Marshal(authority)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	return value, nil
}

func decodeImageTagAuthority(value []byte, state string) (*imageTagAuthority, error) {
	var authority imageTagAuthority
	if len(value) == 0 || json.Unmarshal(value, &authority) != nil ||
		validateImageTagAuthority(authority, state) != nil {
		return nil, corruptImageTagAuthority()
	}
	return &authority, nil
}

func validateImageTagAuthority(authority imageTagAuthority, state string) error {
	hash, hashErr := hex.DecodeString(authority.PlanHash)
	if authority.Schema != 1 || authority.State != state || authority.Tag == "" || authority.Image == "" ||
		recordcodec.ValidateID(ids.KindPlan, authority.PlanID) != nil || hashErr != nil || len(hash) != sha256.Size {
		return corruptImageTagAuthority()
	}
	switch state {
	case imageTagSelected:
		if recordcodec.ValidateID(ids.KindTask, authority.TaskID) != nil || authority.SelectedAt.IsZero() ||
			recordcodec.ValidateTimestamp("image tag selected_at", authority.SelectedAt) != nil ||
			!authority.CommittedAt.IsZero() {
			return corruptImageTagAuthority()
		}
	case imageTagCommitting:
		if authority.TaskID != "" || !authority.SelectedAt.IsZero() || !authority.CommittedAt.IsZero() {
			return corruptImageTagAuthority()
		}
	case imageTagCommitted:
		if recordcodec.ValidateID(ids.KindTask, authority.TaskID) != nil || !authority.SelectedAt.IsZero() ||
			authority.CommittedAt.IsZero() ||
			recordcodec.ValidateTimestamp("image tag committed_at", authority.CommittedAt) != nil {
			return corruptImageTagAuthority()
		}
	default:
		return corruptImageTagAuthority()
	}
	return nil
}

func sameImageTagAuthority(left, right imageTagAuthority) bool {
	return left.Schema == right.Schema && left.Tag == right.Tag && left.PlanID == right.PlanID &&
		left.PlanHash == right.PlanHash && left.Image == right.Image
}

func corruptImageTagAuthority() error {
	return errs.New(errs.KindInternal, "image tag authority is corrupt")
}
