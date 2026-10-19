package attachremoval

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strconv"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const detachChunkBytes = 256 << 10

type detachManifest struct {
	TaskID string              `json:"task_id"`
	WorkID string              `json:"work_id"`
	Input  workexecution.Input `json:"input"`
	Length int                 `json:"length"`
	Chunks []string            `json:"chunks"`
}

func DetachInputPrefix(workID string) string { return "/v1/private/detach-work-inputs/" + workID + "/" }
func DetachInputKey(workID string) string    { return DetachInputPrefix(workID) + "root" }
func detachChunkKey(workID string, ordinal int) string {
	return DetachInputPrefix(workID) + "chunks/" + strconv.Itoa(ordinal)
}

type Store interface {
	workqueue.Store
	Get(context.Context, string) (*keyvalue.GetResult, error)
}

type InputRepository struct {
	store        Store
	transactions *workqueue.Repository
}

func NewInputRepository(store Store) (*InputRepository, error) {
	transactions, err := workqueue.NewRepository(store)
	if err != nil {
		return nil, err
	}
	return &InputRepository{store: store, transactions: transactions}, nil
}

// StageDetach writes only inaccessible immutable chunks. Publication MUST join
// the returned root with the work item, captured resource guards and replay.
// A partial staging failure grants no execution and changes no resource state.
func (repository *InputRepository) StageDetach(ctx context.Context, inputID string, input DetachInput,
	guards []keyvalue.Condition,
) (workexecution.Input, workqueue.Change, error) {
	if len(guards) == 0 {
		return workexecution.Input{}, workqueue.Change{}, checkpointConflict(
			"Detach staging requires its admission owner",
		)
	}
	raw, reference, err := captureDetachInput(ctx, inputID, input)
	if err != nil {
		return workexecution.Input{}, workqueue.Change{}, err
	}
	defer clear(raw)
	manifest := detachManifest{TaskID: input.TaskID, WorkID: input.WorkItemID, Input: reference, Length: len(raw)}
	rootKey := DetachInputKey(input.WorkItemID)
	change := workqueue.Change{Conditions: append([]keyvalue.Condition(nil), guards...)}
	change.Conditions = append(change.Conditions, keyvalue.Condition{Key: rootKey})
	for offset, ordinal := 0, 0; offset < len(raw); offset, ordinal = offset+detachChunkBytes, ordinal+1 {
		chunk := raw[offset:min(offset+detachChunkBytes, len(raw))]
		digest := sha256.Sum256(chunk)
		manifest.Chunks = append(manifest.Chunks, hex.EncodeToString(digest[:]))
		key := detachChunkKey(input.WorkItemID, ordinal)
		read, err := repository.read(ctx, []string{key}, 0)
		if err != nil {
			return workexecution.Input{}, workqueue.Change{}, err
		}
		value, revision := read.Values[0], int64(0)
		if value != nil {
			same := bytes.Equal(value.Value, chunk)
			revision = value.ModRevision
			keyvalue.ClearValues(read.Values)
			if !same {
				return workexecution.Input{}, workqueue.Change{}, checkpointConflict(
					"Detach staging contains a different immutable source",
				)
			}
		} else {
			keyvalue.ClearValues(read.Values)
			conditions := append([]keyvalue.Condition(nil), guards...)
			conditions = append(conditions, keyvalue.Condition{Key: rootKey}, keyvalue.Condition{Key: key})
			result, err := repository.transactions.Commit(ctx, workqueue.Change{Conditions: conditions,
				Mutations: []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: key, Value: chunk}}})
			keyvalue.ClearValues(result.FailureReads)
			if err != nil {
				return workexecution.Input{}, workqueue.Change{}, err
			}
			if !result.Succeeded {
				return workexecution.Input{}, workqueue.Change{}, checkpointConflict("Detach staging lost admission ownership")
			}
			revision = result.Revision
		}
		change.Conditions = append(change.Conditions, keyvalue.Condition{Key: key, ModRevision: revision})
	}
	root, err := recordcodec.Encode("detach-work-manifest", manifest)
	if err != nil {
		return workexecution.Input{}, workqueue.Change{}, err
	}
	change.Mutations = []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: rootKey, Value: root}}
	return reference, change, nil
}

// LoadDetach verifies the root and every captured byte at the work view's MVCC
// revision. No current desired state or Task-history input is consulted.
func (repository *InputRepository) LoadDetach(
	ctx context.Context,
	view workqueue.View,
) (DetachInput, []keyvalue.Condition, error) {
	item := view.Item.Item
	if item.Operation != workexecution.Detach || item.Input == nil || item.TaskID != view.Task.Task.ID ||
		view.ReadRevision <= 0 {
		return DetachInput{}, nil, checkpointConflict("Detach work has no captured input")
	}
	key := DetachInputKey(item.ID)
	read, err := repository.read(ctx, []string{key}, view.ReadRevision)
	if err != nil {
		return DetachInput{}, nil, err
	}
	defer keyvalue.ClearValues(read.Values)
	root := read.Values[0]
	if root == nil || len(root.Value) > 16<<10 {
		return DetachInput{}, nil, checkpointConflict("Detach input manifest is unavailable")
	}
	manifest, err := recordcodec.Decode[detachManifest](root.Value, "detach-work-manifest")
	if err != nil || manifest.TaskID != item.TaskID || manifest.WorkID != item.ID || manifest.Input != *item.Input ||
		manifest.Input.Validate() != nil || manifest.Length <= 0 || manifest.Length > maximumDetachBytes ||
		len(manifest.Chunks) != (manifest.Length+detachChunkBytes-1)/detachChunkBytes {
		return DetachInput{}, nil, recordcodec.CorruptRecord()
	}
	keys := make([]string, len(manifest.Chunks))
	for ordinal := range keys {
		keys[ordinal] = detachChunkKey(item.ID, ordinal)
	}
	chunks, err := repository.read(ctx, keys, view.ReadRevision)
	if err != nil {
		return DetachInput{}, nil, err
	}
	defer keyvalue.ClearValues(chunks.Values)
	raw := make([]byte, 0, manifest.Length)
	defer func() { clear(raw) }()
	conditions := []keyvalue.Condition{{Key: key, ModRevision: root.ModRevision}}
	for ordinal, chunk := range chunks.Values {
		if chunk == nil || len(chunk.Value) != min(detachChunkBytes, manifest.Length-ordinal*detachChunkBytes) {
			return DetachInput{}, nil, checkpointConflict("Detach captured input chunk is unavailable")
		}
		digest := sha256.Sum256(chunk.Value)
		if hex.EncodeToString(digest[:]) != manifest.Chunks[ordinal] {
			return DetachInput{}, nil, recordcodec.CorruptRecord()
		}
		raw = append(raw, chunk.Value...)
		conditions = append(conditions, keyvalue.Condition{Key: chunk.Key, ModRevision: chunk.ModRevision})
	}
	digest := sha256.Sum256(raw)
	if hex.EncodeToString(digest[:]) != item.Input.SHA256 {
		return DetachInput{}, nil, recordcodec.CorruptRecord()
	}
	input, err := decodeDetachInput(ctx, raw)
	if err != nil {
		return DetachInput{}, nil, err
	}
	if input.TaskID != item.TaskID || input.WorkItemID != item.ID || input.Snapshot.Source.Attach.ID != item.TargetID ||
		input.Snapshot.Runtime != nil && input.Snapshot.Runtime.Source.Executor.HostID != item.Placement.HostID {
		input.Clear()
		return DetachInput{}, nil, recordcodec.CorruptRecord()
	}
	return input, conditions, nil
}

func (repository *InputRepository) read(
	ctx context.Context,
	keys []string,
	revision int64,
) (*keyvalue.GetManyResult, error) {
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return nil, err
	}
	if read == nil || read.ReadRevision <= 0 || revision > 0 && read.ReadRevision != revision ||
		len(read.Values) != len(keys) {
		if read != nil {
			keyvalue.ClearValues(read.Values)
		}
		return nil, errs.New(errs.KindStateConflict, "Detach captured-input read is incomplete")
	}
	for index, value := range read.Values {
		if value != nil &&
			(value.Key != keys[index] || value.ModRevision <= 0 || value.ModRevision > read.ReadRevision) {
			keyvalue.ClearValues(read.Values)
			return nil, recordcodec.CorruptRecord()
		}
	}
	return read, nil
}
