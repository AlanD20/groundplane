package networkwork

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strconv"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/jcs"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

const inputChunkBytes = 128 * 1024

type inputManifest struct {
	TaskID string              `json:"task_id"`
	WorkID string              `json:"work_id"`
	Input  workexecution.Input `json:"input"`
	Length int                 `json:"length"`
	Chunks []string            `json:"chunks"`
}

func InputKey(workID string) string    { return "/v1/network-work-input/" + workID + "/root" }
func InputPrefix(workID string) string { return "/v1/network-work-input/" + workID + "/" }
func inputChunkKey(workID string, ordinal int) string {
	return InputPrefix(workID) + "chunks/" + strconv.Itoa(ordinal)
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

// Stage captures content in bounded immutable chunks. The returned root write
// MUST join publication of the native work item and its desired-state change.
// Chunks alone are not executable input. Guards identify the live admission claim;
// staging cannot continue after its owner has been replaced or published.
func (repository *InputRepository) Stage(ctx context.Context, taskID, workID, inputID string,
	input *agentpb.NetworkWorkInput, guards []keyvalue.Condition,
) (workexecution.Input, workqueue.Change, error) {
	if ids.Validate(ids.KindTask, taskID) != nil || ids.Validate(ids.KindWorkItem, workID) != nil || len(guards) == 0 {
		return workexecution.Input{}, workqueue.Change{}, conflict(
			"Network input requires its captured admission owner",
		)
	}
	_, raw, reference, err := executionplan.CaptureNetworkWorkInput(inputID, input)
	if err != nil {
		return workexecution.Input{}, workqueue.Change{}, err
	}
	defer clear(raw)
	manifest := inputManifest{TaskID: taskID, WorkID: workID, Input: reference, Length: len(raw)}
	publication := workqueue.Change{Conditions: append([]keyvalue.Condition(nil), guards...)}
	publication.Conditions = append(publication.Conditions, keyvalue.Condition{Key: InputKey(workID)})
	for offset, ordinal := 0, 0; offset < len(raw); offset, ordinal = offset+inputChunkBytes, ordinal+1 {
		chunk := raw[offset:min(offset+inputChunkBytes, len(raw))]
		digest := sha256.Sum256(chunk)
		manifest.Chunks = append(manifest.Chunks, hex.EncodeToString(digest[:]))
		key := inputChunkKey(workID, ordinal)
		read, err := repository.read(ctx, []string{key}, 0)
		if err != nil {
			return workexecution.Input{}, workqueue.Change{}, err
		}
		value := read.Values[0]
		var revision int64
		if value != nil {
			matches := bytes.Equal(value.Value, chunk)
			revision = value.ModRevision
			keyvalue.ClearValues(read.Values)
			if !matches {
				return workexecution.Input{}, workqueue.Change{}, conflict(
					"Network input chunk already contains different captured content",
				)
			}
		} else {
			conditions := append([]keyvalue.Condition(nil), guards...)
			conditions = append(conditions, keyvalue.Condition{Key: InputKey(workID)}, keyvalue.Condition{Key: key})
			result, err := repository.transactions.Commit(ctx, workqueue.Change{Conditions: conditions,
				Mutations: []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: key, Value: chunk}}})
			keyvalue.ClearValues(result.FailureReads)
			if err != nil {
				return workexecution.Input{}, workqueue.Change{}, err
			}
			if !result.Succeeded {
				return workexecution.Input{}, workqueue.Change{}, conflict("Network input staging ownership changed")
			}
			revision = result.Revision
		}
		publication.Conditions = append(publication.Conditions, keyvalue.Condition{Key: key, ModRevision: revision})
	}
	encoded, err := json.Marshal(manifest)
	if err != nil {
		return workexecution.Input{}, workqueue.Change{}, err
	}
	canonical, err := jcs.Canonicalize(encoded)
	if err != nil {
		return workexecution.Input{}, workqueue.Change{}, err
	}
	publication.Mutations = []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: InputKey(workID), Value: canonical}}
	return reference, publication, nil
}

func (repository *InputRepository) Load(
	ctx context.Context,
	view workqueue.View,
) (*agentpb.NetworkWorkInput, []keyvalue.Condition, error) {
	item := view.Item.Item
	if item.Operation != workexecution.Network || item.Input == nil || item.TaskID != view.Task.Task.ID ||
		view.ReadRevision <= 0 {
		return nil, nil, conflict("Network work has no captured input")
	}
	key := InputKey(item.ID)
	read, err := repository.read(ctx, []string{key}, view.ReadRevision)
	if err != nil {
		return nil, nil, err
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil || len(read.Values[0].Value) > 4096 {
		return nil, nil, conflict("Network captured input root is unavailable")
	}
	manifest, err := jcs.Decode[inputManifest](read.Values[0].Value)
	if err != nil || manifest.TaskID != item.TaskID || manifest.WorkID != item.ID || manifest.Input != *item.Input ||
		manifest.Length <= 0 || manifest.Length > executionplan.MaximumPlanBytes ||
		len(manifest.Chunks) != (manifest.Length+inputChunkBytes-1)/inputChunkBytes {
		return nil, nil, conflict("Network captured input root is inconsistent")
	}
	keys := make([]string, len(manifest.Chunks))
	for index := range keys {
		keys[index] = inputChunkKey(item.ID, index)
	}
	chunks, err := repository.read(ctx, keys, view.ReadRevision)
	if err != nil {
		return nil, nil, err
	}
	defer keyvalue.ClearValues(chunks.Values)
	raw := make([]byte, 0, manifest.Length)
	defer func() { clear(raw) }()
	conditions := []keyvalue.Condition{{Key: key, ModRevision: read.Values[0].ModRevision}}
	for index, chunk := range chunks.Values {
		if chunk == nil || len(chunk.Value) != min(inputChunkBytes, manifest.Length-index*inputChunkBytes) {
			return nil, nil, conflict("Network captured input chunk is unavailable")
		}
		digest := sha256.Sum256(chunk.Value)
		if hex.EncodeToString(digest[:]) != manifest.Chunks[index] {
			return nil, nil, conflict("Network captured input chunk changed")
		}
		raw = append(raw, chunk.Value...)
		conditions = append(conditions, keyvalue.Condition{Key: keys[index], ModRevision: chunk.ModRevision})
	}
	input := &agentpb.NetworkWorkInput{}
	if err := proto.Unmarshal(raw, input); err != nil {
		return nil, nil, conflict("Network captured input cannot be decoded")
	}
	owned, canonical, reference, err := executionplan.CaptureNetworkWorkInput(manifest.Input.ID, input)
	defer clear(canonical)
	if err != nil {
		return nil, nil, err
	}
	if reference != manifest.Input || !bytes.Equal(raw, canonical) {
		return nil, nil, conflict("Network captured input does not match its sealed digest")
	}
	return owned, conditions, nil
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
		return nil, conflict("Network input read is incomplete")
	}
	for index, value := range read.Values {
		if value != nil &&
			(value.Key != keys[index] || value.ModRevision <= 0 || value.ModRevision > read.ReadRevision) {
			keyvalue.ClearValues(read.Values)
			return nil, conflict("Network input read has inconsistent ownership")
		}
	}
	return read, nil
}

func conflict(message string) error { return errs.New(errs.KindStateConflict, message) }
