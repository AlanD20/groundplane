package networkwork

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
)

type cascadeGraphRoot struct {
	TaskID string `json:"task_id"`
	ZoneID string `json:"zone_id"`
	Count  uint32 `json:"count"`
	SHA256 string `json:"sha256"`
}

func CascadeGraphPrefix(taskID string) string {
	return "/v1/private/backing-zone-graphs/" + taskID + "/"
}

func cascadeGraphKey(taskID string) string { return CascadeGraphPrefix(taskID) + "root" }
func cascadeGraphItemKey(taskID string, ordinal int) string {
	return CascadeGraphPrefix(taskID) + "items/" + strconv.Itoa(ordinal)
}

// StageCascadeGraph persists immutable graph records separately from queue
// membership. Its small root must join the accepted Task and resource fences.
// Every write is bounded independently of the number of consumer Attaches.
func (repository *InputRepository) StageCascadeGraph(ctx context.Context, task workexecution.Task,
	graph []workexecution.Item, admission []keyvalue.Condition,
) (workqueue.Change, error) {
	if task.Action != workexecution.ActionRemove || task.Scope.TenantID != "" || task.PublicationComplete ||
		task.FinishedAt != nil || task.Stop != nil || task.CollectionStartedAt != nil || len(admission) == 0 {
		return workqueue.Change{}, conflict("Backing Zone graph requires unpublished removal authority")
	}
	if err := workexecution.ValidateGraph(task, graph); err != nil {
		return workqueue.Change{}, err
	}
	rootKey := cascadeGraphKey(task.ID)
	guards := append([]keyvalue.Condition(nil), admission...)
	guards = append(guards, keyvalue.Condition{Key: rootKey}, keyvalue.Condition{Key: workqueue.TaskKey(task.ID)})
	digest := sha256.New()
	for ordinal, item := range graph {
		if item.AttemptNumber != 0 || item.ActiveAttemptID != "" || item.Result != nil || item.FirstFailure != nil ||
			item.Input == nil || item.Phase != workexecution.Ready || item.ForwardRevoked ||
			item.Cleanup != workexecution.CleanupNone || item.Outcome != "" ||
			ordinal < len(graph)-1 && item.Operation != workexecution.Detach ||
			ordinal == len(graph)-1 && (item.Operation != workexecution.Network || item.TargetID != task.TargetID) {
			return workqueue.Change{}, conflict("Backing Zone graph must contain captured Detaches followed by its bridge")
		}
		raw, err := workqueue.EncodeItem(item)
		if err != nil {
			return workqueue.Change{}, err
		}
		// Hash each record first to make the ordered concatenation unambiguous.
		sum := sha256.Sum256(raw)
		_, _ = digest.Write(sum[:])
		key := cascadeGraphItemKey(task.ID, ordinal)
		read, err := repository.read(ctx, []string{key}, 0)
		if err != nil {
			clear(raw)
			return workqueue.Change{}, err
		}
		if saved := read.Values[0]; saved != nil {
			equal := bytes.Equal(raw, saved.Value)
			clear(raw)
			keyvalue.ClearValues(read.Values)
			if !equal {
				return workqueue.Change{}, conflict("Backing Zone captured graph item already differs")
			}
			continue
		}
		conditions := append([]keyvalue.Condition(nil), guards...)
		conditions = append(conditions, keyvalue.Condition{Key: key})
		result, err := repository.transactions.Commit(ctx, workqueue.Change{Conditions: conditions,
			Mutations: []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: key, Value: raw}}})
		clear(raw)
		keyvalue.ClearValues(read.Values)
		keyvalue.ClearValues(result.FailureReads)
		if err != nil {
			return workqueue.Change{}, err
		}
		if !result.Succeeded {
			return workqueue.Change{}, conflict("Backing Zone admission changed while capturing its graph")
		}
	}
	raw, err := recordcodec.Encode("backing-zone-work-graph", cascadeGraphRoot{TaskID: task.ID, ZoneID: task.TargetID,
		Count: uint32(len(graph)), SHA256: hex.EncodeToString(digest.Sum(nil))})
	if err != nil {
		return workqueue.Change{}, err
	}
	return workqueue.Change{Conditions: guards, Mutations: []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: rootKey, Value: raw}}}, nil
}

func (repository *InputRepository) LoadCascadeGraph(ctx context.Context, task workqueue.TaskSnapshot,
	revision int64,
) ([]workexecution.Item, workqueue.Change, error) {
	if task.Revision <= 0 || revision < task.Revision ||
		task.Task.Action != workexecution.ActionRemove || task.Task.Scope.TenantID != "" {
		return nil, workqueue.Change{}, conflict("Backing Zone publication has no open captured graph")
	}
	key := cascadeGraphKey(task.Task.ID)
	read, err := repository.read(ctx, []string{key}, revision)
	if err != nil {
		return nil, workqueue.Change{}, err
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil {
		return nil, workqueue.Change{}, conflict("Backing Zone captured graph is unavailable")
	}
	root, err := recordcodec.Decode[cascadeGraphRoot](read.Values[0].Value, "backing-zone-work-graph")
	if err != nil || root.TaskID != task.Task.ID || root.ZoneID != task.Task.TargetID || root.Count == 0 ||
		root.Count != task.Task.ExpectedItems || root.Count > workexecution.MaximumTaskItems {
		return nil, workqueue.Change{}, conflict("Backing Zone graph differs from its accepted Task")
	}
	graph := make([]workexecution.Item, 0, int(root.Count))
	digest := sha256.New()
	for offset := 0; offset < int(root.Count); {
		end := min(offset+32, int(root.Count))
		keys := make([]string, 0, end-offset)
		for ordinal := offset; ordinal < end; ordinal++ {
			keys = append(keys, cascadeGraphItemKey(root.TaskID, ordinal))
		}
		batch, err := repository.read(ctx, keys, revision)
		if err != nil {
			return nil, workqueue.Change{}, err
		}
		for _, value := range batch.Values {
			if value == nil {
				keyvalue.ClearValues(batch.Values)
				return nil, workqueue.Change{}, conflict("Backing Zone captured graph is incomplete")
			}
			item, err := workqueue.DecodeItem(value.Value)
			if err != nil {
				keyvalue.ClearValues(batch.Values)
				return nil, workqueue.Change{}, err
			}
			sum := sha256.Sum256(value.Value)
			_, _ = digest.Write(sum[:])
			graph = append(graph, item)
		}
		keyvalue.ClearValues(batch.Values)
		offset = end
	}
	if hex.EncodeToString(digest.Sum(nil)) != root.SHA256 {
		return nil, workqueue.Change{}, conflict("Backing Zone captured graph digest changed")
	}
	if err := workexecution.ValidateGraph(task.Task, graph); err != nil {
		return nil, workqueue.Change{}, err
	}
	return graph, workqueue.Change{Conditions: []keyvalue.Condition{{Key: key, ModRevision: read.Values[0].ModRevision}}}, nil
}
