package workqueue

import (
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

// PrepareStagedGraph accepts the complete captured membership while storing a
// bounded initial subset atomically with its Task. The caller publishes the
// immutable graph authority in this same transaction. Dispatch remains closed
// until PrepareGraphBatch has stored every exact member and opens the barrier.
func PrepareStagedGraph(task workexecution.Task, graph, initial []workexecution.Item,
	marker idempotency.IdempotencyMarker,
) (Change, error) {
	if len(initial) == 0 || len(initial) > 8 || len(initial) >= len(graph) {
		return Change{}, invalid("staged graph acceptance requires a bounded incomplete initial subset")
	}
	if err := workexecution.ValidateGraph(task, graph); err != nil {
		return Change{}, err
	}
	change, err := PrepareTask(task, marker)
	if err != nil {
		return Change{}, err
	}
	keep := false
	defer func() {
		if !keep {
			keyvalue.ZeroMutationBytes(change.Mutations)
		}
	}()
	for _, item := range initial {
		found := false
		for _, captured := range graph {
			if captured.ID != item.ID {
				continue
			}
			matches, err := samePublishedItem(captured, item)
			if err != nil {
				return Change{}, err
			}
			found = matches
			break
		}
		if !found {
			return Change{}, invalid("initial work differs from the captured graph")
		}
		part, err := prepareItemCreation(item)
		if err != nil {
			return Change{}, err
		}
		change.Conditions = append(change.Conditions, part.Conditions...)
		change.Mutations = append(change.Mutations, part.Mutations...)
	}
	next, err := workexecution.CountPublication(task, initial, task.CreatedAt)
	if err != nil {
		return Change{}, err
	}
	value, err := EncodeTask(next)
	if err != nil {
		return Change{}, err
	}
	for index := range change.Mutations {
		if change.Mutations[index].Key == TaskKey(task.ID) {
			clear(change.Mutations[index].Value)
			change.Mutations[index].Value = value
			keep = true
			return change, nil
		}
	}
	clear(value)
	return Change{}, invalid("staged graph acceptance lost its owning Task publication")
}
