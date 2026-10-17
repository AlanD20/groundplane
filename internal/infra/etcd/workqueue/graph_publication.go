package workqueue

import (
	"bytes"
	"context"
	"slices"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PrepareGraphBatch stages at most eight captured work items, or opens the
// publication barrier after all exact items exist. It never commits: callers
// must join the immutable operation-plan comparison and input publication.
// Rebuilding graph from current desired state is not a valid caller contract.
// The Task revision fences membership and every pre-dispatch change.
func (repository *Repository) PrepareGraphBatch(ctx context.Context, task TaskSnapshot,
	graph []workexecution.Item, at time.Time,
) (Change, bool, error) {
	if task.Revision <= 0 || task.Task.PublicationComplete || task.Task.FinishedAt != nil || at.Before(task.Task.UpdatedAt) {
		return Change{}, false, errs.New(errs.KindStateConflict, "task graph publication is no longer open")
	}
	if err := workexecution.ValidateGraph(task.Task, graph); err != nil {
		return Change{}, false, err
	}
	expected := make(map[string]workexecution.Item, len(graph))
	for _, item := range graph {
		if item.AttemptNumber != 0 || item.ActiveAttemptID != "" || item.Result != nil || item.FirstFailure != nil ||
			item.Cleanup != workexecution.CleanupNone || item.Outcome != "" ||
			item.Phase != workexecution.Ready && item.Phase != workexecution.Waiting {
			return Change{}, false, invalid("captured graph contains executed work")
		}
		expected[item.ID] = item
	}
	staged := uint64(0)
	after := ""
	for {
		page, err := repository.ListWork(ctx, task.Task.ID, after, 100, task.Revision)
		if err != nil {
			return Change{}, false, err
		}
		for _, snapshot := range page.Items {
			item, exists := expected[snapshot.Item.ID]
			if !exists {
				return Change{}, false, corrupt()
			}
			matches, err := samePublishedItem(item, snapshot.Item)
			if err != nil {
				return Change{}, false, err
			}
			if !matches {
				return Change{}, false, errs.New(errs.KindStateConflict, "staged work differs from the captured graph")
			}
			delete(expected, item.ID)
			staged++
		}
		if !page.More {
			break
		}
		if len(page.Items) == 0 || page.LastID <= after {
			return Change{}, false, corrupt()
		}
		after = page.LastID
	}
	if staged != task.Task.Progress.Total() {
		return Change{}, false, corrupt()
	}
	if len(expected) == 0 {
		change, err := PreparePublicationComplete(task, graph, at)
		return change, true, err
	}
	remaining := make([]workexecution.Item, 0, len(expected))
	for _, item := range expected {
		remaining = append(remaining, item)
	}
	slices.SortFunc(remaining, func(left, right workexecution.Item) int { return strings.Compare(left.ID, right.ID) })
	change, err := PrepareItems(task, remaining[:min(8, len(remaining))], at)
	return change, false, err
}

func samePublishedItem(expected, actual workexecution.Item) (bool, error) {
	left, err := EncodeItem(expected)
	if err != nil {
		return false, err
	}
	defer clear(left)
	right, err := EncodeItem(actual)
	if err != nil {
		return false, err
	}
	defer clear(right)
	return bytes.Equal(left, right), nil
}
