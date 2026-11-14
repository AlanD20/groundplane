package runtimeprovenance

import (
	"slices"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// A metadata-only intervening prerequisite need not rewrite runtime. Accept the
// last real writer anywhere in the captured dependency chain, never an arbitrary
// successful sibling or a different Apply.
func CapturedPrerequisite(view workqueue.View, predecessor string, graph []workexecution.Item) (bool, error) {
	if len(graph) == 0 || len(graph) != int(view.Task.Task.ExpectedItems) {
		return false, errs.New(errs.KindStateConflict, "Runtime predecessor requires the complete captured graph")
	}
	byID := make(map[string][]string, len(graph))
	matched := false
	for _, item := range graph {
		if item.TaskID != view.Task.Task.ID {
			return false, errs.New(errs.KindStateConflict, "Runtime dependency belongs to a different Apply")
		}
		if _, duplicate := byID[item.ID]; duplicate {
			return false, errs.New(errs.KindStateConflict, "Runtime dependency graph repeats a work identity")
		}
		byID[item.ID] = item.Dependencies
		if item.ID == view.Item.Item.ID {
			matched = slices.Equal(item.Dependencies, view.Item.Item.Dependencies)
		}
	}
	if !matched {
		return false, errs.New(errs.KindStateConflict, "Runtime dependency graph differs from captured work")
	}
	stack := slices.Clone(view.Item.Item.Dependencies)
	visited := make(map[string]bool, len(graph))
	for len(stack) != 0 {
		id := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if visited[id] {
			continue
		}
		visited[id] = true
		dependencies, found := byID[id]
		if !found {
			return false, errs.New(errs.KindStateConflict, "Runtime dependency is absent from the captured graph")
		}
		if id == predecessor {
			return true, nil
		}
		stack = append(stack, dependencies...)
	}
	return false, nil
}
