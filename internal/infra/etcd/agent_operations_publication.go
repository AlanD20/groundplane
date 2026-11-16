package etcd

import (
	"context"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentoperations"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// PrepareAgentWorkAuthority captures the selected Agent lifecycle input after
// prerequisite Tasks have settled. The coordinator joins this fragment to the
// original accepted Task promotion, without a second Task or replay marker.
func (repository *TaskRepository) PrepareAgentWorkAuthority(
	ctx context.Context,
	capture agentoperations.Capture,
) (workqueue.Change, error) {
	_, item, err := capture.Work()
	if err != nil {
		return workqueue.Change{}, err
	}
	input, err := agentoperations.EncodeCapture(capture)
	if err != nil {
		return workqueue.Change{}, err
	}
	fence, err := repository.readSoftwareImageFence(ctx)
	if err != nil {
		clear(input)
		return workqueue.Change{}, err
	}
	return workqueue.Change{Conditions: []keyvalue.Condition{{Key: agentoperations.InputKey(item.ID)}, fence},
		Mutations: []keyvalue.Mutation{
			{Type: keyvalue.MutationPut, Key: agentoperations.InputKey(item.ID), Value: input},
		}}, nil
}
