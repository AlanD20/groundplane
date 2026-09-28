package imagedelivery

import (
	"context"
	"encoding/json"

	"github.com/AlanD20/groundplane/internal/common/imagefence"
	"github.com/AlanD20/groundplane/internal/controller/workloadseal"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type ImageStore interface {
	Get(context.Context, string) (*keyvalue.GetResult, error)
	Range(context.Context, keyvalue.RangeRequest) (*keyvalue.RangeResult, error)
	Transact(context.Context, []keyvalue.Condition, []keyvalue.Mutation) (keyvalue.TransactionResult, error)
}

// SelectionResolver fences the complete producer, not just its Agent lookup.
// Even an uncertain/cancelled publication cannot commit after removal starts:
// etcd compares the captured revision in the publication transaction itself.
type SelectionResolver struct {
	Images workloadseal.Resolver
	Store  ImageStore
}

func (resolver SelectionResolver) ResolveWorkloadImages(
	ctx context.Context,
	agentID string,
	selectors []*agentpb.WorkloadImageSelector,
) (*agentpb.WorkloadImageResolutionResult, error) {
	fence, revision, err := readFence(ctx, resolver.Store)
	if err != nil {
		return nil, err
	}
	if fence.Operation != "" {
		return nil, errs.New(errs.KindResourceInUse, "host image removal is in progress")
	}
	if err := imagefence.Capture(ctx, revision); err != nil {
		return nil, err
	}
	return resolver.Images.ResolveWorkloadImages(ctx, agentID, selectors)
}

type removalFence struct {
	Operation string `json:"operation"`
}

func readFence(ctx context.Context, store ImageStore) (removalFence, int64, error) {
	read, err := store.Get(ctx, imagefence.Key)
	if err != nil {
		return removalFence{}, 0, err
	}
	if read == nil {
		return removalFence{}, 0, errs.New(errs.KindInternal, "image fence read is missing")
	}
	if read.Entry == nil {
		return removalFence{}, 0, nil
	}
	var fence removalFence
	if err := json.Unmarshal(read.Entry.Value, &fence); err != nil {
		return fence, 0, errs.New(errs.KindInternal, "image removal fence is corrupt")
	}
	return fence, read.Entry.ModRevision, nil
}

func acquireRemoval(ctx context.Context, store ImageStore, operationID string) error {
	fence, revision, err := readFence(ctx, store)
	if err != nil {
		return err
	}
	if fence.Operation == operationID {
		return nil
	}
	if fence.Operation != "" {
		return errs.New(errs.KindResourceInUse, "another image removal must finish or be retried first")
	}
	value, err := json.Marshal(removalFence{Operation: operationID})
	if err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	result, err := store.Transact(
		ctx,
		[]keyvalue.Condition{{Key: imagefence.Key, ModRevision: revision}},
		[]keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: imagefence.Key, Value: value}},
	)
	if err != nil {
		return err
	}
	if !result.Succeeded {
		return errs.New(errs.KindResourceInUse, "image removal fence changed; retry the Task")
	}
	return nil
}

func releaseRemoval(ctx context.Context, store ImageStore, operationID string) error {
	fence, revision, err := readFence(ctx, store)
	if err != nil {
		return err
	}
	if fence.Operation == "" {
		return nil
	}
	if fence.Operation != operationID {
		return errs.New(errs.KindResourceInUse, "image removal belongs to another operation")
	}
	result, err := store.Transact(
		ctx,
		[]keyvalue.Condition{{Key: imagefence.Key, ModRevision: revision}},
		[]keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: imagefence.Key, Value: []byte(`{"operation":""}`)}},
	)
	if err != nil {
		return err
	}
	if !result.Succeeded {
		return errs.New(errs.KindResourceInUse, "image removal fence changed before completion")
	}
	return nil
}
