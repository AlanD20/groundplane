package etcd

import (
	"context"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/desiredauthoring"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
)

func prepareAttachDesiredAuthoring(ctx context.Context, store hierarchyStore, record attachrecord.Record,
	oldName string, remove bool, marker idempotencyrecord.IdempotencyMarker,
) (desiredMutationFragment, error) {
	publication, err := desiredauthoring.PrepareAttachment(ctx, store, record, oldName, remove, marker)
	if err != nil {
		return desiredMutationFragment{}, err
	}
	result := desiredMutationFragment{conditions: publication.Conditions, mutations: publication.Mutations}
	for _, mutation := range publication.Mutations {
		result.values = append(result.values, mutation.Value)
	}
	return result, nil
}
