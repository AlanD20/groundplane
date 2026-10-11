package blueprint

import (
	"context"

	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

func (repository *durableRepository) GetAttach(
	ctx context.Context, attachID string,
) (etcdstore.Versioned[attachrecord.Record], error) {
	return repository.attaches.GetAttach(ctx, attachID)
}

func (repository *durableRepository) GetAuthoredAttachRemovalIntent(
	ctx context.Context,
	taskID string,
) (etcdstore.Versioned[environmentchanges.AttachRemovalIntent], bool, error) {
	return repository.attaches.GetAuthoredAttachRemovalIntent(ctx, taskID)
}

func (repository *durableRepository) GetAttachFactsVersioned(
	ctx context.Context,
	current etcdstore.Versioned[attachrecord.Record],
) (etcdstore.Versioned[attachrecord.EncryptedFacts], bool, error) {
	return repository.attaches.GetAttachFactsVersioned(ctx, current)
}
