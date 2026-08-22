package controller

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

func (mutator *fakeAttachMutator) ListAttaches(
	context.Context,
	string,
	etcd.PageRequest,
) (etcd.Page[etcd.AttachRecord], error) {
	return etcd.Page[etcd.AttachRecord]{}, nil
}

func (mutator *fakeAttachMutator) RenameAttach(
	_ context.Context,
	attachID string,
	request apiTypes.AttachRenameRequest,
	key string,
) (etcd.IdempotencyResponse, error) {
	mutator.createHit = true
	mutator.request.Name = request.Name
	mutator.attachID = attachID
	mutator.key = key
	return mutator.response, nil
}
