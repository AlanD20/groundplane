package handlers

import (
	"context"

	testattachments "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	testidempotencyowner "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

func (mutator *fakeAttachMutator) ListAttaches(
	context.Context, string, testkeyvalue.PageRequest,

) (testkeyvalue.Page[testattachments.Record], error) {
	return testkeyvalue.Page[testattachments.Record]{}, nil
}

func (mutator *fakeAttachMutator) RenameAttach(
	_ context.Context,
	attachID string,
	request apiTypes.AttachRenameRequest,
	key string,
) (testidempotencyowner.IdempotencyResponse, error) {
	mutator.createHit = true
	mutator.request.Name = request.Name
	mutator.attachID = attachID
	mutator.key = key
	return mutator.response, nil
}
