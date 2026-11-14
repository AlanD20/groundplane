package desiredauthoring

import (
	"context"
	"github.com/AlanD20/groundplane/internal/core"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
)

// PrepareAttachment changes connection declarations without requiring rendered
// runtime. Pending first-deployment inputs remain pending, never synthesized.
func PrepareAttachment(
	ctx context.Context,
	store Store,
	record attachrecord.Record,
	oldName string,
	remove bool,
	marker idempotency.IdempotencyMarker,
) (Publication, error) {
	snapshot, err := loadSnapshot(ctx, store, record.EnvironmentID)
	if err != nil {
		return Publication{}, err
	}
	input := &snapshot.input.Input
	if remove {
		delete(input.Attachments, record.Name)
	} else {
		spec, err := attachmentSpec(ctx, store, record, snapshot.readRevision)
		if err != nil {
			return Publication{}, err
		}
		if oldName != "" {
			RenameAttachment(input, oldName, record.Name, spec)
		} else {
			if input.Attachments == nil {
				input.Attachments = make(map[string]core.AttachmentSpec)
			}
			input.Attachments[record.Name] = spec
		}
	}
	return snapshot.stage(ctx, store, marker)
}
