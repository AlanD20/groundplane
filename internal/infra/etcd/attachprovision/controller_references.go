package attachprovision

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

func (repository *InputRepository) CaptureControllerReferences(
	ctx context.Context,
	work workqueue.View,
) ([]attachments.BindingReference, []keyvalue.Condition, error) {
	source, err := repository.LoadCaptured(ctx, work)
	if err != nil {
		return nil, nil, err
	}
	defer source.Clear()
	generation := source.Generation.Record
	record, err := generation.ControllerBinding(work.Task.Task.CreatedAt)
	if err != nil {
		return nil, nil, err
	}
	ids := []string{record.CredentialAttachID}
	if generation.Binding != nil {
		for _, grant := range generation.Binding.Grants {
			ids = append(ids, grant.Record.ID)
		}
	} else if generation.Provision != nil {
		for _, grant := range generation.Provision.Grants {
			ids = append(ids, grant.Record.ID)
		}
	}
	references := make([]attachments.BindingReference, 0, len(ids))
	guards := source.Conditions
	for _, id := range ids {
		if id == record.ID && generation.Binding == nil {
			digest, err := attachments.BindingDigest(record)
			if err != nil {
				return nil, nil, err
			}
			references = append(
				references,
				attachments.BindingReference{
					AttachID:      id,
					BindingSHA256: digest,
					FactsSHA256:   generation.GeneratedInputs.CiphertextSHA256,
				},
			)
			continue
		}
		reference, conditions, err := attachments.ReadBindingReference(ctx, repository.store, id, work.ReadRevision)
		if err != nil {
			return nil, nil, err
		}
		references = append(references, reference)
		guards = append(guards, conditions...)
	}
	return references, guards, nil
}
