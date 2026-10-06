package desiredauthoring

import (
	"context"
	"github.com/AlanD20/groundplane/internal/core"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
)

func AttachmentMutation(ctx context.Context, store Store, record attachrecord.Record, oldName string, remove bool) func(*core.BlueprintDesiredInput, *environmentprojection.EnvironmentComposeProjection) error {
	return func(input *core.BlueprintDesiredInput, _ *environmentprojection.EnvironmentComposeProjection) error {
		if remove {
			delete(input.Attachments, record.Name)
			return nil
		}
		spec, err := AttachmentSpec(ctx, store, record)
		if err != nil {
			return err
		}
		if oldName != "" {
			RenameAttachment(input, oldName, record.Name, spec)
		} else {
			if input.Attachments == nil {
				input.Attachments = make(map[string]core.AttachmentSpec)
			}
			input.Attachments[record.Name] = spec
		}
		return nil
	}
}
