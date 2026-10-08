package desiredauthoring

import (
	"context"
	"sort"

	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func AttachmentSpec(ctx context.Context, store Store, record attachments.Record) (core.AttachmentSpec, error) {
	consumer, found, err := blueprints.ReadCurrentProjection(ctx, store, record.EnvironmentID, 0)
	if err != nil {
		return core.AttachmentSpec{}, err
	}
	if !found {
		return core.AttachmentSpec{}, errs.New(errs.KindStateConflict, "Attach consumer projection is unavailable")
	}
	backing, found, err := blueprints.ReadCurrentProjection(
		ctx,
		store,
		record.BackingEnvironmentID,
		consumer.ReadRevision,
	)
	if err != nil {
		return core.AttachmentSpec{}, err
	}
	if !found {
		return core.AttachmentSpec{}, errs.New(errs.KindStateConflict, "Attach backing projection is unavailable")
	}
	consumerName, backingName := "", ""
	for _, service := range consumer.Record.DesiredServices {
		if service.Desired.ID == record.ServiceID {
			consumerName = service.Desired.Name
		}
	}
	for _, service := range backing.Record.DesiredServices {
		if service.Desired.ID == record.BackingServiceID {
			backingName = service.Desired.Name
		}
	}
	if consumerName == "" || backingName == "" {
		return core.AttachmentSpec{}, errs.New(errs.KindStateConflict, "Attach Service identity is unavailable")
	}
	keys := []string{hierarchy.ProjectKey(record.BackingProjectID)}
	if record.CredentialAttachID != record.ID {
		keys = append(keys, attachments.AttachKey(record.CredentialAttachID))
	}
	for _, id := range record.GrantAttachIDs {
		keys = append(keys, attachments.AttachKey(id))
	}
	state, err := store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: consumer.ReadRevision})
	if err != nil {
		return core.AttachmentSpec{}, err
	}
	if state == nil || len(state.Values) != len(keys) {
		return core.AttachmentSpec{}, errs.New(errs.KindInternal, "Attach authoring identities are incomplete")
	}
	for _, value := range state.Values {
		if value == nil {
			return core.AttachmentSpec{}, errs.New(errs.KindStateConflict, "Attach authoring identity is unavailable")
		}
	}
	project, err := hierarchy.DecodeProject(state.Values[0].Value)
	if err != nil {
		return core.AttachmentSpec{}, err
	}
	spec := core.AttachmentSpec{BackingProject: project.Slug, BackingService: backingName, Service: consumerName,
		Credential: core.AttachmentCredentialSpec{Mode: "new"}}
	index := 1
	if record.CredentialAttachID != record.ID {
		owner, err := attachments.DecodeAttachRecord(state.Values[index].Value)
		if err != nil {
			return core.AttachmentSpec{}, err
		}
		if !owner.OwnsCredential() || owner.BackingServiceID != record.BackingServiceID {
			return core.AttachmentSpec{}, errs.New(errs.KindStateConflict, "Attach credential authority changed")
		}
		spec.Credential = core.AttachmentCredentialSpec{Mode: "existing", Attach: owner.Name}
		index++
	}
	for ; index < len(state.Values); index++ {
		grant, err := attachments.DecodeAttachRecord(state.Values[index].Value)
		if err != nil {
			return core.AttachmentSpec{}, err
		}
		if !grant.OwnsCredential() || grant.BackingServiceID != record.BackingServiceID {
			return core.AttachmentSpec{}, errs.New(errs.KindStateConflict, "Attach grant authority changed")
		}
		spec.Grants = append(spec.Grants, grant.Name)
	}
	sort.Strings(spec.Grants)
	return spec, nil
}

func RenameAttachment(input *core.BlueprintDesiredInput, oldName, name string, spec core.AttachmentSpec) {
	if input.Attachments == nil {
		input.Attachments = make(map[string]core.AttachmentSpec)
	}
	delete(input.Attachments, oldName)
	input.Attachments[name] = spec
	for key, value := range input.Attachments {
		if value.Credential.Attach == oldName {
			value.Credential.Attach = name
		}
		for index, grant := range value.Grants {
			if grant == oldName {
				value.Grants[index] = name
			}
		}
		input.Attachments[key] = value
	}
	for key, entry := range input.Entries {
		if entry.Source.Fact != nil && entry.Source.Fact.Attach == oldName {
			fact := *entry.Source.Fact
			fact.Attach = name
			entry.Source.Fact = &fact
			input.Entries[key] = entry
		}
	}
	if input.Backup != nil {
		for index, source := range input.Backup.Sources {
			if source.Kind == core.BackupSourceAttach && source.Ref == oldName {
				input.Backup.Sources[index].Ref = name
			}
		}
	}
}
