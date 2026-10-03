package attachments

import (
	"context"

	"github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/core"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	hierarchyattach "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletionattach"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ResolveDeletionIdentity opens the immutable facts captured by the hierarchy
// journal, never a mutable ready Attach or a fabricated ordinary lifecycle Task.
func (service *FactService) ResolveDeletionIdentity(
	ctx context.Context,
	input hierarchyattach.Input,
	consume taskplanning.AttachPlanIdentityConsumer,
) error {
	if ctx == nil || consume == nil || hierarchyattach.Validate(input) != nil ||
		!input.Attach.OwnsCredential() || input.Facts == nil || input.Attach.HookBundle ||
		(input.Attach.Status != core.AttachReady && input.Attach.Status != core.AttachFailed) {
		return errs.New(errs.KindStateConflict, "captured Attach deletion identity is unavailable")
	}
	if attachrecord.ValidateAttachEncryptedFacts(*input.Facts) != nil {
		return errs.New(errs.KindInternal, "captured Attach deletion facts are invalid")
	}
	return service.openStoredBundle(ctx, input.Attach, *input.Facts, func(bundle *attachFactBundle) error {
		return consumeAttachTaskIdentity(bundle, consume)
	})
}

func consumeAttachTaskIdentity(
	bundle *attachFactBundle,
	consume taskplanning.AttachPlanIdentityConsumer,
) error {
	identity := taskplanning.AttachPlanIdentity{
		Authentication: bundle.Identity.Authentication,
		Database:       bundle.Identity.Database, Role: bundle.Identity.Role,
		Password: append([]byte(nil), bundle.Identity.Password...),
		Grants:   make([]taskplanning.AttachPlanGrantIdentity, 0, len(bundle.Identity.Grants)),
	}
	for _, grant := range bundle.Identity.Grants {
		identity.Grants = append(identity.Grants, taskplanning.AttachPlanGrantIdentity{
			AttachID: grant.AttachID, Database: grant.Database,
		})
	}
	defer identity.Clear()
	return consume(identity)
}
