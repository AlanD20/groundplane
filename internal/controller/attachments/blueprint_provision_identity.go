package attachments

import (
	"context"

	"github.com/AlanD20/groundplane/internal/controller/taskplanning"
	attachinputs "github.com/AlanD20/groundplane/internal/infra/etcd/blueprintattachinputs"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ResolveBlueprintProvisionIdentity opens the identity sealed at admission.
// Plan reconstruction cannot select new credentials from current backing state.
func (service *FactService) ResolveBlueprintProvisionIdentity(
	ctx context.Context, generation attachinputs.Generation, consume taskplanning.AttachPlanIdentityConsumer,
) error {
	if service == nil || ctx == nil || consume == nil || attachinputs.Validate(generation) != nil ||
		generation.Provision == nil || generation.CredentialOwnerID != generation.AttachID {
		return errs.New(errs.KindValidationFailed, "Blueprint Attach provisioning identity is invalid")
	}
	return service.openStoredBundle(ctx, generation.Provision.Credential.Record, generation.GeneratedInputs,
		func(bundle *attachFactBundle) error { return consumeAttachTaskIdentity(bundle, consume) })
}
