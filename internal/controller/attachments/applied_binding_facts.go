package attachments

import (
	"context"

	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	"github.com/AlanD20/groundplane/internal/core"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ResolveAppliedAttachFact uses resource-owned facts that survive Task retention.
func (service *FactService) ResolveAppliedAttachFact(
	ctx context.Context, binding attachrecord.AppliedBinding, reference core.FactRef,
	destinationSecret bool, consume secretvalue.PlaintextConsumer,
) error {
	if service == nil || ctx == nil || consume == nil || binding.Validate() != nil ||
		reference.Attach != binding.Consumer.Name || reference.Key == "" || binding.Facts == nil {
		return errs.New(errs.KindStateConflict, "Applied Attach facts are unavailable")
	}
	credential, grants := binding.Credential, binding.Grants
	grantID := ""
	if reference.Grant != "" {
		for _, grant := range grants {
			if grant.Name == reference.Grant {
				grantID = grant.ID
				break
			}
		}
		if grantID == "" {
			return errs.New(errs.KindScopeUnauthorized, "Attach fact grant is not captured by this Blueprint")
		}
	}
	definition, declared := attachFactMetadataDefinition(credential.FactSets, grantID, reference.Key)
	if !declared {
		return errs.New(errs.KindValidationFailed, "Attach fact key is not declared by the selected fact set")
	}
	if definition.Secret && !destinationSecret {
		return errs.New(errs.KindValidationFailed, "Secret Attach fact requires a secret Entry destination")
	}
	return service.openStoredBundle(ctx, credential, *binding.Facts,
		func(bundle *attachFactBundle) error {
			for _, set := range bundle.Sets {
				if set.GrantAttachID != grantID {
					continue
				}
				for _, fact := range set.Facts {
					if fact.Key == reference.Key {
						return consume(fact.Value)
					}
				}
			}
			return errs.New(errs.KindInternal, "Captured Attach fact value is missing")
		})
}
