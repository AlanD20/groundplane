package attachments

import (
	"context"

	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	"github.com/AlanD20/groundplane/internal/core"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	attachinputs "github.com/AlanD20/groundplane/internal/infra/etcd/blueprintattachinputs"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ResolveCapturedAttachFact uses only the captured binding and encrypted
// facts captured at admission, never a later record selected by a mutable name.
func (service *FactService) ResolveCapturedAttachFact(
	ctx context.Context, generation attachinputs.Generation, reference core.FactRef,
	destinationSecret bool, consume secretvalue.PlaintextConsumer,
) error {
	if service == nil || ctx == nil || consume == nil || attachinputs.Validate(generation) != nil ||
		(generation.Binding == nil && generation.Provision == nil) || reference.Attach != generation.AttachName || reference.Key == "" {
		return errs.New(errs.KindStateConflict, "Blueprint Attach fact binding is unavailable")
	}
	var credential attachrecord.Record
	var grants []attachinputs.BindingRecord
	if generation.Binding != nil {
		credential, grants = generation.Binding.Credential.Record, generation.Binding.Grants
	} else {
		credential, grants = generation.Provision.Credential.Record, generation.Provision.Grants
	}
	grantID := ""
	if reference.Grant != "" {
		for _, grant := range grants {
			if grant.Record.Name == reference.Grant {
				grantID = grant.Record.ID
				break
			}
		}
		if grantID == "" {
			return errs.New(errs.KindScopeUnauthorized, "Attach fact grant is not captured by this Blueprint")
		}
	}
	definition, declared := attachFactMetadataDefinition(generation.FactSets, grantID, reference.Key)
	if !declared {
		return errs.New(errs.KindValidationFailed, "Attach fact key is not declared by the selected fact set")
	}
	if definition.Secret && !destinationSecret {
		return errs.New(errs.KindValidationFailed, "Secret Attach fact requires a secret Entry destination")
	}
	return service.openStoredBundle(ctx, credential, generation.GeneratedInputs,
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
