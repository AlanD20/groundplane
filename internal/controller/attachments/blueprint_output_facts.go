package attachments

import (
	"context"

	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	"github.com/AlanD20/groundplane/internal/core"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	attachinputs "github.com/AlanD20/groundplane/internal/infra/etcd/blueprintattachinputs"
	attachoutputs "github.com/AlanD20/groundplane/internal/infra/etcd/blueprintattachoutputs"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ResolveBlueprintOutputFact opens only the exact child-owned encrypted hook
// result. Neither the current Attach record nor its mutable facts key is read.
func (service *FactService) ResolveBlueprintOutputFact(
	ctx context.Context,
	input attachinputs.Generation,
	output attachoutputs.Generation,
	reference core.FactRef,
	destinationSecret bool,
	consume secretvalue.PlaintextConsumer,
) error {
	if service == nil || ctx == nil || consume == nil ||
		attachinputs.Validate(input) != nil || attachoutputs.Validate(output) != nil ||
		input.OwnerKind != attachinputs.OwnerChild || input.Transfer == nil ||
		input.Transfer.ParentTaskID != input.ParentTaskID ||
		input.Transfer.ChildTaskID != input.OwnerTaskID ||
		output.EnvironmentID != input.EnvironmentID ||
		output.ParentTaskID != input.ParentTaskID ||
		output.ChildTaskID != input.OwnerTaskID ||
		output.AttachID != input.AttachID || output.OperationID != input.OperationID ||
		reference.Attach != input.AttachName || reference.Grant != "" || reference.Key == "" {
		return errs.New(errs.KindStateConflict, "Blueprint Attach output fact authority changed")
	}
	definition, declared := attachFactMetadataDefinition(input.FactSets, "", reference.Key)
	if !declared {
		return errs.New(errs.KindValidationFailed, "Blueprint Attach output fact is not declared")
	}
	if definition.Secret && !destinationSecret {
		return errs.New(errs.KindValidationFailed, "Secret Attach fact requires a secret Entry destination")
	}
	record, err := attachrecord.NewPendingAttachRecord(
		input.AttachID, input.EnvironmentID, input.AttachName,
		input.BackingProjectID, input.BackingEnvironmentID,
		input.BackingServiceID, input.BackingNetworkID,
		input.ConsumerServiceID, input.CredentialOwnerID,
		nil, input.FactSets, output.ChildTaskID, output.CreatedAt,
	)
	if err != nil {
		return err
	}
	record.HookBundle = true
	return service.openStoredBundle(ctx, record, output.Facts, func(bundle *attachFactBundle) error {
		for _, set := range bundle.Sets {
			if set.GrantAttachID != "" {
				continue
			}
			for _, fact := range set.Facts {
				if fact.Key == reference.Key {
					return consume(fact.Value)
				}
			}
		}
		return errs.New(errs.KindInternal, "Blueprint Attach output fact value is missing")
	})
}
