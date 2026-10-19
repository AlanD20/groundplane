package attachments

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/backinghook"
	"github.com/AlanD20/groundplane/internal/common/ids"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintattachinputs"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// SealCapturedProvisionFacts updates the captured encrypted bundle, not the
// mutable binding or any newer Secret value. The caller owns atomic publication.
func (service *FactService) SealCapturedProvisionFacts(
	ctx context.Context,
	generation blueprintattachinputs.Generation,
	output backinghook.Output,
) (attachrecord.EncryptedFacts, error) {
	if ctx == nil || blueprintattachinputs.Validate(generation) != nil || generation.Provision != nil ||
		generation.SettlesInController() || generation.Hook.Attach == nil {
		return attachrecord.EncryptedFacts{}, errs.New(
			errs.KindStateConflict,
			"captured Attach hook result owner is unavailable",
		)
	}
	createdAt, err := ids.Timestamp(ids.KindTask, generation.ParentTaskID)
	if err != nil {
		return attachrecord.EncryptedFacts{}, err
	}
	record, err := generation.PendingRecord(generation.ParentTaskID, createdAt)
	if err != nil {
		return attachrecord.EncryptedFacts{}, err
	}
	schema := generation.Hook.Facts
	if err := backinghook.ValidateOutput(schema, output); err != nil {
		return attachrecord.EncryptedFacts{}, err
	}
	var sealed attachrecord.EncryptedFacts
	err = service.openStoredBundle(ctx, record, generation.GeneratedInputs, func(bundle *attachFactBundle) error {
		if len(schema) == 0 {
			if len(bundle.Sets) != 0 || len(record.FactSets) != 0 {
				return errs.New(errs.KindInternal, "Backing hook empty fact metadata is corrupt")
			}
			var sealErr error
			sealed, sealErr = service.sealHookBundle(ctx, record.ID, *bundle)
			return sealErr
		}
		if len(bundle.Sets) != 1 || bundle.Sets[0].GrantAttachID != "" || len(record.FactSets) != 1 ||
			len(record.FactSets[0].Facts) != len(schema) {
			return errs.New(errs.KindInternal, "Backing hook fact metadata is corrupt")
		}
		values := make([]attachFactValue, len(output.Facts))
		for index, fact := range output.Facts {
			definition := record.FactSets[0].Facts[index]
			if definition.Key != schema[index].Key || definition.Secret != schema[index].Secret ||
				fact.Key != schema[index].Key || fact.Secret != schema[index].Secret {
				return errs.New(errs.KindStateConflict, "Backing hook fact schema changed")
			}
			values[index] = attachFactValue{Key: fact.Key, Value: append([]byte(nil), fact.Value...)}
		}
		for index := range bundle.Sets[0].Facts {
			clearAttachBytes(bundle.Sets[0].Facts[index].Value)
		}
		bundle.Sets[0].Facts = values
		var sealErr error
		sealed, sealErr = service.sealHookBundle(ctx, record.ID, *bundle)
		return sealErr
	})
	return sealed, err
}
