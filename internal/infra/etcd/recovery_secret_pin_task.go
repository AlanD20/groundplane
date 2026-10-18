package etcd

import (
	"github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpinrecord"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpins"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func addRecoverySecretPin(
	operationID string,
	pin tasksecretpinrecord.Record,
	selected map[string]tasksecretpinrecord.Record,
) error {
	if tasksecretpinrecord.Validate(pin) != nil || pin.OperationID != operationID {
		return errs.New(errs.KindValidationFailed, "recovery Secret pin is invalid")
	}
	if previous, found := selected[pin.SecretID]; found && previous != pin {
		return errs.New(errs.KindStateConflict, "recovery requires conflicting versions of one Secret")
	}
	selected[pin.SecretID] = pin
	if len(selected) > tasksecretpins.MaximumPins {
		return errs.New(errs.KindValidationFailed, "recovery Secret pin set exceeds its bound")
	}
	return nil
}

func collectRecoverySecretPins(
	operationID string, files []taskmaterialization.Record, selected map[string]tasksecretpinrecord.Record,
) error {
	for _, file := range files {
		if file.Source.GeneratedEnvironment == nil {
			continue
		}
		for _, value := range file.Source.GeneratedEnvironment.Values {
			if value.Secret == nil {
				continue
			}
			source := value.Secret
			pin := tasksecretpinrecord.Record{OperationID: operationID, SecretID: source.SecretID,
				MetadataRevision: source.Revision, CiphertextSHA256: source.CiphertextSHA256}
			if err := addRecoverySecretPin(operationID, pin, selected); err != nil {
				return err
			}
		}
	}
	return nil
}

// A rejected or uncertain publication may discard preparation only after
// proving its Task did not publish. Cleanup uses a separate bounded context.
