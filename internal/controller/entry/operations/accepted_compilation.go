package operations

import (
	"bytes"
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/acceptedaction"
	"github.com/AlanD20/groundplane/internal/controller/taskdependencies"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"time"
)

func (service *entryDesiredMutationService) AcceptedPreparations() []taskdependencies.Registration {
	result := make([]taskdependencies.Registration, 0, 3)
	for _, action := range []workexecution.Action{workexecution.ActionCreate, workexecution.ActionUpdate, workexecution.ActionRemove} {
		result = append(
			result,
			acceptedaction.Registration(
				service.accepted,
				"entry-mutation",
				action,
				ids.KindEnvEntry,
				service.prepareAcceptedEntry,
			),
		)
	}
	return result
}

func (service *entryDesiredMutationService) prepareAcceptedEntry(ctx context.Context, accepted workqueue.TaskSnapshot,
	_ int64, at time.Time, input acceptedEntryMutation,
) (taskdependencies.Prepared, error) {
	defer clear(input.Intent.Ciphertext)
	if accepted.Task.Scope.EnvironmentID != input.EnvironmentID || accepted.Task.TargetID != input.EntryID {
		return taskdependencies.Prepared{}, errs.New(errs.KindStateConflict, "Entry ownership changed after acceptance")
	}
	request := entryDesiredMutationRequest{accepted: accepted, at: at, action: input.Action,
		environmentID: input.EnvironmentID, entryID: input.EntryID, locator: input.Locator,
		evidence: entryDesiredEvidence{
			durable: input.Intent,
			matchesStaged: func(_ context.Context, existing idempotency.ProtectedIntentRecord) (bool, error) {
				return existing.EnvelopeVersion == input.Intent.EnvelopeVersion &&
					existing.Cipher == input.Intent.Cipher &&
					existing.DigestAlgorithm == input.Intent.DigestAlgorithm &&
					existing.CiphertextDigest == input.Intent.CiphertextDigest &&
					bytes.Equal(existing.Ciphertext, input.Intent.Ciphertext), nil
			},
		}}
	switch input.Action {
	case entryDesiredMutationCreate:
		if input.Create == nil || input.Edit != nil {
			return taskdependencies.Prepared{}, errs.New(errs.KindInternal, "Accepted Entry creation is incomplete")
		}
		desired, err := prepareEntryCreation(*input.Create)
		if err != nil {
			return taskdependencies.Prepared{}, err
		}
		desired.ID = input.EntryID
		request.desired = desired
	case entryDesiredMutationEdit, entryDesiredMutationRemove:
		environmentID, current, err := service.resolveCurrentEntry(ctx, input.EntryID)
		if err != nil {
			return taskdependencies.Prepared{}, err
		}
		if environmentID != input.EnvironmentID {
			return taskdependencies.Prepared{}, errs.New(errs.KindStateConflict, "Entry moved after acceptance")
		}
		request.desired = current.Entry
		if input.Action == entryDesiredMutationEdit {
			if input.Edit == nil || input.Create != nil {
				return taskdependencies.Prepared{}, errs.New(errs.KindInternal, "Accepted Entry edit is incomplete")
			}
			edited, err := prepareEntryEditInput(*input.Edit)
			if err != nil {
				return taskdependencies.Prepared{}, err
			}
			request.desired.Source, request.desired.Exposure = edited.Source, edited.Exposure
		}
	default:
		return taskdependencies.Prepared{}, errs.New(errs.KindInternal, "Accepted Entry action is unsupported")
	}
	if input.Action != entryDesiredMutationRemove {
		exposure, err := service.work.ResolveEntryMutationExposure(ctx, input.EnvironmentID, input.Bindings)
		if err != nil {
			return taskdependencies.Prepared{}, err
		}
		request.desired.Exposure = exposure
		if input.Bindings.SecretID != "" {
			request.desired.Source.SecretRef = input.Bindings.SecretID
		}
	}
	return service.prepareEntryMutation(ctx, request)
}
