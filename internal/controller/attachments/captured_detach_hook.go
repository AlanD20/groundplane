package attachments

import (
	"context"
	"slices"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/backingendpoint"
	"github.com/AlanD20/groundplane/internal/common/backinghook"
	"github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/infra/etcd/attachremoval"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ResolveCapturedDetachHook opens only retained Attach facts and an operation's
// encrypted input capture. Missing capture is an error; it never reads latest
// Secrets or manufactures a TaskRecord to resolve a native operation.
func (service *FactService) ResolveCapturedDetachHook(ctx context.Context, source attachremoval.Input,
	consume taskplanning.BackingHookInputConsumer,
) error {
	if ctx == nil || consume == nil || attachremoval.Validate(source) != nil || !source.Attach.HookBundle ||
		!source.Attach.OwnsCredential() || source.Facts == nil || source.Backing.Desired.Hooks == nil || source.Backing.Desired.Hooks.Detach == nil {
		return errs.New(errs.KindStateConflict, "captured Custom Detach inputs are unavailable")
	}
	for _, definition := range source.Backing.Desired.Hooks.Inputs {
		if definition.Generate == "" && (source.HookInputSet == nil || source.HookInputs == nil) {
			return errs.New(errs.KindStateConflict, "Custom Detach literal and Secret inputs have not been captured")
		}
	}
	return service.openStoredBundle(ctx, source.Attach, *source.Facts, func(bundle *attachFactBundle) error {
		input := backinghook.Input{Context: backinghook.Context{Event: backinghook.Detach,
			BackingServiceID: source.Attach.BackingServiceID, AttachID: source.Attach.ID,
			TenantID: source.TenantID, ProjectID: source.ProjectID, EnvironmentID: source.Attach.EnvironmentID, ServiceID: source.Attach.ServiceID}}
		defer clearBackingHookInput(&input)
		input.Values = append(input.Values, backinghook.Value{Key: "HOST", Value: []byte(backingendpoint.New(source.Attach.BackingServiceID))})
		for _, value := range bundle.HookInputs {
			input.Values = append(input.Values, backinghook.Value{Key: value.Key, Value: append([]byte(nil), value.Value...)})
		}
		if source.HookInputSet != nil {
			if source.HookInputs == nil {
				return errs.New(errs.KindStateConflict, "Custom Detach captured ciphertext is missing")
			}
			if err := service.openCapturedBackingHookInputs(ctx, source.HookOperationID, *source.HookInputSet, *source.HookInputs,
				func(captured *backingHookTaskInputBundle) error {
					for _, value := range captured.Values {
						input.Values = append(input.Values, backinghook.Value{Key: value.Key, Value: append([]byte(nil), value.Value...)})
					}
					return nil
				}); err != nil {
				return err
			}
		}
		if len(bundle.Sets) > 1 {
			return errs.New(errs.KindStateConflict, "Custom Detach has an invalid captured fact set")
		}
		if len(bundle.Sets) == 1 {
			for _, value := range bundle.Sets[0].Facts {
				input.Facts = append(input.Facts, backinghook.Value{Key: value.Key, Value: append([]byte(nil), value.Value...)})
			}
		}
		slices.SortFunc(input.Values, func(left, right backinghook.Value) int { return strings.Compare(left.Key, right.Key) })
		return consume(input)
	})
}
