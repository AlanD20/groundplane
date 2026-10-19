package attachments

import (
	"context"
	"slices"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/backingendpoint"
	"github.com/AlanD20/groundplane/internal/common/backinghook"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/attachplan"
	"github.com/AlanD20/groundplane/internal/infra/etcd/servicelifecyclework"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskconfiguration"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ResolveCapturedLifecycleHook opens the native operation's fixed envelope.
// It neither constructs an old Task record nor resolves latest Secret values.
func (service *FactService) ResolveCapturedLifecycleHook(ctx context.Context, captured servicelifecyclework.Input,
	stored *taskconfiguration.BackingHookEncryptedInputs, consume attachplan.HookInputConsumer,
) error {
	if service == nil || ctx == nil || consume == nil || captured.Validate() != nil || captured.Runtime == nil ||
		captured.Runtime.HookConfiguration == nil || (captured.HookInputs == nil) != (stored == nil) {
		return errs.New(errs.KindStateConflict, "captured Service lifecycle hook inputs are unavailable")
	}
	configuration := captured.Runtime.HookConfiguration
	event, definition := backinghook.BeforeStop, configuration.BeforeStop
	if captured.Action == workexecution.ActionStart {
		event, definition = backinghook.AfterStart, configuration.AfterStart
	}
	input := backinghook.Input{Context: backinghook.Context{Event: event, BackingServiceID: captured.ServiceID}}
	defer clearBackingHookInput(&input)
	input.Values = append(
		input.Values,
		backinghook.Value{Key: "HOST", Value: []byte(backingendpoint.New(captured.ServiceID))},
	)
	if stored != nil {
		expected := taskconfiguration.TaskBackingHookInputSet{ProjectID: captured.Scope.ProjectID,
			CiphertextSHA256: captured.HookInputs.CiphertextSHA256, SecretSources: captured.HookInputs.SecretSources}
		if err := service.openCapturedBackingHookInputs(ctx, captured.HookInputs.OperationID, expected, *stored,
			func(bundle *backingHookTaskInputBundle) error {
				for _, value := range bundle.Values {
					input.Values = append(input.Values, backinghook.Value{Key: value.Key, Value: append([]byte(nil), value.Value...)})
				}
				return nil
			}); err != nil {
			return err
		}
	}
	slices.SortFunc(input.Values, func(a, b backinghook.Value) int { return strings.Compare(a.Key, b.Key) })
	for _, expected := range configuration.Inputs {
		found := false
		for _, value := range input.Values {
			found = found || value.Key == expected.Key
		}
		if !found {
			return errs.New(errs.KindStateConflict, "captured Service lifecycle hook input is missing")
		}
	}
	if err := backinghook.Validate(*definition, input, nil); err != nil {
		return err
	}
	return consume(input)
}
