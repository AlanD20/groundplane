package attachments

import (
	"context"
	"encoding/hex"
	"slices"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/backingendpoint"
	"github.com/AlanD20/groundplane/internal/common/backinghook"
	"github.com/AlanD20/groundplane/internal/controller/attachplan"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backingprovisionwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskconfiguration"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (service *FactService) ResolveCapturedCreationHook(ctx context.Context, captured backingprovisionwork.Input,
	stored *taskconfiguration.BackingHookEncryptedInputs, consume attachplan.HookInputConsumer,
) error {
	if service == nil || ctx == nil || consume == nil {
		return errs.New(errs.KindInternal, "captured backing creation hook resolver is unavailable")
	}
	procedure, err := captured.Procedure()
	if err != nil {
		return err
	}
	if procedure.AfterStart == nil || (procedure.AfterStart.EncryptedInputs == nil) != (stored == nil) {
		return errs.New(errs.KindStateConflict, "captured backing creation hook inputs are unavailable")
	}
	input := backinghook.Input{
		Context: backinghook.Context{Event: backinghook.AfterStart, BackingServiceID: procedure.ServiceId},
		Values:  []backinghook.Value{{Key: "HOST", Value: []byte(backingendpoint.New(procedure.ServiceId))}},
	}
	defer clearBackingHookInput(&input)
	if stored != nil {
		expected := taskconfiguration.TaskBackingHookInputSet{ProjectID: procedure.ProjectId,
			CiphertextSHA256: hex.EncodeToString(
				procedure.AfterStart.EncryptedInputs.Sha256,
			), SecretSources: captured.Hook.Secrets}
		if err := service.openCapturedBackingHookInputs(ctx, procedure.OperationId, expected, *stored,
			func(bundle *backingHookTaskInputBundle) error {
				for _, value := range bundle.Values {
					input.Values = append(input.Values, backinghook.Value{Key: value.Key, Value: slices.Clone(value.Value)})
				}
				return nil
			}); err != nil {
			return err
		}
	}
	slices.SortFunc(input.Values, func(a, b backinghook.Value) int { return strings.Compare(a.Key, b.Key) })
	definition := backinghook.Definition{
		Command:        slices.Clone(procedure.AfterStart.Command),
		TimeoutSeconds: procedure.AfterStart.TimeoutSeconds,
	}
	if err := backinghook.Validate(definition, input, nil); err != nil {
		return err
	}
	return consume(input)
}
