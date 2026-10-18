package attachments

import (
	"context"
	"slices"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/backingendpoint"
	"github.com/AlanD20/groundplane/internal/common/backinghook"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/attachplan"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintattachinputs"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskconfiguration"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ResolveCapturedProvisionHook opens the generation published with the work.
// It does not reread Secrets, current backing definitions or old Task records.
func (service *FactService) ResolveCapturedProvisionHook(ctx context.Context, generation blueprintattachinputs.Generation,
	scope workexecution.Scope, consume attachplan.HookInputConsumer,
) error {
	if service == nil || ctx == nil || consume == nil || blueprintattachinputs.Validate(generation) != nil ||
		generation.SettlesInController() || generation.Provision != nil || generation.Hook.Attach == nil ||
		scope.EnvironmentID != generation.EnvironmentID || ids.Validate(ids.KindTenant, scope.TenantID) != nil ||
		ids.Validate(ids.KindProject, scope.ProjectID) != nil {
		return errs.New(errs.KindValidationFailed, "captured Attach hook authority is invalid")
	}
	createdAt, err := ids.Timestamp(ids.KindTask, generation.ParentTaskID)
	if err != nil {
		return err
	}
	record, err := attachrecord.NewPendingAttachRecord(generation.AttachID, generation.EnvironmentID,
		generation.AttachName, generation.BackingProjectID, generation.BackingEnvironmentID, generation.BackingServiceID,
		generation.BackingNetworkID, generation.ConsumerServiceID, generation.CredentialOwnerID, nil,
		generation.FactSets, generation.ParentTaskID, createdAt)
	if err != nil {
		return err
	}
	record.HookBundle = true
	return service.openStoredBundle(ctx, record, generation.GeneratedInputs, func(bundle *attachFactBundle) error {
		input := backinghook.Input{Context: backinghook.Context{Event: backinghook.Attach,
			BackingServiceID: generation.BackingServiceID, AttachID: generation.AttachID,
			TenantID: scope.TenantID, ProjectID: scope.ProjectID, EnvironmentID: scope.EnvironmentID,
			ServiceID: generation.ConsumerServiceID}}
		defer clearBackingHookInput(&input)
		input.Values = append(input.Values, backinghook.Value{Key: "HOST", Value: []byte(backingendpoint.New(generation.BackingServiceID))})
		for _, value := range bundle.HookInputs {
			input.Values = append(input.Values, backinghook.Value{Key: value.Key, Value: append([]byte(nil), value.Value...)})
		}
		if generation.ResolvedInputs != nil {
			expected := taskconfiguration.TaskBackingHookInputSet{ProjectID: generation.BackingProjectID,
				CiphertextSHA256: generation.ResolvedInputs.CiphertextSHA256, SecretSources: generation.SecretSources}
			if err := service.openCapturedBackingHookInputs(ctx, generation.OperationID, expected, *generation.ResolvedInputs,
				func(captured *backingHookTaskInputBundle) error {
					for _, value := range captured.Values {
						input.Values = append(input.Values, backinghook.Value{Key: value.Key, Value: append([]byte(nil), value.Value...)})
					}
					return nil
				}); err != nil {
				return err
			}
		}
		slices.SortFunc(input.Values, func(left, right backinghook.Value) int { return strings.Compare(left.Key, right.Key) })
		for _, expected := range generation.Hook.Inputs {
			found := false
			for _, value := range input.Values {
				found = found || value.Key == expected.Key
			}
			if !found {
				return errs.New(errs.KindStateConflict, "captured Attach hook input is missing")
			}
		}
		if err := backinghook.Validate(*generation.Hook.Attach, input, generation.Hook.Facts); err != nil {
			return err
		}
		return consume(input)
	})
}
