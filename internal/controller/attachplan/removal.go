package attachplan

import (
	"context"

	"github.com/AlanD20/groundplane/internal/adapters"
	"github.com/AlanD20/groundplane/internal/common/attachexecution"
	"github.com/AlanD20/groundplane/internal/common/backinghook"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/attachremoval"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskconfiguration"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// RemovalFacts is the Controller's encrypted-fact and exact Secret-capture
// boundary. Planning has no dependency on the public Attach mutation service.
type RemovalFacts interface {
	SealBackingHookInputsAtRevision(context.Context, string, string, backinghook.Configuration, int64) (*taskconfiguration.BackingHookEncryptedInputs, error)
	ResolveCapturedDetachHook(context.Context, attachremoval.Input, HookInputConsumer) error
	ResolveDeletionIdentity(context.Context, attachremoval.Input, IdentityConsumer) error
}

type RemovalPlanner struct{ facts RemovalFacts }

func NewRemovalPlanner(facts RemovalFacts) (*RemovalPlanner, error) {
	if facts == nil {
		return nil, errs.New(errs.KindInternal, "Attach removal requires the captured fact resolver")
	}
	return &RemovalPlanner{facts: facts}, nil
}

// CaptureHookInputs seals literals and exact Secret versions at the removal's
// fixed snapshot. The publication owner must pin those sources atomically with
// the ciphertext before any assignment is eligible.
func (planner *RemovalPlanner) CaptureHookInputs(ctx context.Context, operationID string,
	source attachremoval.Input,
) (*taskconfiguration.BackingHookEncryptedInputs, error) {
	if attachremoval.Validate(source) != nil || source.Backing.Desired.Adapter != "custom" || source.Backing.Desired.Hooks == nil {
		return nil, errs.New(errs.KindValidationFailed, "Custom Attach removal capture is invalid")
	}
	return planner.facts.SealBackingHookInputsAtRevision(ctx, operationID, source.Attach.BackingProjectID,
		*source.Backing.Desired.Hooks, source.SnapshotRevision)
}

// Build resolves only the immutable encrypted snapshot. It returns a typed
// credential/hook operation without generic steps or a manufactured Task.
func (planner *RemovalPlanner) Build(ctx context.Context, source attachremoval.Input,
	reference *agentpb.WorkInputReference, consumer *agentpb.AttachConsumerTransition,
) (_ *agentpb.AttachRemovalInput, resultErr error) {
	if ctx == nil || attachremoval.Validate(source) != nil ||
		!attachremoval.NeedsAgent(source) && consumer == nil || reference == nil {
		return nil, errs.New(errs.KindValidationFailed, "captured Attach removal input is unavailable")
	}
	input := &agentpb.AttachRemovalInput{CapturedSource: proto.CloneOf(reference), AttachId: source.Attach.ID,
		BackingServiceId: source.Attach.BackingServiceID, TenantId: source.TenantID, ProjectId: source.ProjectID,
		EnvironmentId: source.Attach.EnvironmentID, ServiceId: source.Attach.ServiceID, Consumer: proto.CloneOf(consumer)}
	defer func() {
		if resultErr != nil {
			attachexecution.ClearRemoval(input)
		}
	}()
	if !attachremoval.NeedsAgent(source) {
		return input, attachexecution.ValidateRemoval(input)
	}
	adapter, found := adapters.Get(source.Backing.Desired.Adapter)
	if !found {
		return nil, errs.New(errs.KindValidationFailed, "captured Attach adapter is not registered")
	}
	if adapter.Custom() {
		err := planner.facts.ResolveCapturedDetachHook(ctx, source, func(values backinghook.Input) error {
			definition := source.Backing.Desired.Hooks.Detach
			hook := &agentpb.BackingHookProcedure{Event: agentpb.BackingHookEvent_BACKING_HOOK_EVENT_DETACH,
				Command: append([]string(nil), definition.Command...), TimeoutSeconds: definition.TimeoutSeconds,
				BackingServiceId: input.BackingServiceId, AttachId: input.AttachId, TenantId: input.TenantId,
				ProjectId: input.ProjectId, EnvironmentId: input.EnvironmentId, ServiceId: input.ServiceId}
			for _, value := range values.Values {
				hook.Inputs = append(hook.Inputs, &agentpb.BackingHookValue{Key: value.Key, Value: append([]byte(nil), value.Value...)})
			}
			for _, value := range values.Facts {
				hook.Facts = append(hook.Facts, &agentpb.BackingHookValue{Key: value.Key, Value: append([]byte(nil), value.Value...)})
			}
			input.Cleanup = &agentpb.AttachRemovalInput_CustomDetach{CustomDetach: hook}
			return attachexecution.ValidateRemoval(input)
		})
		if err != nil {
			return nil, err
		}
		return input, nil
	}
	authentication, err := core.ResolveBackingAuthentication(adapter.SupportsAuthenticationModes(), source.Backing.Desired.Authentication)
	if err != nil || authentication != source.Backing.Desired.Authentication || authentication == core.BackingAuthenticationNone {
		return nil, errs.New(errs.KindStateConflict, "captured Attach removal authentication is invalid")
	}
	err = planner.facts.ResolveDeletionIdentity(ctx, source, func(identity Identity) error {
		if identity.Authentication != authentication || len(identity.Grants) != len(source.Attach.GrantAttachIDs) {
			return errs.New(errs.KindStateConflict, "captured Attach credential identity changed")
		}
		credential := &agentpb.AttachCredentialRemoval{AdapterKey: adapter.Key(), Role: identity.Role, Database: identity.Database}
		switch authentication {
		case core.BackingAuthenticationUsernamePassword:
			credential.Authentication = agentpb.BackingAuthentication_BACKING_AUTHENTICATION_USERNAME_PASSWORD
		case core.BackingAuthenticationPassword:
			credential.Authentication = agentpb.BackingAuthentication_BACKING_AUTHENTICATION_PASSWORD
			credential.Password = append([]byte(nil), identity.Password...)
		default:
			return errs.New(errs.KindValidationFailed, "Attach removal requires explicit credential authentication")
		}
		input.Cleanup = &agentpb.AttachRemovalInput_Credential{Credential: credential}
		for index, grant := range identity.Grants {
			if grant.AttachID != source.Attach.GrantAttachIDs[index] {
				return errs.New(errs.KindStateConflict, "captured Attach grant membership changed")
			}
			credential.Grants = append(credential.Grants, &agentpb.AttachGrantRemoval{AttachId: grant.AttachID, Database: grant.Database})
		}
		return attachexecution.ValidateRemoval(input)
	})
	if err != nil {
		return nil, err
	}
	return input, nil
}
