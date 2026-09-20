package executionplan

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func validateAdapterProcedure(operation agentpb.PlanOperation, procedure *agentpb.AdapterProcedure) error {
	if procedure == nil || !validAdapterKey(procedure.AdapterKey) ||
		validateID(ids.KindAttach, procedure.AttachId) != nil ||
		(validateID(ids.KindService, procedure.BackingServiceId) != nil &&
			validateID(ids.KindComponent, procedure.BackingServiceId) != nil) ||
		!validAdapterIdentity(procedure.Role, true) ||
		!validAdapterSecret(procedure.Password) {
		return errs.New(errs.KindValidationFailed, "adapter procedure identity is invalid")
	}
	switch procedure.Authentication {
	case agentpb.BackingAuthentication_BACKING_AUTHENTICATION_UNSPECIFIED,
		agentpb.BackingAuthentication_BACKING_AUTHENTICATION_USERNAME_PASSWORD:
		if procedure.Role == "default" {
			return errs.New(errs.KindValidationFailed, "adapter procedure authentication identity is invalid")
		}
	case agentpb.BackingAuthentication_BACKING_AUTHENTICATION_PASSWORD:
		if procedure.Role != "default" {
			return errs.New(errs.KindValidationFailed, "adapter procedure authentication identity is invalid")
		}
	case agentpb.BackingAuthentication_BACKING_AUTHENTICATION_NONE:
		return errs.New(errs.KindValidationFailed, "no-auth mode cannot carry an adapter procedure")
	default:
		return errs.New(errs.KindValidationFailed, "adapter procedure authentication mode is unsupported")
	}
	switch procedure.Phase {
	case agentpb.AdapterProcedurePhase_ADAPTER_PROCEDURE_PHASE_PROVISION:
		if operation != agentpb.PlanOperation_PLAN_OPERATION_ATTACH &&
			operation != agentpb.PlanOperation_PLAN_OPERATION_RECONCILE &&
			operation != agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY || len(procedure.Password) == 0 ||
			!validAdapterIdentity(procedure.Database, false) || procedure.GrantOn != "" {
			return errs.New(errs.KindValidationFailed, "adapter provision procedure is invalid")
		}
	case agentpb.AdapterProcedurePhase_ADAPTER_PROCEDURE_PHASE_GRANT:
		if operation != agentpb.PlanOperation_PLAN_OPERATION_ATTACH &&
			operation != agentpb.PlanOperation_PLAN_OPERATION_RECONCILE &&
			operation != agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY || len(procedure.Password) != 0 ||
			procedure.Database != "" || !validAdapterIdentity(procedure.GrantOn, true) {
			return errs.New(errs.KindValidationFailed, "adapter grant procedure is invalid")
		}
	case agentpb.AdapterProcedurePhase_ADAPTER_PROCEDURE_PHASE_REVOKE:
		if operation != agentpb.PlanOperation_PLAN_OPERATION_DETACH || len(procedure.Password) != 0 ||
			procedure.Database != "" || !validAdapterIdentity(procedure.GrantOn, true) {
			return errs.New(errs.KindValidationFailed, "adapter revoke procedure is invalid")
		}
	case agentpb.AdapterProcedurePhase_ADAPTER_PROCEDURE_PHASE_DETACH:
		passwordRequired := procedure.Authentication ==
			agentpb.BackingAuthentication_BACKING_AUTHENTICATION_PASSWORD
		if operation != agentpb.PlanOperation_PLAN_OPERATION_DETACH ||
			(passwordRequired != (len(procedure.Password) != 0)) ||
			!validAdapterIdentity(procedure.Database, false) || procedure.GrantOn != "" {
			return errs.New(errs.KindValidationFailed, "adapter detach procedure is invalid")
		}
	default:
		return errs.New(errs.KindValidationFailed, "adapter procedure phase is unsupported")
	}
	return nil
}

func validAdapterKey(value string) bool {
	if len(value) == 0 || len(value) > MaximumAdapterKeyBytes {
		return false
	}
	for index, character := range []byte(value) {
		if (character >= 'a' && character <= 'z') || (character >= '0' && character <= '9') ||
			(index > 0 && (character == '-' || character == '_' || character == '.' || character == ':')) {
			continue
		}
		return false
	}
	return true
}

func validAdapterSecret(value []byte) bool {
	if len(value) > MaximumAdapterSecretBytes {
		return false
	}
	for _, character := range value {
		if (character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z') ||
			(character >= '0' && character <= '9') || character == '-' || character == '_' || character == '.' {
			continue
		}
		return false
	}
	return true
}

func validAdapterIdentity(value string, required bool) bool {
	if value == "" {
		return !required
	}
	if len(value) > maximumAdapterIdentityBytes || !isASCIIAlphaNumeric(value[0]) {
		return false
	}
	for _, character := range []byte(value[1:]) {
		if isASCIIAlphaNumeric(character) || character == '_' || character == '-' || character == '.' {
			continue
		}
		return false
	}
	return true
}

func isASCIIAlphaNumeric(character byte) bool {
	return character >= 'a' && character <= 'z' ||
		character >= 'A' && character <= 'Z' ||
		character >= '0' && character <= '9'
}
