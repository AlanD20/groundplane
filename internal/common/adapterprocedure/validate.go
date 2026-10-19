// Package adapterprocedure validates the closed backing-adapter machine
// contract independently of execution plans, catalogs and storage.
package adapterprocedure

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

const (
	MaximumKeyBytes      = 64
	MaximumSecretBytes   = 256
	MaximumIdentityBytes = 63
)

func Validate(procedure *agentpb.AdapterProcedure) error {
	if procedure == nil || len(procedure.ProtoReflect().GetUnknown()) != 0 || !validKey(procedure.AdapterKey) ||
		ids.Validate(ids.KindAttach, procedure.AttachId) != nil ||
		ids.Validate(ids.KindService, procedure.BackingServiceId) != nil &&
			ids.Validate(ids.KindComponent, procedure.BackingServiceId) != nil ||
		!validIdentity(procedure.Role, true) || !validSecret(procedure.Password) {
		return invalid("adapter procedure identity is invalid")
	}
	switch procedure.Authentication {
	case agentpb.BackingAuthentication_BACKING_AUTHENTICATION_USERNAME_PASSWORD:
		if procedure.Role == "default" {
			return invalid("adapter username/password authentication requires a named role")
		}
	case agentpb.BackingAuthentication_BACKING_AUTHENTICATION_PASSWORD:
		if procedure.Role != "default" {
			return invalid("password-only adapter authentication requires the default role")
		}
	default:
		return invalid("adapter procedure requires an explicit credential authentication mode")
	}
	switch procedure.Phase {
	case agentpb.AdapterProcedurePhase_ADAPTER_PROCEDURE_PHASE_PROVISION:
		if len(procedure.Password) == 0 || !validIdentity(procedure.Database, false) || procedure.GrantOn != "" {
			return invalid("adapter provision procedure is invalid")
		}
	case agentpb.AdapterProcedurePhase_ADAPTER_PROCEDURE_PHASE_GRANT,
		agentpb.AdapterProcedurePhase_ADAPTER_PROCEDURE_PHASE_REVOKE:
		if len(procedure.Password) != 0 || procedure.Database != "" || !validIdentity(procedure.GrantOn, true) {
			return invalid("adapter grant or revoke procedure is invalid")
		}
	case agentpb.AdapterProcedurePhase_ADAPTER_PROCEDURE_PHASE_DETACH:
		passwordRequired := procedure.Authentication == agentpb.BackingAuthentication_BACKING_AUTHENTICATION_PASSWORD
		if passwordRequired != (len(procedure.Password) != 0) || !validIdentity(procedure.Database, false) ||
			procedure.GrantOn != "" {
			return invalid("adapter detach procedure is invalid")
		}
	default:
		return invalid("adapter procedure phase is unsupported")
	}
	return nil
}

func validKey(value string) bool {
	if len(value) == 0 || len(value) > MaximumKeyBytes {
		return false
	}
	for index, character := range []byte(value) {
		if character >= 'a' && character <= 'z' || character >= '0' && character <= '9' ||
			index > 0 && (character == '-' || character == '_' || character == '.' || character == ':') {
			continue
		}
		return false
	}
	return true
}

func validSecret(value []byte) bool {
	if len(value) > MaximumSecretBytes {
		return false
	}
	for _, character := range value {
		if alphaNumeric(character) || character == '-' || character == '_' || character == '.' {
			continue
		}
		return false
	}
	return true
}

func validIdentity(value string, required bool) bool {
	if value == "" {
		return !required
	}
	if len(value) > MaximumIdentityBytes || !alphaNumeric(value[0]) {
		return false
	}
	for _, character := range []byte(value[1:]) {
		if alphaNumeric(character) || character == '_' || character == '-' || character == '.' {
			continue
		}
		return false
	}
	return true
}

func alphaNumeric(character byte) bool {
	return character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' ||
		character >= '0' && character <= '9'
}

func invalid(detail string) error { return errs.New(errs.KindValidationFailed, detail) }
