package controller

import (
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// encodeBackingAuthentication converts durable Controller authority to the
// closed wire enum. Empty is retained only for adapters without selectable
// modes, such as Postgres.
func encodeBackingAuthentication(
	authentication core.BackingAuthentication,
) (agentpb.BackingAuthentication, error) {
	switch authentication {
	case "":
		return agentpb.BackingAuthentication_BACKING_AUTHENTICATION_UNSPECIFIED, nil
	case core.BackingAuthenticationUsernamePassword:
		return agentpb.BackingAuthentication_BACKING_AUTHENTICATION_USERNAME_PASSWORD, nil
	case core.BackingAuthenticationPassword:
		return agentpb.BackingAuthentication_BACKING_AUTHENTICATION_PASSWORD, nil
	case core.BackingAuthenticationNone:
		return agentpb.BackingAuthentication_BACKING_AUTHENTICATION_NONE, nil
	default:
		return 0, errs.New(errs.KindValidationFailed, "backing authentication mode is invalid")
	}
}
