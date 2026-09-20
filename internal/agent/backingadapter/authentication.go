package backingadapter

import (
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// decodeBackingAuthentication validates the closed wire enum against the
// selected adapter before any Agent effect is compiled.
func decodeBackingAuthentication(
	authentication agentpb.BackingAuthentication,
	supported bool,
) (core.BackingAuthentication, error) {
	if authentication == agentpb.BackingAuthentication_BACKING_AUTHENTICATION_UNSPECIFIED {
		if supported {
			return "", errs.New(errs.KindValidationFailed, "adapter authentication mode is required")
		}
		return core.ResolveBackingAuthentication(false, "")
	}
	var mode core.BackingAuthentication
	switch authentication {
	case agentpb.BackingAuthentication_BACKING_AUTHENTICATION_USERNAME_PASSWORD:
		mode = core.BackingAuthenticationUsernamePassword
	case agentpb.BackingAuthentication_BACKING_AUTHENTICATION_PASSWORD:
		mode = core.BackingAuthenticationPassword
	case agentpb.BackingAuthentication_BACKING_AUTHENTICATION_NONE:
		mode = core.BackingAuthenticationNone
	default:
		return "", errs.New(errs.KindValidationFailed, "adapter authentication mode is unsupported")
	}
	return core.ResolveBackingAuthentication(supported, mode)
}
