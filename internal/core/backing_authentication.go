package core

import "github.com/AlanD20/groundplane/pkg/errs"

// BackingAuthentication is immutable backing-instance policy, not an Attach
// choice. Empty authoring input selects the named-credential Valkey default.
type BackingAuthentication string

const (
	BackingAuthenticationUsernamePassword BackingAuthentication = "username_password"
	BackingAuthenticationPassword         BackingAuthentication = "password"
	BackingAuthenticationNone             BackingAuthentication = "none"
)

func ResolveBackingAuthentication(supported bool, authentication BackingAuthentication) (BackingAuthentication, error) {
	if !supported {
		if authentication != "" {
			return "", errs.New(errs.KindValidationFailed, "adapter does not support authentication selection")
		}
		return "", nil
	}
	switch authentication {
	case "", BackingAuthenticationUsernamePassword:
		return BackingAuthenticationUsernamePassword, nil
	case BackingAuthenticationPassword, BackingAuthenticationNone:
		return authentication, nil
	default:
		return "", errs.New(errs.KindValidationFailed, "unsupported backing authentication mode")
	}
}
