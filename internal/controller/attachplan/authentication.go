package attachplan

import (
	"github.com/AlanD20/groundplane/internal/adapters"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Fixed-authentication adapters do not author a selectable mode, but their
// machine procedures still require an explicit username/password identity.
// Selectable adapters must retain the exact captured operator choice.
func credentialWireAuthentication(
	key string,
	selected core.BackingAuthentication,
) (agentpb.BackingAuthentication, error) {
	adapter, found := adapters.Get(key)
	if !found || adapter.Custom() {
		return 0, errs.New(errs.KindValidationFailed, "credential delivery requires a registered built-in adapter")
	}
	if _, err := core.ResolveBackingAuthentication(adapter.SupportsAuthenticationModes(), selected); err != nil {
		return 0, err
	}
	if !adapter.SupportsAuthenticationModes() {
		return agentpb.BackingAuthentication_BACKING_AUTHENTICATION_USERNAME_PASSWORD, nil
	}
	switch selected {
	case core.BackingAuthenticationUsernamePassword:
		return agentpb.BackingAuthentication_BACKING_AUTHENTICATION_USERNAME_PASSWORD, nil
	case core.BackingAuthenticationPassword:
		return agentpb.BackingAuthentication_BACKING_AUTHENTICATION_PASSWORD, nil
	default:
		return 0, errs.New(errs.KindStateConflict, "no-auth bindings do not have credential procedures")
	}
}
