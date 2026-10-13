package attachments

import (
	"context"
	"slices"
	"strconv"

	"github.com/AlanD20/groundplane/internal/adapters"
	"github.com/AlanD20/groundplane/internal/common/backingaccess"
	"github.com/AlanD20/groundplane/internal/core"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ResolveStoredAccessCredentials opens one captured ready fact bundle. Admission
// supplies same-revision consumer/owner/grant records and seals the private
// request inside consume. This never rereads mutable facts during execution.
func (service *FactService) ResolveStoredAccessCredentials(
	ctx context.Context,
	consumer, owner keyvalue.Versioned[attachrecord.Record],
	grant *keyvalue.Versioned[attachrecord.Record],
	stored attachrecord.EncryptedFacts,
	adapter adapters.Adapter,
	consume func(backingaccess.Request) error,
) error {
	if ctx == nil || consume == nil || adapter == nil || adapter.Custom() ||
		attachrecord.ValidateAttachVersion(consumer) != nil || attachrecord.ValidateAttachVersion(owner) != nil ||
		consumer.ReadRevision <= 0 || owner.ReadRevision != consumer.ReadRevision ||
		consumer.Record.Status != core.AttachReady || owner.Record.Status != core.AttachReady ||
		!owner.Record.OwnsCredential() || consumer.Record.CredentialAttachID != owner.Record.ID ||
		consumer.Record.EnvironmentID != owner.Record.EnvironmentID ||
		consumer.Record.BackingServiceID != owner.Record.BackingServiceID ||
		consumer.Record.BackingNetworkID != owner.Record.BackingNetworkID ||
		stored.AttachID != owner.Record.ID || owner.Record.HookBundle {
		return errs.New(
			errs.KindStateConflict,
			"Backing access check requires a ready consumer and its captured credential owner",
		)
	}
	grantID := ""
	if grant != nil {
		if attachrecord.ValidateAttachVersion(*grant) != nil || grant.ReadRevision != consumer.ReadRevision ||
			grant.Record.Status != core.AttachReady || !grant.Record.OwnsCredential() ||
			grant.Record.EnvironmentID != consumer.Record.EnvironmentID ||
			grant.Record.BackingServiceID != owner.Record.BackingServiceID ||
			!slices.Contains(owner.Record.GrantAttachIDs, grant.Record.ID) {
			return errs.New(
				errs.KindScopeUnauthorized,
				"Backing access check grant is not owned by the captured credential",
			)
		}
		grantID = grant.Record.ID
	}
	return service.openStoredBundle(ctx, owner.Record, stored, func(bundle *attachFactBundle) error {
		request := backingaccess.Request{
			AttachID: consumer.Record.ID, CredentialAttachID: owner.Record.ID,
			EnvironmentID: consumer.Record.EnvironmentID, ServiceID: consumer.Record.ServiceID,
			BackingServiceID: consumer.Record.BackingServiceID,
			Database:         bundle.Identity.Database, Username: bundle.Identity.Role,
			Password: slices.Clone(bundle.Identity.Password),
		}
		defer request.Clear()
		switch adapter.Key() {
		case "postgres":
			request.Family = backingaccess.PostgreSQL
		case "mysql":
			request.Family = backingaccess.MySQL
		case "valkey":
			request.Family = backingaccess.Valkey
		default:
			return errs.New(errs.KindValidationFailed, "this Backing adapter does not provide an access check")
		}
		authentication, err := core.ResolveBackingAuthentication(
			adapter.SupportsAuthenticationModes(), bundle.Identity.Authentication,
		)
		if err != nil {
			return errs.New(errs.KindInternal, "captured Backing authentication is invalid")
		}
		if !adapter.SupportsAuthenticationModes() {
			request.Authentication = backingaccess.UsernamePassword
		} else {
			switch authentication {
			case core.BackingAuthenticationUsernamePassword:
				request.Authentication = backingaccess.UsernamePassword
			case core.BackingAuthenticationPassword:
				request.Authentication = backingaccess.Password
				request.Username = ""
			case core.BackingAuthenticationNone:
				request.Authentication = backingaccess.None
			}
		}
		if grantID != "" {
			found := false
			for _, identity := range bundle.Identity.Grants {
				if identity.AttachID == grantID {
					request.Database, found = identity.Database, true
					break
				}
			}
			if !found {
				return errs.New(errs.KindInternal, "captured Backing grant identity is unavailable")
			}
		}
		portText := ""
		found := false
		for _, set := range bundle.Sets {
			if set.GrantAttachID != grantID {
				continue
			}
			found = true
			for _, fact := range set.Facts {
				switch fact.Key {
				case adapter.FactsPrefix() + string(adapters.FactHost):
					request.Host = string(fact.Value)
				case adapter.FactsPrefix() + string(adapters.FactPort):
					portText = string(fact.Value)
				}
			}
			break
		}
		port, err := strconv.ParseUint(portText, 10, 16)
		if !found || err != nil || strconv.FormatUint(port, 10) != portText {
			return errs.New(errs.KindInternal, "captured Backing endpoint is unavailable")
		}
		request.Port = uint16(port)
		if err := request.Validate(); err != nil {
			return errs.New(errs.KindInternal, "captured Backing access credentials are incomplete")
		}
		return consume(request)
	})
}
