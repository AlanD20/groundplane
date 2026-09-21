package attachments

import (
	"context"
	"github.com/AlanD20/groundplane/internal/adapters"
	"github.com/AlanD20/groundplane/internal/common/backingendpoint"
	taskplanning "github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	taskconfiguration "github.com/AlanD20/groundplane/internal/infra/etcd/taskconfiguration"
)

func (service *MutationService) prepareAttachFacts(
	ctx context.Context, attachID string, operationID string,
	consumer etcdstore.Versioned[servicerecord.ServiceRecord],
	scope etcd.AttachCreateScope,
	adapter adapters.Adapter,
) (*taskplanning.AttachPlanIdentity, []attachrecord.FactSetMetadata, *attachrecord.EncryptedFacts,
	*taskconfiguration.BackingHookEncryptedInputs, error,
) {
	if adapter.Custom() {
		hooks := scope.BackingService.Record.Desired.Hooks
		if hooks == nil || hooks.Attach == nil && hooks.Detach == nil {
			return nil, nil, nil, nil, nil
		}
		metadata, encrypted, hookInputs, err := service.facts.SealCustomHookBundle(
			ctx, attachID, scope.BackingProject.Record.ID, operationID, *hooks,
		)
		return nil, metadata, encrypted, hookInputs, err
	}
	authentication := scope.BackingService.Record.Desired.Authentication
	identityName, err := ProvisionIdentity(attachID, consumer.Record.Desired.Name)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	role := identityName
	var password []byte
	if authentication == core.BackingAuthenticationPassword {
		role = "default"
	}
	if authentication == core.BackingAuthenticationNone {
		role = ""
	} else {
		password, err = GeneratePassword(service.random)
		if err != nil {
			return nil, nil, nil, nil, err
		}
	}
	defer clear(password)
	own := adapters.Input{
		Authentication: authentication,
		Host:           backingendpoint.New(scope.BackingService.Record.Desired.ID), Port: adapter.Port(),
		Database: identityName, Role: role, Password: password,
	}
	grantFacts := make([]GrantInput, 0, len(scope.Grants))
	planIdentity := &taskplanning.AttachPlanIdentity{
		Authentication: authentication,
		Database:       identityName, Role: role, Password: append([]byte(nil), password...),
		Grants: make([]taskplanning.AttachPlanGrantIdentity, 0, len(scope.Grants)),
	}
	failed := true
	defer func() {
		if failed {
			planIdentity.Clear()
		}
	}()
	for _, grant := range scope.Grants {
		database := ""
		if err := service.facts.ResolveReadyDatabase(ctx, grant, func(value string) error {
			database = value
			return nil
		}); err != nil {
			return nil, nil, nil, nil, err
		}
		grantFacts = append(grantFacts, GrantInput{
			AttachID: grant.Record.ID,
			Params: adapters.Input{
				Authentication: authentication,
				Host:           backingendpoint.New(scope.BackingService.Record.Desired.ID), Port: adapter.Port(),
				Database: database, Role: role, Password: password,
			},
		})
		planIdentity.Grants = append(planIdentity.Grants, taskplanning.AttachPlanGrantIdentity{
			AttachID: grant.Record.ID, Database: database,
		})
	}
	metadata, encrypted, err := service.facts.SealFactSets(ctx, attachID, adapter, own, grantFacts)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	failed = false
	return planIdentity, metadata, encrypted, nil, nil
}

func attachGrantIDs(grants []etcdstore.Versioned[attachrecord.Record]) []string {
	values := make([]string, len(grants))
	for index, grant := range grants {
		values[index] = grant.Record.ID
	}
	return values
}

func cloneAttachFactMetadata(values []attachrecord.FactSetMetadata) []attachrecord.FactSetMetadata {
	cloned := make([]attachrecord.FactSetMetadata, len(values))
	for index, value := range values {
		cloned[index] = attachrecord.FactSetMetadata{
			GrantAttachID: value.GrantAttachID,
			Facts:         append([]attachrecord.FactDefinition(nil), value.Facts...),
		}
	}
	return cloned
}
