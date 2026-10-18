package blueprint

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/desiredrevision"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	attachinputs "github.com/AlanD20/groundplane/internal/infra/etcd/blueprintattachinputs"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type authoredAttachFactReader interface {
	GetAttachFactsVersioned(
		context.Context,
		etcdstore.Versioned[attachrecord.Record],
	) (etcdstore.Versioned[attachrecord.EncryptedFacts], bool, error)
}

func (service *Service) captureAuthoredAttachBinding(
	ctx context.Context, parentID string, spec core.AttachmentSpec,
	consumer etcdstore.Versioned[attachrecord.Record],
	current []etcdstore.Versioned[attachrecord.Record],
	allocator *desiredrevision.BlueprintIdentityAllocator,
) (etcd.BlueprintAttachInputGenerationPublication, error) {
	reader, ok := service.repository.(authoredAttachFactReader)
	if !ok {
		return etcd.BlueprintAttachInputGenerationPublication{}, errs.New(
			errs.KindInternal,
			"Attach fact capture is unavailable",
		)
	}
	byID := make(map[string]etcdstore.Versioned[attachrecord.Record], len(current))
	for _, record := range current {
		byID[record.Record.ID] = record
	}
	credential, found := byID[consumer.Record.CredentialAttachID]
	if !found || credential.ReadRevision != consumer.ReadRevision {
		return etcd.BlueprintAttachInputGenerationPublication{}, errs.New(
			errs.KindStateConflict,
			"Attach credential capture is incomplete",
		)
	}
	facts, found, err := reader.GetAttachFactsVersioned(ctx, credential)
	if err != nil {
		return etcd.BlueprintAttachInputGenerationPublication{}, err
	}
	needsFacts := len(credential.Record.FactSets) != 0 || credential.Record.HookBundle
	if found != needsFacts {
		return etcd.BlueprintAttachInputGenerationPublication{}, errs.New(
			errs.KindStateConflict,
			"Attach credential facts are unavailable",
		)
	}
	failed := true
	defer func() {
		if failed {
			clear(facts.Record.Ciphertext)
		}
	}()
	binding := &attachinputs.CapturedBinding{
		Consumer:      attachinputs.BindingRecord{Record: consumer.Record, Revision: consumer.Revision},
		Credential:    attachinputs.BindingRecord{Record: credential.Record, Revision: credential.Revision},
		FactsRevision: facts.Revision,
	}
	for _, id := range credential.Record.GrantAttachIDs {
		grant, found := byID[id]
		if !found || grant.ReadRevision != consumer.ReadRevision {
			return etcd.BlueprintAttachInputGenerationPublication{}, errs.New(
				errs.KindStateConflict,
				"Attach grant capture is incomplete",
			)
		}
		binding.Grants = append(
			binding.Grants,
			attachinputs.BindingRecord{Record: grant.Record, Revision: grant.Revision},
		)
	}
	project, err := service.repository.GetProject(ctx, consumer.Record.BackingProjectID)
	if err != nil {
		return etcd.BlueprintAttachInputGenerationPublication{}, err
	}
	environment, err := service.repository.GetEnvironment(ctx, consumer.Record.BackingEnvironmentID)
	if err != nil {
		return etcd.BlueprintAttachInputGenerationPublication{}, err
	}
	backing, err := service.repository.GetService(ctx, consumer.Record.BackingServiceID)
	if err != nil {
		return etcd.BlueprintAttachInputGenerationPublication{}, err
	}
	fingerprint, err := core.AttachmentSpecFingerprint(spec)
	if err != nil {
		return etcd.BlueprintAttachInputGenerationPublication{}, err
	}
	operationID := allocator.Named(ids.KindOperation, "attach-input:"+consumer.Record.Name)
	generation := attachinputs.Generation{
		ID: operationID, OperationID: operationID, EnvironmentID: consumer.Record.EnvironmentID,
		RevisionID: parentID, ParentTaskID: parentID,
		AttachID: consumer.Record.ID, AttachName: consumer.Record.Name, AuthoredSpecSHA256: fingerprint,
		ConsumerServiceID: consumer.Record.ServiceID, CredentialOwnerID: credential.Record.ID,
		BackingProjectID: project.Record.ID, BackingEnvironmentID: environment.Record.ID,
		BackingServiceID: backing.Record.Desired.ID, BackingNetworkID: consumer.Record.BackingNetworkID,
		AdapterKey: backing.Record.Desired.Adapter, Authentication: backing.Record.Desired.Authentication,
		Binding: binding, FactSets: credential.Record.FactSets, GeneratedInputs: facts.Record,
	}
	if err := attachinputs.ValidateDraft(generation); err != nil {
		return etcd.BlueprintAttachInputGenerationPublication{}, err
	}
	failed = false
	return etcd.BlueprintAttachInputGenerationPublication{
		Generation: generation, BackingProject: project, BackingEnvironment: environment, BackingService: backing,
	}, nil
}
