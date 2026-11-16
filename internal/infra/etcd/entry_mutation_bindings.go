package etcd

import (
	"context"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/secrets"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// EntryMutationBindings contains identities only, never a selected generation,
// value, runtime projection, or permission to execute work.
type EntryMutationBindings struct {
	SecretID    string   `json:"secret_id,omitempty"`
	ServiceIDs  []string `json:"service_ids,omitempty"`
	AllServices bool     `json:"all_services,omitempty"`
}

func (repository *HierarchyRepository) BindEntryMutationReferences(ctx context.Context, environmentID, projectID,
	secretRef string, exposure []string,
) (EntryMutationBindings, error) {
	var result EntryMutationBindings
	if secretRef != "" {
		metadata, err := secrets.NewReader(repository.store).ResolveSecret(ctx, projectID, secretRef)
		if err != nil {
			return EntryMutationBindings{}, err
		}
		result.SecretID = metadata.Record.Secret.ID
	}
	if len(exposure) == 1 && exposure[0] == "all" {
		result.AllServices = true
		return result, nil
	}
	if len(exposure) == 0 {
		return result, nil
	}
	owned, found, err := blueprints.ReadCurrentOwnedIdentities(ctx, repository.store, environmentID, 0)
	if err != nil {
		return EntryMutationBindings{}, err
	}
	if !found {
		return EntryMutationBindings{}, errs.New(errs.KindServiceNotFound, "Entry exposure Service was not found")
	}
	byName := make(map[string]string, len(owned.Record.Services))
	for _, service := range owned.Record.Services {
		byName[service.Name] = service.ID
	}
	for _, name := range exposure {
		id := byName[name]
		if id == "" {
			return EntryMutationBindings{}, errs.New(errs.KindServiceNotFound, "Entry exposure Service was not found")
		}
		result.ServiceIDs = append(result.ServiceIDs, id)
	}
	return result, nil
}

func (repository *HierarchyRepository) ResolveEntryMutationExposure(ctx context.Context, environmentID string,
	bindings EntryMutationBindings,
) ([]string, error) {
	if bindings.AllServices {
		return []string{"all"}, nil
	}
	if len(bindings.ServiceIDs) == 0 {
		return nil, nil
	}
	owned, found, err := blueprints.ReadCurrentOwnedIdentities(ctx, repository.store, environmentID, 0)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, errs.New(errs.KindServiceNotFound, "An accepted Entry exposure Service no longer exists")
	}
	byID := make(map[string]string, len(owned.Record.Services))
	for _, service := range owned.Record.Services {
		byID[service.ID] = service.Name
	}
	result := make([]string, 0, len(bindings.ServiceIDs))
	for _, id := range bindings.ServiceIDs {
		name := byID[id]
		if name == "" {
			return nil, errs.New(errs.KindServiceNotFound, "An accepted Entry exposure Service no longer exists")
		}
		result = append(result, name)
	}
	return result, nil
}
