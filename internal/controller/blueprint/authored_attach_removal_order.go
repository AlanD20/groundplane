package blueprint

import (
	"context"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Credential dependencies come from retained declarations as well as bindings:
// an interrupted Apply may never have materialized a dependent binding.
func (service *Service) orderAuthoredAttachRemovals(ctx context.Context,
	environmentID string, units []blueprintunits.Unit,
	owned environmentprojection.EnvironmentOwnedIdentities, revision int64,
) ([]blueprintunits.Unit, error) {
	removals := make(map[string]int)
	consumers := make(map[string]string)
	for index, unit := range units {
		if unit.Removal && unit.Target.Kind == ids.KindAttach {
			removals[unit.Target.ID] = index
		}
	}
	if len(removals) == 0 {
		return units, nil
	}
	addReferences := func(id string, references []string) error {
		for _, referencedID := range references {
			index, removing := removals[referencedID]
			if !removing {
				continue
			}
			if _, removing := removals[id]; !removing {
				return errs.New(errs.KindResourceInUse,
					"Blueprint omits an Attach still used by a retained credential or grant binding")
			}
			units[index].After = canonicalAuthoredUnitKeys(append(units[index].After,
				blueprintunits.ResourceKey{Kind: ids.KindAttach, ID: id}))
		}
		return nil
	}
	for id, index := range removals {
		source, err := service.repository.ReadBlueprintOmissionSource(ctx, owned, ids.KindAttach, id, revision)
		if err != nil {
			return nil, err
		}
		spec, found := source.Desired.Input.Attachments[source.Identity.Name]
		if !found {
			return nil, errs.New(errs.KindStateConflict, "Removed Attach declaration is unavailable")
		}
		for _, identity := range source.Identities.Services {
			if identity.Name == spec.Service {
				consumers[id] = identity.ID
			}
		}
		if consumers[id] == "" {
			return nil, errs.New(errs.KindStateConflict, "Removed Attach consumer identity is unavailable")
		}
		units[index].Writes = canonicalAuthoredUnitKeys(append(units[index].Writes,
			blueprintunits.ResourceKey{Kind: ids.KindService, ID: consumers[id]}))
		references := slices.Clone(spec.Grants)
		if spec.Credential.Mode == "existing" {
			references = append(references, spec.Credential.Attach)
		}
		resolved := make([]string, 0, len(references))
		for _, reference := range references {
			resolvedID := ""
			if ids.Validate(ids.KindAttach, reference) == nil {
				resolvedID = reference
			}
			for _, identity := range source.Identities.Attaches {
				if identity.Name == reference || identity.ID == reference {
					resolvedID = identity.ID
				}
			}
			if resolvedID == "" {
				return nil, errs.New(errs.KindStateConflict, "Removed Attach dependency identity is unavailable")
			}
			resolved = append(resolved, resolvedID)
		}
		if err := addReferences(id, resolved); err != nil {
			return nil, err
		}
	}
	cursor := ""
	for {
		page, err := service.repository.ListAttaches(ctx, environmentID, keyvalue.PageRequest{
			Limit: keyvalue.MaximumPageLimit, Cursor: cursor, Revision: revision,
		})
		if err != nil {
			return nil, err
		}
		if page.Revision != revision {
			return nil, errs.New(errs.KindStateConflict, "Blueprint Attach removal topology changed")
		}
		for _, source := range page.Items {
			record := source.Record
			references := slices.Clone(record.GrantAttachIDs)
			if !record.OwnsCredential() {
				references = append(references, record.CredentialAttachID)
			}
			if err := addReferences(record.ID, references); err != nil {
				return nil, err
			}
		}
		if page.NextCursor == "" {
			break
		}
		cursor = page.NextCursor
	}
	// Serialize physical changes for each consumer in an existing dependency
	// order. A lexicographic chain alone could reverse credential dependencies.
	pending := make([]string, 0, len(removals))
	for id := range removals {
		pending = append(pending, id)
	}
	slices.Sort(pending)
	visited := make(map[string]bool, len(pending))
	last := make(map[string]string)
	for len(pending) != 0 {
		selected := -1
		for index, id := range pending {
			ready := true
			for _, dependency := range units[removals[id]].After {
				if dependency.Kind == ids.KindAttach {
					if _, removing := removals[dependency.ID]; removing && !visited[dependency.ID] {
						ready = false
						break
					}
				}
			}
			if ready {
				selected = index
				break
			}
		}
		if selected < 0 {
			return nil, errs.New(
				errs.KindValidationFailed,
				"Removed Attach credentials or grants form a dependency cycle",
			)
		}
		id := pending[selected]
		index := removals[id]
		if predecessor := last[consumers[id]]; predecessor != "" {
			units[index].After = canonicalAuthoredUnitKeys(append(units[index].After,
				blueprintunits.ResourceKey{Kind: ids.KindAttach, ID: predecessor}))
		}
		last[consumers[id]], visited[id] = id, true
		pending = slices.Delete(pending, selected, selected+1)
	}
	return units, nil
}
