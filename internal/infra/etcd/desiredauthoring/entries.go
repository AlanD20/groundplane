package desiredauthoring

import (
	"context"
	"sort"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PrepareUnrenderedEntries changes only authored Entry selection. The caller
// must separately prove that no acknowledged consumer or file needs updating.
// A nil runtime stays nil; this does not manufacture a rendered Environment.
func PrepareUnrenderedEntries(ctx context.Context, store Store, environmentID string,
	marker idempotency.IdempotencyMarker, expectedHead int64, records []entries.Record,
) (Publication, error) {
	if marker.Kind != idempotency.IdempotencyMarkerTask || marker.State != idempotency.IdempotencyMarkerPending ||
		marker.Locator.ScopeKind != idempotency.IdempotencyScopeEnvironment || marker.Locator.ScopeID != environmentID ||
		idempotency.ValidateIdempotencyMarker(marker) != nil || ids.Validate(ids.KindTask, marker.TaskID) != nil {
		return Publication{}, errs.New(errs.KindValidationFailed, "Entry metadata requires its native Task marker")
	}
	snapshot, err := loadSnapshot(ctx, store, environmentID)
	if err != nil {
		return Publication{}, err
	}
	if snapshot.headRevision != expectedHead || snapshot.projection != nil {
		return Publication{}, errs.New(errs.KindStateConflict, "Entry metadata or runtime changed before publication")
	}
	previous := environmentprojection.CloneEnvironmentOwnedIdentities(snapshot.owned)
	previous.RevisionID = snapshot.baseRevisionID
	snapshot.input.RevisionID, snapshot.owned.RevisionID = marker.TaskID, marker.TaskID
	byID := make(map[string]environmentprojection.OwnedIdentity, len(previous.Entries))
	for _, identity := range previous.Entries {
		byID[identity.ID] = identity
	}
	authored := make(map[string]core.EntrySpec, len(records))
	identities := make([]environmentprojection.OwnedIdentity, 0, len(records))
	var newborn []blueprintunits.ResourceKey
	services := make(map[string]bool, len(snapshot.owned.Services))
	for _, service := range snapshot.owned.Services {
		services[service.Name] = true
	}
	for _, record := range records {
		if record.EnvironmentID != environmentID || entries.ValidateRecord(record) != nil {
			return Publication{}, errs.New(errs.KindValidationFailed, "Entry metadata belongs to another Environment")
		}
		key := entries.BlueprintIdentityKey(record)
		if key == "" {
			return Publication{}, errs.New(errs.KindValidationFailed, "Entry metadata has no authored identity")
		}
		if _, duplicate := authored[key]; duplicate {
			return Publication{}, errs.New(errs.KindNameConflict, "Entry metadata repeats an authored key")
		}
		value := record.Entry
		if len(value.Exposure) != 1 || value.Exposure[0] != "all" {
			for _, name := range value.Exposure {
				if !services[name] {
					return Publication{}, errs.New(errs.KindServiceNotFound, "Entry exposure Service was not found")
				}
			}
		}
		authored[key] = core.EntrySpec{Kind: value.Kind, Path: value.Path, UID: value.UID, GID: value.GID,
			Secret: value.Secret, Exposure: append([]string(nil), value.Exposure...),
			Source: core.EntrySourceSpec{
				Literal:   value.Source.Literal,
				SecretRef: value.Source.SecretRef,
				Fact:      value.Source.Fact,
			}}
		identity, exists := byID[value.ID]
		if !exists {
			identity = environmentprojection.OwnedIdentity{ID: value.ID, BirthRevisionID: marker.TaskID}
			newborn = append(newborn, blueprintunits.ResourceKey{Kind: ids.KindEnvEntry, ID: value.ID})
		}
		identity.Name, identity.ValueGenerationID = key, record.CurrentValueGenerationID
		identities = append(identities, identity)
	}
	sort.Slice(identities, func(i, j int) bool { return identities[i].Name < identities[j].Name })
	snapshot.input.Input.Entries, snapshot.owned.Entries = authored, identities
	for _, script := range snapshot.input.Input.Scripts {
		if script.Execution == nil {
			continue
		}
		for _, key := range script.Execution.Entries {
			if _, found := authored[key]; !found {
				return Publication{}, errs.New(
					errs.KindResourceInUse,
					"Entry is still granted to a Script; remove that grant first",
				)
			}
		}
	}
	if err := environmentprojection.ValidateEnvironmentDesiredInput(snapshot.input); err != nil {
		return Publication{}, err
	}
	result, err := snapshot.stage(ctx, store, marker)
	if err != nil {
		return Publication{}, err
	}
	birth, err := blueprintunits.PrepareInitialAbsencePublication(environmentID, marker.TaskID, newborn)
	if err != nil {
		return Publication{}, err
	}
	result.Conditions = append(result.Conditions, birth.Conditions()...)
	result.Mutations = append(result.Mutations, birth.Mutations()...)
	return result, nil
}
