package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// EntryMutationSource separates authored Entry selection from optional rendered
// runtime. Missing runtime is not an empty Environment or permission for effects.
type EntryMutationSource struct {
	Desired keyvalue.Versioned[environmentprojection.EnvironmentDesiredInput]
	Owned   environmentprojection.EnvironmentOwnedIdentities
	Entries []entries.Record
	Runtime *keyvalue.Versioned[environmentprojection.EnvironmentComposeProjection]
}

func (repository *EnvironmentBlueprintRepository) ReadEntryMutationSource(
	ctx context.Context, environmentID string,
) (EntryMutationSource, error) {
	return repository.readEntryMutationSourceAt(ctx, environmentID, 0)
}

func (repository *EnvironmentBlueprintRepository) readEntryMutationSourceAt(
	ctx context.Context, environmentID string, revision int64,
) (EntryMutationSource, error) {
	desired, found, err := blueprints.ReadCurrentDesiredInput(ctx, repository.store, environmentID, revision)
	if err != nil {
		return EntryMutationSource{}, err
	}
	if !found {
		return EntryMutationSource{}, errs.New(
			errs.KindStateConflict,
			"Entry mutation requires initialized desired state",
		)
	}
	owned, found, err := blueprints.ReadOwnedIdentitiesRevision(ctx, repository.store,
		environmentID, desired.Record.RevisionID, desired.ReadRevision)
	if err != nil {
		return EntryMutationSource{}, err
	}
	if !found || owned.Record.RenderGeneration != desired.Record.RenderGeneration {
		return EntryMutationSource{}, errs.New(errs.KindInternal, "Entry authored identities are unavailable")
	}
	records, err := environmentprojection.AuthoredEntryRecords(desired.Record, owned.Record)
	if err != nil {
		return EntryMutationSource{}, err
	}
	result := EntryMutationSource{Desired: desired, Owned: owned.Record, Entries: records}
	runtime, available, err := blueprints.ReadAvailableRuntimeRevision(ctx, repository.store,
		environmentID, desired.Record.RevisionID, desired.ReadRevision)
	if err != nil {
		return EntryMutationSource{}, err
	}
	if available {
		runtime.Revision = desired.Revision
		result.Runtime, result.Entries = &runtime, runtime.Record.Entries
	}
	return result, nil
}
