package environmentqueries

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// EntryDeclarations contains desired metadata, never resolved execution input.
type EntryDeclarations struct {
	RevisionID string
	Entries    []entries.Record
}

func (repository *ProjectionReader) GetEnvironmentEntryDeclarations(ctx context.Context,
	environmentID string,
) (keyvalue.Versioned[EntryDeclarations], bool, error) {
	input, found, err := blueprints.ReadCurrentDesiredInput(ctx, repository.store, environmentID, 0)
	if err != nil || !found {
		return keyvalue.Versioned[EntryDeclarations]{}, found, err
	}
	return repository.entryDeclarations(ctx, input)
}

func (repository *ProjectionReader) GetEnvironmentEntryDeclarationsRevision(ctx context.Context,
	environmentID, revisionID string,
) (keyvalue.Versioned[EntryDeclarations], bool, error) {
	input, found, err := blueprints.ReadDesiredInputRevision(ctx, repository.store, environmentID, revisionID, 0)
	if err != nil || !found {
		return keyvalue.Versioned[EntryDeclarations]{}, found, err
	}
	return repository.entryDeclarations(ctx, input)
}

func (repository *ProjectionReader) entryDeclarations(ctx context.Context,
	input keyvalue.Versioned[environmentprojection.EnvironmentDesiredInput],
) (keyvalue.Versioned[EntryDeclarations], bool, error) {
	// An exact-head render also contains adapter-generated Entries. Keep those
	// when available; only an unrendered head needs authored reconstruction.
	runtime, available, err := blueprints.ReadAvailableRuntimeRevision(ctx, repository.store,
		input.Record.EnvironmentID, input.Record.RevisionID, input.ReadRevision)
	if err != nil {
		return keyvalue.Versioned[EntryDeclarations]{}, false, err
	}
	if available {
		return keyvalue.Versioned[EntryDeclarations]{
			Record:   EntryDeclarations{RevisionID: input.Record.RevisionID, Entries: runtime.Record.Entries},
			Revision: input.Revision, ReadRevision: input.ReadRevision,
		}, true, nil
	}
	owned, found, err := blueprints.ReadOwnedIdentitiesRevision(ctx, repository.store,
		input.Record.EnvironmentID, input.Record.RevisionID, input.ReadRevision)
	if err != nil {
		return keyvalue.Versioned[EntryDeclarations]{}, false, err
	}
	if !found || owned.Record.RenderGeneration != input.Record.RenderGeneration {
		return keyvalue.Versioned[EntryDeclarations]{}, false, errs.New(
			errs.KindInternal,
			"Entry declared identities are unavailable",
		)
	}
	records, err := environmentprojection.AuthoredEntryRecords(input.Record, owned.Record)
	if err != nil {
		return keyvalue.Versioned[EntryDeclarations]{}, false, err
	}
	return keyvalue.Versioned[EntryDeclarations]{
		Record:   EntryDeclarations{RevisionID: input.Record.RevisionID, Entries: records},
		Revision: input.Revision, ReadRevision: input.ReadRevision,
	}, true, nil
}
