package environmentqueries

import (
	"context"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// FindDeclaredEntry includes accepted Entries whose settlement has not yet
// created a flat record or reverse lookup. Ownership and input share one revision.
func (repository *ProjectionReader) FindDeclaredEntry(ctx context.Context, entryID string) (
	keyvalue.Versioned[entries.Record], bool, error,
) {
	if ids.Validate(ids.KindEnvEntry, entryID) != nil {
		return keyvalue.Versioned[entries.Record]{}, false, errs.New(errs.KindValidationFailed, "Entry id is invalid")
	}
	start, revision := "", int64(0)
	for {
		page, err := repository.store.Range(ctx, keyvalue.RangeRequest{
			Prefix: EnvironmentDesiredHeadScanPrefix, StartExclusive: start, Limit: 128, Revision: revision,
		})
		if err != nil {
			return keyvalue.Versioned[entries.Record]{}, false, err
		}
		if page == nil || page.ReadRevision <= 0 || page.More && len(page.Values) == 0 {
			return keyvalue.Versioned[entries.Record]{}, false, errs.New(
				errs.KindInternal,
				"Entry ownership scan did not advance",
			)
		}
		revision = page.ReadRevision
		for _, value := range page.Values {
			start = value.Key
			if !strings.HasSuffix(value.Key, "/current") {
				continue
			}
			environmentID := strings.TrimSuffix(
				strings.TrimPrefix(value.Key, EnvironmentDesiredHeadScanPrefix),
				"/current",
			)
			owns, err := environmentOwnsDesiredResource(
				ctx,
				repository.store,
				environmentID,
				ids.KindEnvEntry,
				entryID,
				revision,
			)
			if err != nil {
				return keyvalue.Versioned[entries.Record]{}, false, err
			}
			if !owns {
				continue
			}
			input, found, err := blueprints.ReadCurrentDesiredInput(ctx, repository.store, environmentID, revision)
			if err != nil {
				return keyvalue.Versioned[entries.Record]{}, false, err
			}
			if !found {
				return keyvalue.Versioned[entries.Record]{}, false, errs.New(
					errs.KindInternal,
					"Entry owner has no desired input",
				)
			}
			declared, found, err := repository.entryDeclarations(ctx, input)
			if err != nil {
				return keyvalue.Versioned[entries.Record]{}, false, err
			}
			if found {
				for _, record := range declared.Record.Entries {
					if record.Entry.ID == entryID {
						return keyvalue.Versioned[entries.Record]{
							Record:       record,
							Revision:     declared.Revision,
							ReadRevision: revision,
						}, true, nil
					}
				}
			}
			return keyvalue.Versioned[entries.Record]{}, false, errs.New(
				errs.KindInternal,
				"Entry owner has no matching declaration",
			)
		}
		if !page.More {
			return keyvalue.Versioned[entries.Record]{}, false, nil
		}
	}
}
