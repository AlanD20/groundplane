package blueprint

import (
	"sort"

	"github.com/AlanD20/groundplane/internal/core"
	entryrecord "github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// authoredEntryRecords reconstructs desired Entry metadata from the pinned
// operator input and its identity sidecar. Flat Entry records may lag a
// pending Blueprint head, so they cannot supply the coordinator's identity or
// selected value generation.
func authoredEntryRecords(
	desired projectionrecord.EnvironmentDesiredInput,
	identities projectionrecord.EnvironmentOwnedIdentities,
) ([]entryrecord.Record, error) {
	if desired.EnvironmentID != identities.EnvironmentID ||
		desired.RevisionID != identities.RevisionID ||
		len(desired.Input.Entries) != len(identities.Entries) {
		return nil, errs.New(errs.KindStateConflict, "Blueprint Entry desired identities changed")
	}
	result := make([]entryrecord.Record, len(identities.Entries))
	for index, identity := range identities.Entries {
		spec, found := desired.Input.Entries[identity.Name]
		if !found {
			return nil, errs.New(errs.KindStateConflict, "Blueprint Entry desired identity is missing")
		}
		entry, err := core.ProjectEntrySpec(identity.Name, spec, identity.ID)
		if err != nil {
			return nil, errs.Wrap(errs.KindInternal, err)
		}
		if entry.Secret && entry.Source.Kind == core.SourceLiteral {
			entry.Source.Literal = ""
		}
		record, err := entryrecord.NewBlueprintRecord(
			desired.EnvironmentID, identity.Name, entry, identity.ValueGenerationID,
		)
		if err != nil {
			return nil, err
		}
		result[index] = record
	}
	sort.Slice(result, func(left, right int) bool {
		return result[left].Entry.ID < result[right].Entry.ID
	})
	return result, nil
}
