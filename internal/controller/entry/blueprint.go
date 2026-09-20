package entry

import (
	entryrecord "github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"sort"

	"github.com/AlanD20/groundplane/internal/core"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// BlueprintProjection validates pinned Entry metadata without losing authored
// keys. Reconciliation and explicit Script grants consume this same projection.
func BlueprintProjection(
	current []etcdstore.Versioned[entryrecord.Record],
) ([]entryrecord.Record, error) {
	records := make([]entryrecord.Record, len(current))
	for index, versioned := range current {
		var err error
		records[index], err = entryrecord.NewRecord(
			versioned.Record.EnvironmentID,
			versioned.Record.Entry,
			versioned.Record.CurrentValueGenerationID,
		)
		if err != nil {
			return nil, err
		}
		records[index].BlueprintKey = versioned.Record.BlueprintKey
	}
	sort.Slice(records, func(left int, right int) bool {
		return records[left].Entry.ID < records[right].Entry.ID
	})
	return records, nil
}

// BlueprintAuthoring emits only Blueprint-owned desired fields, never values
// from the private Entry generation store or generated runtime state.
func BlueprintAuthoring(
	records []entryrecord.Record,
) (map[string]core.EntrySpec, error) {
	result := make(map[string]core.EntrySpec)
	for _, record := range records {
		if record.BlueprintKey == "" {
			continue
		}
		if _, duplicate := result[record.BlueprintKey]; duplicate {
			return nil, errs.New(errs.KindInternal, "Environment Blueprint Entry key is duplicated")
		}
		entry := record.Entry
		source := core.EntrySourceSpec{
			Literal:   entry.Source.Literal,
			SecretRef: entry.Source.SecretRef,
			Fact:      entry.Source.Fact,
		}
		result[record.BlueprintKey] = core.EntrySpec{
			Kind: entry.Kind, Path: entry.Path, UID: entry.UID, GID: entry.GID,
			Source: source, Exposure: append([]string(nil), entry.Exposure...), Secret: entry.Secret,
		}
	}
	return result, nil
}
