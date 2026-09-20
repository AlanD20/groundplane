package scriptdefinition

import (
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	entryrecord "github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	scriptrecord "github.com/AlanD20/groundplane/internal/infra/etcd/scripts"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Authoring projects Blueprint-owned Scripts using their immutable authored keys.
func Authoring(
	records []etcd.Versioned[scriptrecord.Record],
	volumes []etcd.EnvironmentVolumeIdentity,
	entries []entryrecord.Record,
) (map[string]core.ScriptSpec, error) {
	result := make(map[string]core.ScriptSpec)
	for _, versioned := range records {
		record := versioned.Record
		if record.Origin != "blueprint" || record.ReconciliationKey == "" {
			continue
		}
		if _, duplicate := result[record.ReconciliationKey]; duplicate {
			return nil, errs.New(errs.KindInternal, "Environment Blueprint Script key is duplicated")
		}
		execution, err := authoringExecution(record, volumes, entries)
		if err != nil {
			return nil, err
		}
		result[record.ReconciliationKey] = core.ScriptSpec{
			Slug:      record.Desired.Slug,
			Service:   record.Desired.ServiceName,
			When:      record.Desired.When,
			Order:     record.Desired.Order,
			Script:    record.Desired.Body,
			Execution: execution,
		}
	}
	return result, nil
}
