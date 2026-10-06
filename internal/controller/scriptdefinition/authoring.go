package scriptdefinition

import (
	"github.com/AlanD20/groundplane/internal/core"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"

	entryrecord "github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/scriptauthoring"
	scriptrecord "github.com/AlanD20/groundplane/internal/infra/etcd/scripts"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Authoring projects Blueprint-owned Scripts using their immutable authored keys.
func Authoring(
	records []etcdstore.Versioned[scriptrecord.Record],
	volumes []projectionrecord.EnvironmentVolumeIdentity,
	entries []entryrecord.Record,
) (map[string]core.ScriptSpec, error) {
	result := make(map[string]core.ScriptSpec)
	for _, versioned := range records {
		record := versioned.Record
		key, err := scriptrecord.BlueprintAuthoringKey(record)
		if err != nil {
			return nil, err
		}
		if _, duplicate := result[key]; duplicate {
			return nil, errs.New(errs.KindInternal, "Environment Blueprint Script key is duplicated")
		}
		_, spec, err := scriptauthoring.Script(record, volumes, entries)
		if err != nil {
			return nil, err
		}
		result[key] = spec
	}
	return result, nil
}
