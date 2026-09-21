package etcd

import (
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	scriptrecord "github.com/AlanD20/groundplane/internal/infra/etcd/scripts"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func classifyScriptWriteConflict(
	values []*etcdstore.KeyValue,
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	target etcdstore.Versioned[ServiceRecord],
	record scriptrecord.Record,
	expectedScriptRevision int64,
	extras scriptWriteConflictExtras,
) error {
	expected := 11
	if expectedScriptRevision == 0 {
		expected++
	}
	hasTenant := project.Record.TenantID != ""
	if hasTenant {
		expected++
	}
	if extras.newSlug != "" {
		expected++
	}
	if extras.bodyGeneration {
		expected++
	}
	if len(values) != expected {
		return errs.New(errs.KindInternal, "Script write compare evidence is incomplete")
	}
	if expectedScriptRevision == 0 {
		if values[0] != nil || values[1] != nil {
			return errs.New(errs.KindStateConflict, "Script stable identity is already in use")
		}
		if values[2] != nil {
			return errs.New(errs.KindNameConflict, "Script slug is already in use")
		}
	} else {
		if values[0] == nil {
			return errs.New(errs.KindScriptNotFound, "Script was not found")
		}
		if values[0].ModRevision != expectedScriptRevision {
			return stateConflict("script", record.Desired.ID)
		}
		for _, index := range []int{1, 2} {
			if values[index] == nil || string(values[index].Value) != record.Desired.ID {
				return errs.New(errs.KindInternal, "Script index changed or is corrupt")
			}
		}
	}
	if values[3] == nil {
		return errs.New(errs.KindEnvironmentNotFound, "Environment was not found")
	}
	if values[3].ModRevision != environment.Revision {
		return stateConflict("environment", environment.Record.ID)
	}
	if values[4] == nil {
		return errs.New(errs.KindProjectNotFound, "Project was not found")
	}
	if values[4].ModRevision != project.Revision {
		return stateConflict("project", project.Record.ID)
	}
	if values[5] == nil {
		return errs.New(errs.KindServiceNotFound, "Script target Service was not found")
	}
	if values[5].ModRevision != target.Revision {
		return stateConflict("service", target.Record.Desired.ID)
	}
	for _, index := range []int{6, 7, 8, 9} {
		if values[index] != nil {
			return errs.New(errs.KindResourceInUse, "Script hierarchy or target deletion is in progress")
		}
	}
	extraIndex := 10
	if values[extraIndex] == nil {
		return errs.New(errs.KindInternal, "Environment active Script-set generation is missing")
	}
	extraIndex++
	if expectedScriptRevision == 0 {
		if values[extraIndex] != nil {
			return errs.New(errs.KindStateConflict, "Script stable identity is already in use")
		}
		extraIndex++
	}
	if hasTenant && values[extraIndex] != nil {
		return errs.New(errs.KindResourceInUse, "Tenant deletion is in progress")
	}
	if hasTenant {
		extraIndex++
	}
	if extras.newSlug != "" {
		if values[extraIndex] != nil {
			return errs.New(errs.KindNameConflict, "Script slug is already in use")
		}
		extraIndex++
	}
	if extras.bodyGeneration && values[extraIndex] != nil {
		return errs.New(errs.KindStateConflict, "Script body generation is already in use")
	}
	return stateConflict("script", record.Desired.ID)
}

type scriptWriteConflictExtras struct {
	newSlug        string
	bodyGeneration bool
}
