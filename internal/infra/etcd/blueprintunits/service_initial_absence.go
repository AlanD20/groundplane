package blueprintunits

import (
	"slices"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// RequireInitialServiceAbsence proves this identity has never acquired an
// applied runtime. The scheduler must also join live native resource claims.
// No execution receipt is invented for a definition-only removal: its original
// positive birth-absence evidence remains authoritative after the head changes.
func RequireInitialServiceAbsence(snapshot Snapshot, serviceID string) ([]keyvalue.Condition, error) {
	target := ResourceKey{Kind: ids.KindService, ID: serviceID}
	if !validKey(target) || snapshot.ReadRevision <= 0 || snapshot.HeadRevision <= 0 ||
		ids.Validate(ids.KindEnvironment, snapshot.EnvironmentID) != nil {
		return nil, invalidRecord()
	}
	found := false
	for _, applied := range snapshot.Applied {
		if applied.Record.Target != target {
			continue
		}
		if found || applied.Revision != 0 || applied.Record.State != Absent || applied.Record.HasEffectSource() {
			return nil, errs.New(
				errs.KindStateConflict,
				"Service has applied effects; removal requires captured runtime proof",
			)
		}
		found = true
	}
	if !found {
		return nil, errs.New(
			errs.KindStateConflict,
			"Service has no positive evidence that its workload was never created",
		)
	}
	for _, execution := range snapshot.Executions {
		if slices.Contains(execution.Record.Unit.Writes, target) ||
			slices.Contains(execution.Record.Unit.Reads, target) {
			return nil, errs.New(errs.KindStateConflict, "Service still has unsettled Blueprint execution")
		}
	}
	conditions := []keyvalue.Condition{
		{Key: blueprints.EnvironmentBlueprintHeadKey(snapshot.EnvironmentID), ModRevision: snapshot.HeadRevision},
		{Key: EpochKey(snapshot.EnvironmentID), ModRevision: snapshot.EpochRevision},
		{Key: AppliedKey(snapshot.EnvironmentID, target)},
	}
	birthFound := false
	for _, birth := range snapshot.initialAbsence {
		if !slices.Contains(birth.Record.Targets, target) {
			continue
		}
		if birthFound || birth.Revision <= 0 || birth.ReadRevision != snapshot.ReadRevision ||
			birth.Record.EnvironmentID != snapshot.EnvironmentID || !validInitialAbsence(birth.Record) {
			return nil, corruptRecord()
		}
		birthFound = true
		conditions = append(conditions, keyvalue.Condition{
			Key: initialAbsenceKey(snapshot.EnvironmentID, birth.Record.BirthRevisionID), ModRevision: birth.Revision,
		})
	}
	if !birthFound {
		return nil, errs.New(errs.KindStateConflict, "Service birth evidence is unavailable")
	}
	return conditions, nil
}
