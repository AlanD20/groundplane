package blueprintunits

import (
	"slices"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core/blueprintreconcile"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PrepareControllerEntrySettlement records a Controller-owned value effect
// without manufacturing an Agent execution. The caller must join these
// mutations atomically to creation of the exact Entry value generation.
func PrepareControllerEntrySettlement(
	snapshot Snapshot,
	unit Unit,
	operationID string,
	valueGenerationID string,
) (MutationPlan, error) {
	if snapshot.Desired == nil || snapshot.Desired.Record.ParentTaskID != snapshot.HeadTaskID ||
		unit.Target.Kind != ids.KindEnvEntry || unit.Removal ||
		ids.Validate(ids.KindOperation, operationID) != nil ||
		ids.Validate(ids.KindConfig, valueGenerationID) != nil ||
		len(unit.Writes) != 1 || unit.Writes[0] != unit.Target {
		return MutationPlan{}, invalidRecord()
	}
	found := false
	for _, desired := range snapshot.Desired.Record.Units {
		if desired.Target != unit.Target {
			continue
		}
		if desired.Fingerprint != unit.Fingerprint || desired.Removal != unit.Removal ||
			!slices.Equal(desired.Reads, unit.Reads) ||
			!slices.Equal(desired.Writes, unit.Writes) ||
			!slices.Equal(desired.After, unit.After) {
			return MutationPlan{}, errs.New(errs.KindStateConflict, "Blueprint Entry unit changed")
		}
		found = true
		break
	}
	if !found {
		return MutationPlan{}, errs.New(errs.KindStateConflict, "Blueprint Entry unit is not desired")
	}
	selection, err := Select(snapshot)
	if err != nil {
		return MutationPlan{}, err
	}
	if !slices.Contains(selection.Ready, blueprintreconcile.ResourceKey{
		Kind: unit.Target.Kind, ID: unit.Target.ID,
	}) {
		return MutationPlan{}, errs.New(errs.KindStateConflict, "Blueprint Entry unit is not ready")
	}
	applied := AppliedRecord{
		EnvironmentID: snapshot.EnvironmentID, Target: unit.Target,
		State: Applied, Fingerprint: unit.Fingerprint,
		ParentTaskID:                snapshot.HeadTaskID,
		ControllerOperationID:       operationID,
		ControllerValueGenerationID: valueGenerationID,
	}
	plan, err := PrepareMutation(snapshot, []AppliedChange{{Target: unit.Target, Next: &applied}}, nil)
	if err != nil {
		return MutationPlan{}, err
	}
	plan.conditions = append(plan.conditions, etcdstore.Condition{
		Key: DesiredPlanKey(snapshot.EnvironmentID), ModRevision: snapshot.Desired.Revision,
	})
	return plan, nil
}
