package blueprintunits

import (
	"slices"

	"github.com/AlanD20/groundplane/internal/common/ids"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const maximumDesiredUnits = 512

// PrepareAuthoredPlanPublication seals the accepted resource selection beside
// its new Blueprint head and immutable graph. Unlike later expansion, this
// compares the preceding head; it must join the transaction advancing that head.
func PrepareAuthoredPlanPublication(snapshot Snapshot, next DesiredPlan) (MutationPlan, error) {
	// Captured graphs follow work IDs; selection records follow resource keys.
	// Sorting a private copy preserves the immutable graph's item/unit pairing.
	next.Units = slices.Clone(next.Units)
	slices.SortFunc(next.Units, func(left, right Unit) int { return compareKey(left.Target, right.Target) })
	if ids.Validate(ids.KindEnvironment, snapshot.EnvironmentID) != nil || snapshot.ReadRevision <= 0 ||
		snapshot.HeadRevision < 0 || snapshot.ReadRevision < snapshot.HeadRevision ||
		(snapshot.HeadRevision == 0 && snapshot.HeadTaskID != "") ||
		(snapshot.HeadRevision > 0 && ids.Validate(ids.KindTask, snapshot.HeadTaskID) != nil) ||
		snapshot.EpochRevision < 0 || snapshot.EpochRevision > snapshot.ReadRevision ||
		(snapshot.EpochRevision == 0 && snapshot.Epoch.Sequence != 0) ||
		(snapshot.EpochRevision > 0 && (snapshot.Epoch.EnvironmentID != snapshot.EnvironmentID ||
			snapshot.Epoch.Sequence == 0 || snapshot.Epoch.Sequence == ^uint64(0))) ||
		next.EnvironmentID != snapshot.EnvironmentID || next.ParentTaskID == snapshot.HeadTaskID ||
		!next.Complete || len(next.Units) > maximumDesiredUnits || !validDesiredPlan(next) {
		return MutationPlan{}, invalidRecord()
	}
	currentRevision := int64(0)
	if snapshot.Desired != nil {
		current := snapshot.Desired
		if current.Revision <= 0 || current.Revision > snapshot.EpochRevision ||
			current.Record.EnvironmentID != snapshot.EnvironmentID || !validDesiredPlan(current.Record) {
			return MutationPlan{}, invalidRecord()
		}
		currentRevision = current.Revision
	}
	epochValue, err := EncodeEpoch(EpochRecord{
		EnvironmentID: snapshot.EnvironmentID, Sequence: snapshot.Epoch.Sequence + 1,
	})
	if err != nil {
		return MutationPlan{}, err
	}
	desiredValue, err := EncodeDesiredPlan(next)
	if err != nil {
		clear(epochValue)
		return MutationPlan{}, err
	}
	return MutationPlan{
		conditions: []etcdstore.Condition{
			{Key: blueprints.EnvironmentBlueprintHeadKey(snapshot.EnvironmentID), ModRevision: snapshot.HeadRevision},
			{Key: EpochKey(snapshot.EnvironmentID), ModRevision: snapshot.EpochRevision},
			{Key: DesiredPlanKey(snapshot.EnvironmentID), ModRevision: currentRevision},
		},
		mutations: []etcdstore.Mutation{
			{Type: etcdstore.MutationPut, Key: EpochKey(snapshot.EnvironmentID), Value: epochValue},
			{Type: etcdstore.MutationPut, Key: DesiredPlanKey(snapshot.EnvironmentID), Value: desiredValue},
		},
	}, nil
}

// PrepareDesiredPlan advances the current parent's effective-unit selection.
// Its transaction must also fence the parent claim. An incomplete plan may
// grow after a prerequisite hook produces facts; a complete plan is sealed.
func PrepareDesiredPlan(snapshot Snapshot, next DesiredPlan) (MutationPlan, error) {
	if ids.Validate(ids.KindEnvironment, snapshot.EnvironmentID) != nil ||
		ids.Validate(ids.KindTask, snapshot.HeadTaskID) != nil || snapshot.HeadRevision <= 0 ||
		snapshot.ReadRevision < snapshot.HeadRevision || snapshot.EpochRevision < 0 ||
		(snapshot.EpochRevision == 0 && snapshot.Epoch.Sequence != 0) ||
		(snapshot.EpochRevision > 0 && (snapshot.Epoch.EnvironmentID != snapshot.EnvironmentID ||
			snapshot.Epoch.Sequence == 0 || snapshot.Epoch.Sequence == ^uint64(0))) ||
		next.EnvironmentID != snapshot.EnvironmentID || next.ParentTaskID != snapshot.HeadTaskID ||
		len(next.Units) > maximumDesiredUnits || !validDesiredPlan(next) {
		return MutationPlan{}, invalidRecord()
	}
	currentRevision := int64(0)
	if snapshot.Desired != nil {
		current := snapshot.Desired
		if current.Revision <= 0 || current.Revision > snapshot.EpochRevision ||
			current.Record.EnvironmentID != snapshot.EnvironmentID || !validDesiredPlan(current.Record) {
			return MutationPlan{}, invalidRecord()
		}
		if current.Record.ParentTaskID == snapshot.HeadTaskID && current.Record.Complete {
			return MutationPlan{}, errs.New(errs.KindStateConflict, "Blueprint unit plan is already sealed")
		}
		currentRevision = current.Revision
	}
	if err := validateInitialAbsenceAuthority(snapshot, next); err != nil {
		return MutationPlan{}, err
	}
	epochValue, err := EncodeEpoch(EpochRecord{
		EnvironmentID: snapshot.EnvironmentID, Sequence: snapshot.Epoch.Sequence + 1,
	})
	if err != nil {
		return MutationPlan{}, err
	}
	desiredValue, err := EncodeDesiredPlan(next)
	if err != nil {
		clear(epochValue)
		return MutationPlan{}, err
	}
	plan := MutationPlan{
		conditions: []etcdstore.Condition{
			{Key: blueprints.EnvironmentBlueprintHeadKey(snapshot.EnvironmentID), ModRevision: snapshot.HeadRevision},
			{Key: EpochKey(snapshot.EnvironmentID), ModRevision: snapshot.EpochRevision},
			{Key: DesiredPlanKey(snapshot.EnvironmentID), ModRevision: currentRevision},
		},
		mutations: []etcdstore.Mutation{
			{Type: etcdstore.MutationPut, Key: EpochKey(snapshot.EnvironmentID), Value: epochValue},
			{Type: etcdstore.MutationPut, Key: DesiredPlanKey(snapshot.EnvironmentID), Value: desiredValue},
		},
	}
	return plan, nil
}
