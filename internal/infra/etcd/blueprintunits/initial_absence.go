package blueprintunits

import (
	"context"
	"slices"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	maximumInitialAbsenceBytes   = 1 << 20
	maximumInitialAbsenceTargets = maximumDesiredUnits * 5
)

type initialAbsenceRecord struct {
	EnvironmentID   string        `json:"environment_id"`
	BirthRevisionID string        `json:"birth_revision_id"`
	Targets         []ResourceKey `json:"targets"`
}

func initialAbsencePrefix(environmentID string) string {
	return recordPrefix + environmentID + "/initial-absence/"
}

func initialAbsenceKey(environmentID, birthRevisionID string) string {
	return initialAbsencePrefix(environmentID) + birthRevisionID
}

func encodeInitialAbsence(record initialAbsenceRecord) ([]byte, error) {
	if !validInitialAbsence(record) {
		return nil, invalidRecord()
	}
	value, err := recordcodec.Encode("blueprint-unit-initial-absence", record)
	if err != nil {
		return nil, err
	}
	if len(value) > maximumInitialAbsenceBytes {
		clear(value)
		return nil, invalidRecord()
	}
	return value, nil
}

func decodeInitialAbsence(value []byte) (initialAbsenceRecord, error) {
	if len(value) > maximumInitialAbsenceBytes {
		return initialAbsenceRecord{}, corruptRecord()
	}
	record, err := recordcodec.Decode[initialAbsenceRecord](value, "blueprint-unit-initial-absence")
	if err != nil || !validInitialAbsence(record) {
		return initialAbsenceRecord{}, corruptRecord()
	}
	return record, nil
}

func validInitialAbsence(record initialAbsenceRecord) bool {
	if ids.Validate(ids.KindEnvironment, record.EnvironmentID) != nil ||
		ids.Validate(ids.KindTask, record.BirthRevisionID) != nil ||
		len(record.Targets) == 0 || len(record.Targets) > maximumInitialAbsenceTargets || !validKeys(record.Targets) {
		return false
	}
	for _, target := range record.Targets {
		if !supportsInitialAbsence(target.Kind) {
			return false
		}
	}
	return true
}

// InitialAbsencePublication is committed beside the immutable owned-identity
// record and desired head that establish these newborn identities. Its key is
// create-only, so later parents can retain the proof without a missing-key
// inference or a dependency on the first parent's coordinator.
type InitialAbsencePublication struct {
	conditions []etcdstore.Condition
	mutations  []etcdstore.Mutation
}

func (publication InitialAbsencePublication) Conditions() []etcdstore.Condition {
	return slices.Clone(publication.conditions)
}

func (publication InitialAbsencePublication) Mutations() []etcdstore.Mutation {
	return slices.Clone(publication.mutations)
}

func (publication InitialAbsencePublication) Clear() {
	etcdstore.ClearMutationValues(publication.mutations)
}

func PrepareInitialAbsencePublication(
	environmentID, birthRevisionID string, targets []ResourceKey,
) (InitialAbsencePublication, error) {
	if ids.Validate(ids.KindEnvironment, environmentID) != nil ||
		ids.Validate(ids.KindTask, birthRevisionID) != nil {
		return InitialAbsencePublication{}, invalidRecord()
	}
	if len(targets) == 0 {
		return InitialAbsencePublication{}, nil
	}
	targets = slices.Clone(targets)
	slices.SortFunc(targets, compareKey)
	record := initialAbsenceRecord{
		EnvironmentID: environmentID, BirthRevisionID: birthRevisionID, Targets: targets,
	}
	value, err := encodeInitialAbsence(record)
	if err != nil {
		return InitialAbsencePublication{}, err
	}
	key := initialAbsenceKey(environmentID, birthRevisionID)
	return InitialAbsencePublication{
		conditions: []etcdstore.Condition{{Key: key}},
		mutations:  []etcdstore.Mutation{{Type: etcdstore.MutationPut, Key: key, Value: value}},
	}, nil
}

func supportsInitialAbsence(kind ids.Kind) bool {
	switch kind {
	case ids.KindService, ids.KindNetwork, ids.KindVolume, ids.KindEnvEntry, ids.KindRoute,
		ids.KindAttach, ids.KindComponent, ids.KindScript:
		return true
	default:
		return false
	}
}

func (repository *Repository) loadInitialAbsence(ctx context.Context, snapshot *Snapshot) error {
	prefix := initialAbsencePrefix(snapshot.EnvironmentID)
	start := ""
	for {
		page, err := repository.store.Range(ctx, etcdstore.RangeRequest{
			Prefix: prefix, StartExclusive: start, Limit: pageSize, Revision: snapshot.ReadRevision,
		})
		if err != nil {
			return err
		}
		if page == nil || page.ReadRevision != snapshot.ReadRevision || page.More && len(page.Values) == 0 {
			return corruptRecord()
		}
		for _, value := range page.Values {
			if !strings.HasPrefix(value.Key, prefix) || value.ModRevision <= 0 {
				return corruptRecord()
			}
			record, err := decodeInitialAbsence(value.Value)
			if err != nil || record.EnvironmentID != snapshot.EnvironmentID ||
				value.Key != initialAbsenceKey(snapshot.EnvironmentID, record.BirthRevisionID) {
				return corruptRecord()
			}
			snapshot.initialAbsence = append(snapshot.initialAbsence, etcdstore.Versioned[initialAbsenceRecord]{
				Record: record, Revision: value.ModRevision, ReadRevision: snapshot.ReadRevision,
			})
		}
		if !page.More {
			break
		}
		start = page.Values[len(page.Values)-1].Key
	}
	return nil
}

func materializeInitialAbsence(snapshot *Snapshot) error {
	applied := make(map[ResourceKey]bool, len(snapshot.Applied))
	for _, versioned := range snapshot.Applied {
		if applied[versioned.Record.Target] || !hasAppliedEffectSource(versioned.Record) {
			return corruptRecord()
		}
		applied[versioned.Record.Target] = true
	}
	seeded, err := seededTargets(*snapshot)
	if err != nil {
		return corruptRecord()
	}
	for target := range seeded {
		if applied[target] {
			continue
		}
		snapshot.Applied = append(snapshot.Applied, etcdstore.Versioned[AppliedRecord]{
			Record: AppliedRecord{
				EnvironmentID: snapshot.EnvironmentID,
				Target:        target,
				State:         Absent,
			},
			ReadRevision: snapshot.ReadRevision,
		})
	}
	slices.SortFunc(snapshot.Applied, func(left, right etcdstore.Versioned[AppliedRecord]) int {
		return compareKey(left.Record.Target, right.Record.Target)
	})
	return nil
}

func seededTargets(snapshot Snapshot) (map[ResourceKey]bool, error) {
	seeded := make(map[ResourceKey]bool)
	seenBirths := make(map[string]bool, len(snapshot.initialAbsence))
	for _, versioned := range snapshot.initialAbsence {
		if versioned.Revision <= 0 || versioned.ReadRevision != snapshot.ReadRevision ||
			!validInitialAbsence(versioned.Record) ||
			versioned.Record.EnvironmentID != snapshot.EnvironmentID ||
			seenBirths[versioned.Record.BirthRevisionID] {
			return nil, invalidRecord()
		}
		seenBirths[versioned.Record.BirthRevisionID] = true
		for _, target := range versioned.Record.Targets {
			if seeded[target] {
				return nil, invalidRecord()
			}
			seeded[target] = true
		}
	}
	return seeded, nil
}

func validateInitialAbsenceAuthority(snapshot Snapshot, desired DesiredPlan) error {
	seeded, err := seededTargets(snapshot)
	if err != nil {
		return err
	}
	knownApplied := make(map[ResourceKey]bool, len(snapshot.Applied))
	for _, versioned := range snapshot.Applied {
		if versioned.Record.EnvironmentID != snapshot.EnvironmentID ||
			validateApplied(versioned.Record) != nil || versioned.Revision < 0 ||
			(versioned.Revision == 0 && (!seeded[versioned.Record.Target] ||
				versioned.Record.State != Absent || versioned.Record.SourceTaskID != "")) ||
			(versioned.Revision > 0 && !hasAppliedEffectSource(versioned.Record)) ||
			knownApplied[versioned.Record.Target] {
			return invalidRecord()
		}
		knownApplied[versioned.Record.Target] = true
	}
	for _, unit := range desired.Units {
		if !knownApplied[unit.Target] {
			return errs.New(
				errs.KindStateConflict, "Blueprint unit initial absence is unproven",
			)
		}
	}
	return nil
}
