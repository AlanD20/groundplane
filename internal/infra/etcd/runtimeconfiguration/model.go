// Package runtimeconfiguration retains immutable, non-secret configuration
// source snapshots independently of the Tasks that selected them.
package runtimeconfiguration

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const MaximumFiles = 512

type Snapshot struct {
	ID            string
	EnvironmentID string
	Generation    uint64
	Files         []taskmaterialization.Record
}

type Reference struct {
	ID            string `json:"id"`
	EnvironmentID string `json:"environment_id"`
	Generation    uint64 `json:"generation"`
	SHA256        string `json:"sha256"`
}

// ValidateReference validates the immutable identity carried by an owning
// runtime acknowledgement.
func ValidateReference(reference Reference) error {
	if ids.Validate(ids.KindConfig, reference.ID) != nil ||
		ids.Validate(ids.KindEnvironment, reference.EnvironmentID) != nil ||
		reference.Generation == 0 || !validSHA256(reference.SHA256) {
		return errs.New(errs.KindValidationFailed, "runtime configuration reference is invalid")
	}
	return nil
}

// Merge replaces files by destination while preserving every unchanged
// immutable source reference. Removal records remain ordinary explicit members.
func Merge(
	previous Snapshot,
	changes []taskmaterialization.Record,
	id string,
	generation uint64,
) (Snapshot, error) {
	canonicalPrevious, err := validateAndSortSnapshot(previous)
	if err != nil {
		return Snapshot{}, err
	}
	if ids.Validate(ids.KindConfig, id) != nil || generation == 0 {
		return Snapshot{}, errs.New(
			errs.KindValidationFailed,
			"merged runtime configuration identity is invalid",
		)
	}
	byDestination := make(
		map[string]taskmaterialization.Record,
		len(canonicalPrevious.Files)+len(changes),
	)
	for _, record := range canonicalPrevious.Files {
		if record.EnvironmentID != canonicalPrevious.EnvironmentID ||
			taskmaterialization.ValidateRecord(record, generation) != nil {
			return Snapshot{}, errs.New(
				errs.KindValidationFailed,
				"retained runtime configuration file is invalid",
			)
		}
		byDestination[record.Destination] = record
	}
	changedDestinations := make(map[string]struct{}, len(changes))
	for _, record := range taskmaterialization.Clone(changes) {
		if _, exists := changedDestinations[record.Destination]; exists {
			return Snapshot{}, errs.New(
				errs.KindValidationFailed,
				"runtime configuration changes contain a duplicate destination",
			)
		}
		changedDestinations[record.Destination] = struct{}{}
		if record.EnvironmentID != canonicalPrevious.EnvironmentID ||
			taskmaterialization.ValidateRecord(record, generation) != nil {
			return Snapshot{}, errs.New(
				errs.KindValidationFailed,
				"runtime configuration change is invalid",
			)
		}
		byDestination[record.Destination] = record
	}
	if len(byDestination) > MaximumFiles {
		return Snapshot{}, errs.New(
			errs.KindValidationFailed,
			"runtime configuration snapshot has too many files",
		)
	}
	merged := Snapshot{
		ID: id, EnvironmentID: canonicalPrevious.EnvironmentID, Generation: generation,
		Files: make([]taskmaterialization.Record, 0, len(byDestination)),
	}
	for _, record := range byDestination {
		merged.Files = append(merged.Files, record)
	}
	sort.Slice(merged.Files, func(left, right int) bool {
		return merged.Files[left].Destination < merged.Files[right].Destination
	})
	merged.Files = taskmaterialization.Clone(merged.Files)
	return merged, nil
}

func validateAndSortSnapshot(snapshot Snapshot) (Snapshot, error) {
	if ids.Validate(ids.KindConfig, snapshot.ID) != nil ||
		ids.Validate(ids.KindEnvironment, snapshot.EnvironmentID) != nil ||
		snapshot.Generation == 0 || len(snapshot.Files) > MaximumFiles {
		return Snapshot{}, errs.New(
			errs.KindValidationFailed,
			"runtime configuration snapshot is invalid",
		)
	}
	canonical := Snapshot{
		ID: snapshot.ID, EnvironmentID: snapshot.EnvironmentID, Generation: snapshot.Generation,
		Files: taskmaterialization.Clone(snapshot.Files),
	}
	sort.Slice(canonical.Files, func(left, right int) bool {
		return canonical.Files[left].Destination < canonical.Files[right].Destination
	})
	previousDestination := ""
	for _, record := range canonical.Files {
		if record.EnvironmentID != canonical.EnvironmentID ||
			taskmaterialization.ValidateRecord(record, canonical.Generation) != nil {
			return Snapshot{}, errs.New(
				errs.KindValidationFailed,
				"runtime configuration file is invalid",
			)
		}
		if previousDestination != "" && record.Destination == previousDestination {
			return Snapshot{}, errs.New(
				errs.KindValidationFailed,
				"runtime configuration destinations are not unique",
			)
		}
		previousDestination = record.Destination
	}
	if canonical.Files == nil {
		canonical.Files = []taskmaterialization.Record{}
	}
	return canonical, nil
}

func validSHA256(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size && hex.EncodeToString(decoded) == value
}
