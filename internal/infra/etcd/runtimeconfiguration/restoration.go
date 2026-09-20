package runtimeconfiguration

import (
	"encoding/hex"
	"slices"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// FileRestoration retains the original source identity. Execution may allocate
// new Step/materialization ids but must resolve content using this exact record.
type FileRestoration struct {
	ForwardStepID string
	Prior         taskmaterialization.Record
}

// SelectRestorations selects only destinations written by this Task. A nil
// snapshot is explicit initial acknowledgement absence supplied by the caller,
// never permission to infer a missing applied snapshot from desired state.
func SelectRestorations(
	environmentID string, generation uint64, previous *Snapshot, changes []taskmaterialization.Record,
) ([]FileRestoration, error) {
	if ids.Validate(ids.KindEnvironment, environmentID) != nil || generation == 0 || len(changes) > MaximumFiles {
		return nil, errs.New(errs.KindValidationFailed, "configuration restoration scope is invalid")
	}
	prior := make(map[string]taskmaterialization.Record)
	if previous != nil {
		canonical, err := validateAndSortSnapshot(*previous)
		if err != nil || canonical.EnvironmentID != environmentID || canonical.Generation > generation {
			return nil, errs.New(errs.KindStateConflict, "configuration restoration predecessor is inconsistent")
		}
		for _, file := range canonical.Files {
			prior[file.Destination] = file
		}
	}
	result := make([]FileRestoration, 0, len(changes))
	seen := make(map[string]bool, len(changes))
	for _, change := range taskmaterialization.Clone(changes) {
		if change.EnvironmentID != environmentID || taskmaterialization.ValidateRecord(change, generation) != nil ||
			seen[change.Destination] {
			return nil, errs.New(errs.KindStateConflict, "configuration restoration writer is inconsistent")
		}
		seen[change.Destination] = true
		before, exists := prior[change.Destination]
		if !exists {
			before = change
			before.Source = taskmaterialization.Source{Kind: taskmaterialization.SourceRemoval}
			if !strings.HasPrefix(string(before.OutputKind), "remove_") {
				before.OutputKind = taskmaterialization.OutputKind("remove_" + string(before.OutputKind))
			}
			before.Length = 0
			digest := entrymaterialization.DigestBytes(nil)
			before.SHA256 = hex.EncodeToString(digest[:])
		}
		if taskmaterialization.ValidateRecord(before, generation) != nil {
			return nil, errs.New(errs.KindStateConflict, "configuration restoration file is invalid")
		}
		result = append(result, FileRestoration{ForwardStepID: change.StepID, Prior: before})
	}
	slices.SortFunc(
		result,
		func(left, right FileRestoration) int { return strings.Compare(left.ForwardStepID, right.ForwardStepID) },
	)
	return result, nil
}
