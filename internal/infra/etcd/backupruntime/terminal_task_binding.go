package backupruntime

import (
	"encoding/json"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// ValidateTerminalTaskBinding compares the complete persisted terminal evidence,
// including nullable assignment/timestamps, without consulting current state.
func ValidateTerminalTaskBinding(expected, actual BackupTerminalTaskEvidence) error {
	left, err := json.Marshal(expected)
	if err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	right, err := json.Marshal(actual)
	if err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	if string(left) != string(right) {
		return errs.New(errs.KindStateConflict, "backup terminal receipt Task binding is invalid")
	}
	return nil
}
