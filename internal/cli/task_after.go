package cli

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/spf13/cobra"
)

func addTaskAfterFlag(command *cobra.Command, after *[]string) {
	command.Flags().
		StringArrayVar(after, "after", nil, "run only after this whole Task succeeds and fully settles (repeatable; maximum 16 Task IDs)")
}

func validatedTaskAfter(values []string) ([]string, error) {
	if len(values) > 16 {
		return nil, errs.New(errs.KindValidationFailed, "Run after accepts at most 16 Task IDs")
	}
	seen := make(map[string]bool, len(values))
	for _, id := range values {
		if ids.Validate(ids.KindTask, id) != nil || seen[id] {
			return nil, errs.New(errs.KindValidationFailed, "Run after requires distinct valid Task IDs")
		}
		seen[id] = true
	}
	return append([]string(nil), values...), nil
}
