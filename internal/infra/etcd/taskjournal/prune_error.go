package taskjournal

import (
	"github.com/AlanD20/groundplane/pkg/errs"
)

func CorruptPruneIntent() error {
	return errs.New(errs.KindInternal, "task prune state is corrupt")
}
