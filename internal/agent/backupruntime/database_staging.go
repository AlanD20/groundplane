package backupruntime

import "github.com/AlanD20/groundplane/pkg/errs"

func invalidDatabaseStaging() error {
	return errs.New(
		errs.KindStateConflict,
		"Database execution differs from its captured source or retained staging evidence",
	)
}
