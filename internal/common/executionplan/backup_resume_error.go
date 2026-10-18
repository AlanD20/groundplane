package executionplan

import "github.com/AlanD20/groundplane/pkg/errs"

func backupResumeHistoryInvalid() error {
	return errs.New(errs.KindValidationFailed, "Backup source progress or cumulative transition is invalid")
}
