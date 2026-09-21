package attachments

import (
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func ValidateAttachVersion(current etcdstore.Versioned[Record]) error {
	if current.Revision <= 0 {
		return errs.New(errs.KindValidationFailed, "Attach record revision must be positive")
	}
	return ValidateAttachRecord(current.Record)
}
