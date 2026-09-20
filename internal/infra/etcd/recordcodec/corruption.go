package recordcodec

import "github.com/AlanD20/groundplane/pkg/errs"

func CorruptRecord() error {
	return errs.New(errs.KindInternal, "durable record violates its repository schema")
}
