package recordcodec

import (
	"github.com/AlanD20/groundplane/pkg/errs"
)

func StateConflict(kind string, id string) error {
	return errs.Newf(errs.KindStateConflict, "%s %s changed", kind, id)
}
