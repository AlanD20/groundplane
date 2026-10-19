package materialization

import (
	"io"

	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
)

type Payload struct {
	Header entrymaterialization.Header
	Source io.ReadCloser
}
