package volume

import (
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// cleanupArtifactIdentity gives the captured baseline an identity distinct from
// the candidate while preserving deterministic publication on request replay.
func cleanupArtifactIdentity(candidate string) (string, error) {
	if ids.Validate(ids.KindConfig, candidate) != nil {
		return "", errs.New(errs.KindInternal, "Volume candidate artifact identity is invalid")
	}
	const alphabet = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
	index := strings.IndexByte(alphabet, candidate[len(candidate)-1])
	if index < 0 {
		return "", errs.New(errs.KindInternal, "Volume candidate artifact identity is invalid")
	}
	variant := []byte(candidate)
	variant[len(variant)-1] = alphabet[(index+1)%len(alphabet)]
	return string(variant), nil
}
