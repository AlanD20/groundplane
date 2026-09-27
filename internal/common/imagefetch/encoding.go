package imagefetch

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"github.com/AlanD20/groundplane/internal/common/jcs"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const TimeoutSeconds = 15 * 60

func Encode(plan Plan) (string, string, error) {
	if err := plan.Validate(); err != nil {
		return "", "", err
	}
	raw, err := json.Marshal(plan)
	if err != nil {
		return "", "", errs.Wrap(errs.KindInternal, err)
	}
	canonical, err := jcs.Canonicalize(raw)
	if err != nil {
		return "", "", err
	}
	digest := sha256.Sum256(canonical)
	return string(canonical), hex.EncodeToString(digest[:]), nil
}

func Decode(input, hash string) (Plan, error) {
	digest := sha256.Sum256([]byte(input))
	if len(input) == 0 || len(input) > 4096 || hex.EncodeToString(digest[:]) != hash {
		return Plan{}, errs.New(errs.KindValidationFailed, "image fetch plan hash is invalid")
	}
	plan, err := jcs.Decode[Plan]([]byte(input))
	if err != nil {
		return Plan{}, err
	}
	if err := plan.Validate(); err != nil {
		return Plan{}, err
	}
	return plan, nil
}
