package softwarepreparation

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"github.com/AlanD20/groundplane/internal/common/jcs"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const MaximumInputBytes = 64 << 10

func EncodeInput(input Input) (string, string, error) {
	if err := ValidateInput(input); err != nil {
		return "", "", err
	}
	raw, err := json.Marshal(input)
	if err != nil {
		return "", "", errs.Wrap(errs.KindInternal, err)
	}
	canonical, err := jcs.Canonicalize(raw)
	if err != nil {
		return "", "", err
	}
	if len(canonical) == 0 || len(canonical) > MaximumInputBytes {
		return "", "", errs.New(errs.KindValidationFailed, "software preparation input exceeds its bound")
	}
	digest := sha256.Sum256(canonical)
	return string(canonical), hex.EncodeToString(digest[:]), nil
}

func DecodeInput(value, expectedHash string) (Input, error) {
	if len(value) == 0 || len(value) > MaximumInputBytes {
		return Input{}, errs.New(errs.KindValidationFailed, "software preparation input is empty or oversized")
	}
	digest := sha256.Sum256([]byte(value))
	if hex.EncodeToString(digest[:]) != expectedHash {
		return Input{}, errs.New(errs.KindValidationFailed, "software preparation input hash differs")
	}
	input, err := jcs.Decode[Input]([]byte(value))
	if err != nil {
		return Input{}, err
	}
	if err := ValidateInput(input); err != nil {
		return Input{}, err
	}
	return input, nil
}
