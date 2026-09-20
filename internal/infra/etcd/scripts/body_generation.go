package scripts

import (
	"crypto/sha256"
	"encoding/hex"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func NewScriptBodyGeneration(record Record) (BodyGenerationRecord, error) {
	digest := sha256.Sum256([]byte(record.Desired.Body))
	generation := BodyGenerationRecord{
		ScriptID: record.Desired.ID, Generation: record.ActiveGeneration,
		BodySize: uint32(len(record.Desired.Body)), Body: record.Desired.Body,
		BodySHA256: hex.EncodeToString(digest[:]),
	}
	if err := ValidateScriptBodyGeneration(generation); err != nil {
		return BodyGenerationRecord{}, err
	}
	return generation, nil
}

func ValidateScriptBodyGeneration(generation BodyGenerationRecord) error {
	if err := recordcodec.ValidateID(ids.KindScript, generation.ScriptID); err != nil {
		return err
	}
	if generation.Generation == 0 || generation.BodySize != uint32(len(generation.Body)) {
		return errs.New(errs.KindValidationFailed, "Script body generation or size is invalid")
	}
	if err := core.ValidateScriptBody(generation.Body); err != nil {
		return errs.Wrap(errs.KindValidationFailed, err)
	}
	digest := sha256.Sum256([]byte(generation.Body))
	if generation.BodySHA256 != hex.EncodeToString(digest[:]) {
		return errs.New(errs.KindValidationFailed, "Script body digest is invalid")
	}
	return nil
}

func EncodeScriptBodyGeneration(generation BodyGenerationRecord) ([]byte, error) {
	if err := ValidateScriptBodyGeneration(generation); err != nil {
		return nil, err
	}
	return recordcodec.Encode("script_body_generation", generation)
}

func DecodeScriptBodyGeneration(value []byte) (BodyGenerationRecord, error) {
	generation, err := recordcodec.Decode[BodyGenerationRecord](value, "script_body_generation")
	if err != nil {
		return BodyGenerationRecord{}, err
	}
	if err := ValidateScriptBodyGeneration(generation); err != nil {
		return BodyGenerationRecord{}, recordcodec.CorruptRecord()
	}
	return generation, nil
}
