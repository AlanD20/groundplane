package materializationproof

import (
	"bytes"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/binary"
	"encoding/json"
	"io"

	coreproof "github.com/AlanD20/groundplane/internal/core/materializationproof"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	codecMagic               = "GPM1"
	codecHeaderBytes         = len(codecMagic) + 4 + sha256.Size
	maximumProofPayloadBytes = 900 << 10
)

func encodeProof(proof coreproof.Proof) ([]byte, error) {
	payload, err := json.Marshal(proof.Record())
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	defer clear(payload)
	if len(payload) == 0 || len(payload) > maximumProofPayloadBytes {
		return nil, errs.New(errs.KindValidationFailed, "materialization proof record exceeds the storage limit")
	}
	encoded := make([]byte, codecHeaderBytes+len(payload))
	copy(encoded, codecMagic)
	binary.BigEndian.PutUint32(encoded[len(codecMagic):], uint32(len(payload)))
	digest := sha256.Sum256(payload)
	copy(encoded[len(codecMagic)+4:codecHeaderBytes], digest[:])
	copy(encoded[codecHeaderBytes:], payload)
	return encoded, nil
}

func decodeProof(encoded []byte) (coreproof.Proof, error) {
	if len(encoded) < codecHeaderBytes || string(encoded[:len(codecMagic)]) != codecMagic {
		return coreproof.Proof{}, corruptProof()
	}
	length := binary.BigEndian.Uint32(encoded[len(codecMagic):])
	if length == 0 || length > maximumProofPayloadBytes || int(length) != len(encoded)-codecHeaderBytes {
		return coreproof.Proof{}, corruptProof()
	}
	payload := encoded[codecHeaderBytes:]
	digest := sha256.Sum256(payload)
	if subtle.ConstantTimeCompare(digest[:], encoded[len(codecMagic)+4:codecHeaderBytes]) != 1 {
		return coreproof.Proof{}, corruptProof()
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	var record coreproof.Record
	if err := decoder.Decode(&record); err != nil {
		return coreproof.Proof{}, corruptProof()
	}
	var trailing struct{}
	if err := decoder.Decode(&trailing); err != io.EOF {
		return coreproof.Proof{}, corruptProof()
	}
	proof, err := coreproof.Restore(record)
	if err != nil {
		return coreproof.Proof{}, corruptProof()
	}
	canonical, err := json.Marshal(proof.Record())
	if err != nil {
		return coreproof.Proof{}, errs.Wrap(errs.KindInternal, err)
	}
	defer clear(canonical)
	if !bytes.Equal(canonical, payload) {
		return coreproof.Proof{}, corruptProof()
	}
	return proof, nil
}

func corruptProof() error {
	return errs.New(errs.KindInternal, "materialization proof record is corrupt")
}
