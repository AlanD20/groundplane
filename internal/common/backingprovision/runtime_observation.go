package backingprovision

import (
	"bytes"
	"encoding/json"

	"github.com/AlanD20/groundplane/internal/common/backingruntimefact"
	"github.com/AlanD20/groundplane/internal/common/jcs"
)

const MaximumRuntimeObservationBytes = 32 * 1024

func EncodeRuntimeObservation(observation backingruntimefact.Observation) ([]byte, error) {
	if err := backingruntimefact.ValidateObservation(observation); err != nil {
		return nil, err
	}
	raw, err := json.Marshal(observation)
	if err != nil || len(raw) > MaximumRuntimeObservationBytes {
		return nil, invalid("backing creation runtime observation exceeds its bounded format")
	}
	return jcs.Canonicalize(raw)
}

func DecodeRuntimeObservation(raw []byte) (backingruntimefact.Observation, error) {
	if len(raw) == 0 || len(raw) > MaximumRuntimeObservationBytes {
		return backingruntimefact.Observation{}, invalid("backing creation runtime observation is missing or oversized")
	}
	observation, err := jcs.Decode[backingruntimefact.Observation](raw)
	if err != nil {
		return backingruntimefact.Observation{}, invalid("backing creation runtime observation cannot be decoded")
	}
	canonical, err := EncodeRuntimeObservation(observation)
	if err != nil || !bytes.Equal(canonical, raw) {
		return backingruntimefact.Observation{}, invalid("backing creation runtime observation differs from its canonical evidence")
	}
	return observation, nil
}
