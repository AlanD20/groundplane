package removalexecution

import (
	"bytes"
	"encoding/json"

	"github.com/AlanD20/groundplane/internal/common/jcs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

const MaximumCaptureBytes = 64 * 1024

func EncodeCapture(input *agentpb.ServiceLifecycleWorkInput, capture RecoveryCapture) ([]byte, error) {
	if err := capture.Validate(input); err != nil {
		return nil, err
	}
	raw, err := json.Marshal(capture)
	if err != nil || len(raw) > MaximumCaptureBytes {
		return nil, invalid()
	}
	return jcs.Canonicalize(raw)
}

func DecodeCapture(input *agentpb.ServiceLifecycleWorkInput, raw []byte) (RecoveryCapture, error) {
	if len(raw) == 0 || len(raw) > MaximumCaptureBytes {
		return RecoveryCapture{}, invalid()
	}
	capture, err := jcs.Decode[RecoveryCapture](raw)
	if err != nil {
		return RecoveryCapture{}, invalid()
	}
	expected, err := EncodeCapture(input, capture)
	if err != nil || !bytes.Equal(expected, raw) {
		return RecoveryCapture{}, invalid()
	}
	return capture, nil
}
