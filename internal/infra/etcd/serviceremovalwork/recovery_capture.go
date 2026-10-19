package serviceremovalwork

import (
	"github.com/AlanD20/groundplane/internal/common/removalexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func EncodeRecoveryCapture(
	input *agentpb.ServiceLifecycleWorkInput,
	capture removalexecution.RecoveryCapture,
) ([]byte, error) {
	if err := capture.Validate(input); err != nil {
		return nil, err
	}
	value, err := recordcodec.Encode("service_removal_recovery", capture)
	if err != nil {
		return nil, err
	}
	if len(value) > maximumInputBytes {
		clear(value)
		return nil, conflict("Service removal recovery exceeds its bounded capture size")
	}
	return value, nil
}

func DecodeRecoveryCapture(
	input *agentpb.ServiceLifecycleWorkInput,
	value []byte,
) (removalexecution.RecoveryCapture, error) {
	if len(value) > maximumInputBytes {
		return removalexecution.RecoveryCapture{}, conflict("Service removal recovery exceeds its bounded capture size")
	}
	capture, err := recordcodec.Decode[removalexecution.RecoveryCapture](value, "service_removal_recovery")
	if err != nil {
		return removalexecution.RecoveryCapture{}, err
	}
	if err := capture.Validate(input); err != nil {
		return removalexecution.RecoveryCapture{}, err
	}
	return capture, nil
}
