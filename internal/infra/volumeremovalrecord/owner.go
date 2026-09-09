package volumeremovalrecord

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Owner is the stable Volume-to-operation lookup held through cleanup and
// successor attempts. It is execution ownership, never desired-state authority.
type Owner struct {
	VolumeID      string
	EnvironmentID string
	OperationID   string
}

// OwnerKey permits source acquisition to exclude retirement by stable Volume
// identity, without scanning operation records or depending on a Task attempt.
func OwnerKey(volumeID string) string {
	return "/v1/runtime/volume-removal-owners/" + encodeSegment(volumeID)
}

func validateOwner(owner Owner) error {
	if ids.Validate(ids.KindVolume, owner.VolumeID) != nil ||
		ids.Validate(ids.KindEnvironment, owner.EnvironmentID) != nil ||
		ids.Validate(ids.KindOperation, owner.OperationID) != nil {
		return errs.New(errs.KindValidationFailed, "Volume removal owner is invalid")
	}
	return nil
}

func EncodeOwner(owner Owner) ([]byte, error) {
	if err := validateOwner(owner); err != nil {
		return nil, err
	}
	writer := volumeRemovalWriter{value: []byte("GVRO")}
	writer.uint16(1)
	writer.string(owner.VolumeID)
	writer.string(owner.EnvironmentID)
	writer.string(owner.OperationID)
	return finishVolumeRemovalEncoding(writer)
}

func DecodeOwner(value []byte) (Owner, error) {
	if len(value) > 1024 {
		return Owner{}, Corrupt()
	}
	reader, err := newVolumeRemovalReader(value, "GVRO")
	if err != nil {
		return Owner{}, err
	}
	owner := Owner{VolumeID: reader.string(128), EnvironmentID: reader.string(128), OperationID: reader.string(128)}
	if reader.done() != nil || validateOwner(owner) != nil {
		return Owner{}, Corrupt()
	}
	return owner, nil
}
