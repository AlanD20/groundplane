package volumeremovalrecord

import (
	"encoding/binary"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Owner protects a native removal's Volume and Environment from acceptance
// through settled effects. It contains no execution or desired-state authority.
type Owner struct {
	VolumeID      string
	EnvironmentID string
	TaskID        string
	WorkItemID    string
}

// OwnerKey permits source acquisition to exclude retirement by stable Volume
// identity, without scanning operation records or depending on a Task attempt.
func OwnerKey(volumeID string) string {
	return "/v1/volume-removal-ownership/volumes/" + volumeID
}

// EnvironmentLockKey serializes desired publication throughout one Volume
// removal action. Its Owner value remains stable across native attempts.
func EnvironmentLockKey(environmentID string) string {
	return "/v1/volume-removal-ownership/environments/" + environmentID
}

func validateOwner(owner Owner) error {
	if ids.Validate(ids.KindVolume, owner.VolumeID) != nil ||
		ids.Validate(ids.KindEnvironment, owner.EnvironmentID) != nil ||
		ids.Validate(ids.KindTask, owner.TaskID) != nil || ids.Validate(ids.KindWorkItem, owner.WorkItemID) != nil {
		return errs.New(errs.KindValidationFailed, "Volume removal owner is invalid")
	}
	return nil
}

func EncodeOwner(owner Owner) ([]byte, error) {
	if err := validateOwner(owner); err != nil {
		return nil, err
	}
	value := []byte("GVRN")
	for _, id := range []string{owner.VolumeID, owner.EnvironmentID, owner.TaskID, owner.WorkItemID} {
		value = binary.BigEndian.AppendUint16(value, uint16(len(id)))
		value = append(value, id...)
	}
	return value, nil
}

func DecodeOwner(value []byte) (Owner, error) {
	if len(value) < 4 || len(value) > 1024 || string(value[:4]) != "GVRN" {
		return Owner{}, corruptOwner()
	}
	value = value[4:]
	owner := Owner{}
	for _, target := range []*string{&owner.VolumeID, &owner.EnvironmentID, &owner.TaskID, &owner.WorkItemID} {
		if len(value) < 2 {
			return Owner{}, corruptOwner()
		}
		length := int(binary.BigEndian.Uint16(value))
		value = value[2:]
		if length > 128 || length > len(value) {
			return Owner{}, corruptOwner()
		}
		*target, value = string(value[:length]), value[length:]
	}
	if len(value) != 0 || validateOwner(owner) != nil {
		return Owner{}, corruptOwner()
	}
	return owner, nil
}

func corruptOwner() error {
	return errs.New(errs.KindInternal, "Volume removal ownership is corrupt")
}
