package executionplan

import (
	"crypto/sha256"
	"encoding/binary"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/oklog/ulid/v2"
	"google.golang.org/protobuf/proto"
)

// BackupVolumeRecoveryKey identifies one source's retained mutation journals.
// The full attempt binds it to the captured input and original host generation;
// a later attempt cannot inherit deletion or execution permission from this key.
func BackupVolumeRecoveryKey(attempt workexecution.Attempt, ordinal uint32, pointID, generation string) ([]byte, error) {
	if ids.Validate(ids.KindRecoveryPoint, pointID) != nil || len(generation) != 26 {
		return nil, invalidBackupStaging()
	}
	if _, err := ulid.ParseStrict(generation); err != nil {
		return nil, invalidBackupStaging()
	}
	wire, err := workexecution.EncodeAgentAttempt(attempt)
	if err != nil {
		return nil, err
	}
	encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(wire)
	if err != nil {
		return nil, invalidBackupStaging()
	}
	hash := sha256.New()
	_, _ = hash.Write([]byte("groundplane.backup.volume-recovery-key.v1\x00"))
	_, _ = hash.Write(encoded)
	var source [4]byte
	binary.BigEndian.PutUint32(source[:], ordinal)
	_, _ = hash.Write(source[:])
	_, _ = hash.Write([]byte("\x00" + pointID + "\x00" + generation))
	return hash.Sum(nil), nil
}
