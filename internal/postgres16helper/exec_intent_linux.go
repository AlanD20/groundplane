package postgres16helper

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"io"
	"os"

	"github.com/AlanD20/groundplane/internal/common/postgres16protocol"
	"golang.org/x/sys/unix"
)

const execIntentSize = 253

type execIntent struct {
	Nonce              postgres16protocol.Nonce
	IntentSHA256       postgres16protocol.Digest
	ProfileFrameSHA256 postgres16protocol.Digest
	PID                uint32
	StartTicks         uint64
	BootID             postgres16protocol.ConfinementBootID
	Operation          postgres16protocol.Operation
	SourceSize         uint64
	SourceSHA256       postgres16protocol.Digest
	DeadlineUnixNano   uint64
	JournalSequence    uint64
	RequestSHA256      postgres16protocol.Digest
}

func execIntentName(nonce postgres16protocol.Nonce) string {
	return nonce.String() + ".exec"
}

// storeExecIntent is the final durability fence before releasing the gate to
// execveat. Its presence spends a RestoreApply attempt even when the release
// byte, exec, stream, or terminal result subsequently becomes unknown.
func storeExecIntent(
	directory *os.File, request postgres16protocol.Request,
	state postgres16protocol.ConfinementStateShape, process processRecord,
) error {
	if request.Validate() != nil || !request.Operation.ValidRun() ||
		state.Phase != postgres16protocol.ConfinementPhaseProfileApplied ||
		state.LaunchIntentSHA256 != process.IntentSHA256 || state.ProfileFrameSHA256 == (postgres16protocol.Digest{}) ||
		process.PID == 0 || request.Nonce == (postgres16protocol.Nonce{}) {
		return supervisorError()
	}
	intent := execIntent{
		Nonce: request.Nonce, IntentSHA256: state.LaunchIntentSHA256,
		ProfileFrameSHA256: state.ProfileFrameSHA256,
		PID:                process.PID, StartTicks: process.StartTicks, BootID: process.BootID,
		Operation: request.Operation, SourceSize: request.SourceSize,
		SourceSHA256: request.SourceSHA256, DeadlineUnixNano: request.DeadlineUnixNano,
		JournalSequence: state.Sequence,
	}
	requestSHA256, err := postgres16protocol.ExecutionRequestSHA256(request)
	if err != nil {
		return err
	}
	intent.RequestSHA256 = requestSHA256
	encoded := encodeExecIntent(intent)
	fd, err := unix.Openat2(int(directory.Fd()), execIntentName(request.Nonce), &unix.OpenHow{
		Flags:   unix.O_WRONLY | unix.O_CREAT | unix.O_EXCL | unix.O_CLOEXEC | unix.O_NOFOLLOW,
		Mode:    uint64(postgres16protocol.StateFileMode),
		Resolve: unix.RESOLVE_BENEATH | unix.RESOLVE_NO_SYMLINKS | unix.RESOLVE_NO_MAGICLINKS,
	})
	if err != nil {
		return supervisorError()
	}
	file := os.NewFile(uintptr(fd), execIntentName(request.Nonce))
	defer file.Close()
	if writeAll(file, encoded[:]) != nil || file.Sync() != nil || unix.Fsync(int(directory.Fd())) != nil {
		return supervisorError()
	}
	confirmed, err := loadExecIntent(directory, request.Nonce)
	if err != nil || confirmed != intent {
		return supervisorError()
	}
	return nil
}

func loadExecIntent(directory *os.File, nonce postgres16protocol.Nonce) (execIntent, error) {
	fd, err := unix.Openat2(int(directory.Fd()), execIntentName(nonce), &unix.OpenHow{
		Flags:   unix.O_RDONLY | unix.O_CLOEXEC | unix.O_NOFOLLOW,
		Resolve: unix.RESOLVE_BENEATH | unix.RESOLVE_NO_SYMLINKS | unix.RESOLVE_NO_MAGICLINKS,
	})
	if err != nil {
		return execIntent{}, supervisorError()
	}
	file := os.NewFile(uintptr(fd), execIntentName(nonce))
	defer file.Close()
	var metadata unix.Stat_t
	if err := unix.Fstat(fd, &metadata); err != nil || metadata.Mode&unix.S_IFMT != unix.S_IFREG ||
		metadata.Mode&0o7777 != postgres16protocol.StateFileMode ||
		metadata.Uid != postgres16protocol.StateFileUID || metadata.Gid != postgres16protocol.StateFileGID ||
		metadata.Nlink != 1 || metadata.Size != execIntentSize {
		return execIntent{}, supervisorError()
	}
	var encoded [execIntentSize]byte
	if _, err := io.ReadFull(file, encoded[:]); err != nil {
		return execIntent{}, supervisorError()
	}
	checksum := sha256.Sum256(encoded[:execIntentSize-32])
	if string(encoded[:8]) != "GPPG16E1" || !bytes.Equal(checksum[:], encoded[execIntentSize-32:]) {
		return execIntent{}, supervisorError()
	}
	var intent execIntent
	copy(intent.Nonce[:], encoded[8:40])
	copy(intent.IntentSHA256[:], encoded[40:72])
	copy(intent.ProfileFrameSHA256[:], encoded[72:104])
	intent.PID = binary.BigEndian.Uint32(encoded[104:108])
	intent.StartTicks = binary.BigEndian.Uint64(encoded[108:116])
	copy(intent.BootID[:], encoded[116:132])
	intent.Operation = postgres16protocol.Operation(encoded[132])
	intent.SourceSize = binary.BigEndian.Uint64(encoded[133:141])
	copy(intent.SourceSHA256[:], encoded[141:173])
	intent.DeadlineUnixNano = binary.BigEndian.Uint64(encoded[173:181])
	intent.JournalSequence = binary.BigEndian.Uint64(encoded[181:189])
	copy(intent.RequestSHA256[:], encoded[189:221])
	if intent.Nonce != nonce || !intent.Operation.ValidRun() || intent.PID == 0 || intent.StartTicks == 0 ||
		intent.BootID == (postgres16protocol.ConfinementBootID{}) ||
		intent.IntentSHA256 == (postgres16protocol.Digest{}) ||
		intent.ProfileFrameSHA256 == (postgres16protocol.Digest{}) ||
		intent.DeadlineUnixNano == 0 || intent.JournalSequence == 0 || intent.RequestSHA256 == (postgres16protocol.Digest{}) {
		return execIntent{}, supervisorError()
	}
	return intent, nil
}

func encodeExecIntent(intent execIntent) [execIntentSize]byte {
	var encoded [execIntentSize]byte
	copy(encoded[:8], "GPPG16E1")
	copy(encoded[8:40], intent.Nonce[:])
	copy(encoded[40:72], intent.IntentSHA256[:])
	copy(encoded[72:104], intent.ProfileFrameSHA256[:])
	binary.BigEndian.PutUint32(encoded[104:108], intent.PID)
	binary.BigEndian.PutUint64(encoded[108:116], intent.StartTicks)
	copy(encoded[116:132], intent.BootID[:])
	encoded[132] = byte(intent.Operation)
	binary.BigEndian.PutUint64(encoded[133:141], intent.SourceSize)
	copy(encoded[141:173], intent.SourceSHA256[:])
	binary.BigEndian.PutUint64(encoded[173:181], intent.DeadlineUnixNano)
	binary.BigEndian.PutUint64(encoded[181:189], intent.JournalSequence)
	copy(encoded[189:221], intent.RequestSHA256[:])
	checksum := sha256.Sum256(encoded[:execIntentSize-32])
	copy(encoded[execIntentSize-32:], checksum[:])
	return encoded
}
