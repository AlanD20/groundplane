package postgres16helper

import (
	"crypto/sha256"
	"encoding/binary"
	"runtime"

	"github.com/AlanD20/groundplane/internal/common/postgres16protocol"
	"golang.org/x/sys/unix"
)

// GateSeccompSHA256 measures the exact classic-BPF instruction sequence in the
// native gate. The release manifest must publish this value for each platform.
// Instruction fields are serialized in network order so the measurement is
// independent of C struct padding and host byte order.
func GateSeccompSHA256() (postgres16protocol.Digest, error) {
	var arch uint32
	switch runtime.GOARCH {
	case "amd64":
		arch = unix.AUDIT_ARCH_X86_64
	case "arm64":
		arch = unix.AUDIT_ARCH_AARCH64
	default:
		return postgres16protocol.Digest{}, supervisorError()
	}
	filter := []unix.SockFilter{
		{Code: unix.BPF_LD | unix.BPF_W | unix.BPF_ABS, K: 4},
		{Code: unix.BPF_JMP | unix.BPF_JEQ | unix.BPF_K, Jt: 1, K: arch},
		{Code: unix.BPF_RET | unix.BPF_K, K: unix.SECCOMP_RET_KILL_PROCESS},
		{Code: unix.BPF_LD | unix.BPF_W | unix.BPF_ABS, K: 0},
		{Code: unix.BPF_JMP | unix.BPF_JEQ | unix.BPF_K, Jf: 1, K: unix.SYS_CLONE},
		{Code: unix.BPF_RET | unix.BPF_K, K: unix.SECCOMP_RET_ERRNO | uint32(unix.EPERM)},
		{Code: unix.BPF_JMP | unix.BPF_JEQ | unix.BPF_K, Jf: 1, K: unix.SYS_CLONE3},
		{Code: unix.BPF_RET | unix.BPF_K, K: unix.SECCOMP_RET_ERRNO | uint32(unix.EPERM)},
	}
	filter = append(filter, gateForkFilters()...)
	filter = append(filter, unix.SockFilter{Code: unix.BPF_RET | unix.BPF_K, K: unix.SECCOMP_RET_ALLOW})
	hash := sha256.New()
	for _, instruction := range filter {
		var encoded [8]byte
		binary.BigEndian.PutUint16(encoded[:2], instruction.Code)
		encoded[2], encoded[3] = instruction.Jt, instruction.Jf
		binary.BigEndian.PutUint32(encoded[4:], instruction.K)
		_, _ = hash.Write(encoded[:])
	}
	var digest postgres16protocol.Digest
	copy(digest[:], hash.Sum(nil))
	return digest, nil
}
