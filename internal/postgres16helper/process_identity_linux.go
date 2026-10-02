package postgres16helper

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/postgres16protocol"
	"golang.org/x/sys/unix"
)

func inspectProcess(
	pid int, executable postgres16protocol.ConfinementFileIdentity,
) (postgres16protocol.ConfinementProcessIdentity, error) {
	if pid <= 0 {
		return postgres16protocol.ConfinementProcessIdentity{}, supervisorError()
	}
	base := "/proc/" + strconv.Itoa(pid)
	status, err := os.ReadFile(base + "/status")
	if err != nil || len(status) > 32*1024 {
		return postgres16protocol.ConfinementProcessIdentity{}, supervisorError()
	}
	fields := make(map[string]string)
	for _, line := range strings.Split(string(status), "\n") {
		key, value, found := strings.Cut(line, ":")
		if found {
			fields[key] = strings.TrimSpace(value)
		}
	}
	stat, err := os.ReadFile(base + "/stat")
	if err != nil || len(stat) > 4096 {
		return postgres16protocol.ConfinementProcessIdentity{}, supervisorError()
	}
	end := strings.LastIndexByte(string(stat), ')')
	if end < 0 || end+2 >= len(stat) {
		return postgres16protocol.ConfinementProcessIdentity{}, supervisorError()
	}
	statFields := strings.Fields(string(stat[end+2:]))
	if len(statFields) <= 19 {
		return postgres16protocol.ConfinementProcessIdentity{}, supervisorError()
	}
	start, err := strconv.ParseUint(statFields[19], 10, 64)
	if err != nil || start == 0 {
		return postgres16protocol.ConfinementProcessIdentity{}, supervisorError()
	}
	boot, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		return postgres16protocol.ConfinementProcessIdentity{}, supervisorError()
	}
	bootBytes, err := hex.DecodeString(strings.ReplaceAll(strings.TrimSpace(string(boot)), "-", ""))
	if err != nil || len(bootBytes) != 16 {
		return postgres16protocol.ConfinementProcessIdentity{}, supervisorError()
	}
	var bootID postgres16protocol.ConfinementBootID
	copy(bootID[:], bootBytes)
	uids, err := parseProcessIDs(fields["Uid"])
	if err != nil {
		return postgres16protocol.ConfinementProcessIdentity{}, err
	}
	gids, err := parseProcessIDs(fields["Gid"])
	if err != nil {
		return postgres16protocol.ConfinementProcessIdentity{}, err
	}
	groups, err := parseProcessGroups(fields["Groups"])
	if err != nil {
		return postgres16protocol.ConfinementProcessIdentity{}, err
	}
	identity := postgres16protocol.ConfinementProcessIdentity{
		PID: uint32(pid), StartTicks: start, BootID: bootID,
		RealUID: uids[0], EffectiveUID: uids[1], SavedUID: uids[2], FilesystemUID: uids[3],
		RealGID: gids[0], EffectiveGID: gids[1], SavedGID: gids[2], FilesystemGID: gids[3],
		SupplementaryGIDs: groups, Executable: executable,
	}
	if identity.ParentPID, err = parseStatusUint32(fields["PPid"]); err != nil {
		return postgres16protocol.ConfinementProcessIdentity{}, err
	}
	if identity.ThreadCount, err = parseStatusUint32(fields["Threads"]); err != nil {
		return postgres16protocol.ConfinementProcessIdentity{}, err
	}
	if identity.CapabilityInh, err = parseStatusHex(fields["CapInh"]); err != nil {
		return postgres16protocol.ConfinementProcessIdentity{}, err
	}
	if identity.CapabilityPrm, err = parseStatusHex(fields["CapPrm"]); err != nil {
		return postgres16protocol.ConfinementProcessIdentity{}, err
	}
	if identity.CapabilityEff, err = parseStatusHex(fields["CapEff"]); err != nil {
		return postgres16protocol.ConfinementProcessIdentity{}, err
	}
	if identity.CapabilityBnd, err = parseStatusHex(fields["CapBnd"]); err != nil {
		return postgres16protocol.ConfinementProcessIdentity{}, err
	}
	if identity.CapabilityAmb, err = parseStatusHex(fields["CapAmb"]); err != nil {
		return postgres16protocol.ConfinementProcessIdentity{}, err
	}
	noNewPrivileges, err := parseStatusUint32(fields["NoNewPrivs"])
	if err != nil || noNewPrivileges > 1 {
		return postgres16protocol.ConfinementProcessIdentity{}, supervisorError()
	}
	identity.NoNewPrivileges = noNewPrivileges == 1
	seccomp, err := parseStatusUint32(fields["Seccomp"])
	if err != nil || seccomp > 255 {
		return postgres16protocol.ConfinementProcessIdentity{}, supervisorError()
	}
	identity.SeccompMode = uint8(seccomp)
	processGroup, err := unix.Getpgid(pid)
	if err != nil || processGroup <= 0 {
		return postgres16protocol.ConfinementProcessIdentity{}, supervisorError()
	}
	identity.ProcessGroupID = uint32(processGroup)
	var exeStat unix.Stat_t
	if err := unix.Stat(base+"/exe", &exeStat); err != nil {
		return postgres16protocol.ConfinementProcessIdentity{}, supervisorError()
	}
	if uint64(exeStat.Dev) != executable.Device || exeStat.Ino != executable.Inode {
		return postgres16protocol.ConfinementProcessIdentity{}, supervisorError()
	}
	argv, err := os.ReadFile(base + "/cmdline")
	if err != nil || len(argv) == 0 || len(argv) > 32*1024 {
		return postgres16protocol.ConfinementProcessIdentity{}, supervisorError()
	}
	identity.ArgvSHA256 = postgres16protocol.Digest(sha256.Sum256(argv))
	environment, err := os.ReadFile(base + "/environ")
	if err != nil || len(environment) == 0 || len(environment) > 32*1024 {
		return postgres16protocol.ConfinementProcessIdentity{}, supervisorError()
	}
	identity.EnvironmentSHA256 = postgres16protocol.Digest(sha256.Sum256(environment))
	identity.FDSetSHA256, err = processFDSetDigest(pid)
	if err != nil {
		return postgres16protocol.ConfinementProcessIdentity{}, err
	}
	if err := identity.Validate(); err != nil {
		return postgres16protocol.ConfinementProcessIdentity{}, err
	}
	return identity, nil
}

func processFDSetDigest(pid int) (postgres16protocol.Digest, error) {
	entries, err := os.ReadDir("/proc/" + strconv.Itoa(pid) + "/fd")
	if err != nil {
		return postgres16protocol.Digest{}, supervisorError()
	}
	fds := make([]int, 0, len(entries))
	for _, entry := range entries {
		fd, err := strconv.Atoi(entry.Name())
		if err != nil || fd < 0 || fd > 65535 {
			return postgres16protocol.Digest{}, supervisorError()
		}
		fds = append(fds, fd)
	}
	slices.Sort(fds)
	return fdSetDigest(fds), nil
}

func fdSetDigest(fds []int) postgres16protocol.Digest {
	hash := sha256.New()
	_, _ = hash.Write([]byte("groundplane.postgres16.fd-set.v1\x00"))
	for _, fd := range fds {
		_, _ = fmt.Fprintf(hash, "%d\x00", fd)
	}
	var digest postgres16protocol.Digest
	copy(digest[:], hash.Sum(nil))
	return digest
}

func parseProcessIDs(value string) ([4]uint32, error) {
	var result [4]uint32
	parts := strings.Fields(value)
	if len(parts) != len(result) {
		return result, supervisorError()
	}
	for index, part := range parts {
		parsed, err := strconv.ParseUint(part, 10, 32)
		if err != nil {
			return result, supervisorError()
		}
		result[index] = uint32(parsed)
	}
	return result, nil
}

func parseProcessGroups(value string) ([]uint32, error) {
	parts := strings.Fields(value)
	if len(parts) > 64 {
		return nil, supervisorError()
	}
	result := make([]uint32, 0, len(parts))
	for _, part := range parts {
		parsed, err := strconv.ParseUint(part, 10, 32)
		if err != nil {
			return nil, supervisorError()
		}
		result = append(result, uint32(parsed))
	}
	slices.Sort(result)
	for index := 1; index < len(result); index++ {
		if result[index] == result[index-1] {
			return nil, supervisorError()
		}
	}
	return result, nil
}

func parseStatusUint32(value string) (uint32, error) {
	parsed, err := strconv.ParseUint(value, 10, 32)
	if err != nil {
		return 0, supervisorError()
	}
	return uint32(parsed), nil
}

func parseStatusHex(value string) (uint64, error) {
	parsed, err := strconv.ParseUint(value, 16, 64)
	if err != nil {
		return 0, supervisorError()
	}
	return parsed, nil
}

type processLifetimeEvidence struct {
	StartTicks uint64
	BootID     postgres16protocol.ConfinementBootID
}

func processLifetime(pid int) (processLifetimeEvidence, error) {
	if pid <= 0 {
		return processLifetimeEvidence{}, supervisorError()
	}
	stat, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil || len(stat) == 0 || len(stat) > 4096 {
		return processLifetimeEvidence{}, supervisorError()
	}
	end := strings.LastIndexByte(string(stat), ')')
	if end < 0 || end+2 >= len(stat) {
		return processLifetimeEvidence{}, supervisorError()
	}
	fields := strings.Fields(string(stat[end+2:]))
	if len(fields) <= 19 {
		return processLifetimeEvidence{}, supervisorError()
	}
	start, err := strconv.ParseUint(fields[19], 10, 64)
	if err != nil || start == 0 {
		return processLifetimeEvidence{}, supervisorError()
	}
	boot, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		return processLifetimeEvidence{}, supervisorError()
	}
	decoded, err := hex.DecodeString(strings.ReplaceAll(strings.TrimSpace(string(boot)), "-", ""))
	if err != nil || len(decoded) != 16 {
		return processLifetimeEvidence{}, supervisorError()
	}
	var result processLifetimeEvidence
	result.StartTicks = start
	copy(result.BootID[:], decoded)
	return result, nil
}
