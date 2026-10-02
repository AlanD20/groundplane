package postgres16helper

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/postgres16protocol"
)

const gateStatusSize = 79

func readGateStatus(
	reader io.Reader, pid int, launch privateGateLaunch,
	intentSHA256 postgres16protocol.Digest, parent postgres16protocol.ConfinementProcessIdentity,
) (postgres16protocol.ConfinementGateStatus, postgres16protocol.Digest, error) {
	var frame [gateStatusSize]byte
	if _, err := io.ReadFull(reader, frame[:]); err != nil {
		return postgres16protocol.ConfinementGateStatus{}, postgres16protocol.Digest{}, privateWireError()
	}
	if !bytes.Equal(frame[:8], []byte("GPPG16S1")) ||
		!bytes.Equal(frame[15:47], launch.Intent.Nonce[:]) ||
		!bytes.Equal(frame[47:79], intentSHA256[:]) {
		return postgres16protocol.ConfinementGateStatus{}, postgres16protocol.Digest{}, privateWireError()
	}
	status := postgres16protocol.ConfinementGateStatus{
		Schema: 1, Sequence: uint32(frame[8]), Kind: postgres16protocol.ConfinementGateStatusKind(frame[9]),
		Nonce: launch.Intent.Nonce, IntentSHA256: intentSHA256,
		FatalStage: postgres16protocol.ConfinementFatalStage(frame[10]),
		FatalErrno: binary.BigEndian.Uint32(frame[11:15]),
	}
	switch status.Kind {
	case postgres16protocol.ConfinementGateStatusReady:
		identity, err := inspectProcess(pid, launch.Intent.GateFile)
		if err != nil {
			return postgres16protocol.ConfinementGateStatus{}, postgres16protocol.Digest{}, err
		}
		status.Ready = &identity
	case postgres16protocol.ConfinementGateStatusProfileApplied:
		identity, err := inspectProcess(pid, launch.Intent.GateFile)
		if err != nil {
			return postgres16protocol.ConfinementGateStatus{}, postgres16protocol.Digest{}, err
		}
		lastCapability, err := readLastCapability()
		if err != nil {
			return postgres16protocol.ConfinementGateStatus{}, postgres16protocol.Digest{}, err
		}
		status.Profile = &postgres16protocol.ConfinementSecurityProfile{
			RealUID: identity.RealUID, EffectiveUID: identity.EffectiveUID,
			SavedUID: identity.SavedUID, FilesystemUID: identity.FilesystemUID,
			RealGID: identity.RealGID, EffectiveGID: identity.EffectiveGID,
			SavedGID: identity.SavedGID, FilesystemGID: identity.FilesystemGID,
			SupplementaryGIDs: identity.SupplementaryGIDs, LastCapability: lastCapability,
			CapabilityInh: identity.CapabilityInh, CapabilityPrm: identity.CapabilityPrm,
			CapabilityEff: identity.CapabilityEff, CapabilityBnd: identity.CapabilityBnd,
			CapabilityAmb: identity.CapabilityAmb, NoNewPrivileges: identity.NoNewPrivileges,
			SeccompSHA256:      launch.Intent.GateSeccompSHA256,
			ForkSyscallsDenied: identity.SeccompMode == 2,
		}
		status.FDSetSHA256 = identity.FDSetSHA256
	}
	if err := status.Validate(launch.Intent, launch.Authority, intentSHA256, parent); err != nil {
		return postgres16protocol.ConfinementGateStatus{}, postgres16protocol.Digest{}, err
	}
	return status, postgres16protocol.Digest(sha256.Sum256(frame[:])), nil
}

func readLastCapability() (uint32, error) {
	data, err := os.ReadFile("/proc/sys/kernel/cap_last_cap")
	if err != nil || len(data) > 32 {
		return 0, supervisorError()
	}
	value, err := strconv.ParseUint(strings.TrimSpace(string(data)), 10, 32)
	if err != nil || value > 63 {
		return 0, supervisorError()
	}
	return uint32(value), nil
}
