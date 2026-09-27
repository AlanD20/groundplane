package runnerproxy

import (
	"context"
	"strconv"
	"strings"

	"github.com/AlanD20/groundplane/pkg/errs"
	"golang.org/x/sys/unix"
)

// CapturePeerAuthority runs in trusted host control after Docker inspection has
// proved the listener's exact image, labels and configuration. It binds that
// container's init process to a current-boot namespace and confinement receipt.
// It neither discovers a container by name nor grants authority from a request.
func CapturePeerAuthority(
	ctx context.Context,
	runnerID string,
	epoch uint64,
	containerID string,
	pid int32,
	uid, gid uint32,
) (PeerAuthority, error) {
	if ctx == nil || pid <= 0 {
		return PeerAuthority{}, peerDenied()
	}
	if err := ctx.Err(); err != nil {
		return PeerAuthority{}, err
	}
	pidfd, err := unix.PidfdOpen(int(pid), 0)
	if err != nil {
		return PeerAuthority{}, peerDenied()
	}
	peer := &Peer{pidfd: pidfd, pid: pid, initialized: true}
	defer peer.Close()
	root := "/proc/" + strconv.FormatInt(int64(pid), 10) + "/"
	peer.startTime, err = processStart(root)
	if err != nil {
		return PeerAuthority{}, err
	}
	bootID, err := readProcessFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		return PeerAuthority{}, err
	}
	namespaces, err := processNamespaces(root)
	if err != nil {
		return PeerAuthority{}, err
	}
	uidMap, err := readProcessFile(root + "uid_map")
	if err != nil {
		return PeerAuthority{}, err
	}
	gidMap, err := readProcessFile(root + "gid_map")
	if err != nil {
		return PeerAuthority{}, err
	}
	peer.authority = PeerAuthority{
		RunnerID: runnerID, RuntimeEpoch: epoch, BootID: strings.TrimSpace(bootID),
		UID: uid, GID: gid, ContainerID: containerID,
		Cgroup: "/system.slice/docker-" + containerID + ".scope",
		UIDMap: uidMap, GIDMap: gidMap, Namespaces: namespaces,
	}
	if !validPeerAuthority(peer.authority) {
		return PeerAuthority{}, errs.New(errs.KindValidationFailed, "Runner process authority is invalid")
	}
	if err := peer.Validate(ctx); err != nil {
		return PeerAuthority{}, err
	}
	return peer.authority, nil
}
