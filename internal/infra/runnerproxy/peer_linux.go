package runnerproxy

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
	"golang.org/x/sys/unix"
)

// PeerAuthority is supplied by the Controller's sealed runtime ownership, never
// by a Docker request or workflow file. Namespace identities refer to the
// rootful listener container, not containers in the private rootless daemon.
type PeerAuthority struct {
	RunnerID     string
	RuntimeEpoch uint64
	BootID       string
	UID          uint32
	GID          uint32
	ContainerID  string
	Cgroup       string
	UIDMap       string
	GIDMap       string
	Namespaces   ProcessNamespaces
}

type NamespaceIdentity struct {
	Device uint64
	Inode  uint64
}

type ProcessNamespaces struct {
	Mount   NamespaceIdentity
	PID     NamespaceIdentity
	User    NamespaceIdentity
	Network NamespaceIdentity
	IPC     NamespaceIdentity
	UTS     NamespaceIdentity
	Cgroup  NamespaceIdentity
}

// Peer keeps a pidfd open for the lifetime of an authenticated Unix connection.
// Validate must also run before each request, including keep-alive requests.
type Peer struct {
	mu          sync.Mutex
	pidfd       int
	pid         int32
	startTime   uint64
	authority   PeerAuthority
	closed      bool
	initialized bool
}

func AuthenticatePeer(ctx context.Context, connection *net.UnixConn, authority PeerAuthority) (*Peer, error) {
	if ctx == nil || connection == nil || !validPeerAuthority(authority) {
		return nil, peerDenied()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	raw, err := connection.SyscallConn()
	if err != nil {
		return nil, peerDenied()
	}
	var credentials *unix.Ucred
	var credentialErr error
	pidfd := -1
	if err := raw.Control(func(fd uintptr) {
		credentials, credentialErr = unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED)
		if credentialErr == nil {
			// Obtain the socket's original peer, not a potentially recycled PID.
			pidfd, credentialErr = unix.GetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_PEERPIDFD)
		}
	}); err != nil || credentialErr != nil || credentials == nil || credentials.Pid <= 0 ||
		credentials.Uid != authority.UID || credentials.Gid != authority.GID {
		if pidfd >= 0 {
			if closeErr := unix.Close(pidfd); closeErr != nil {
				return nil, errs.Wrap(errs.KindInternal, closeErr)
			}
		}
		return nil, peerDenied()
	}
	peer := &Peer{pidfd: pidfd, pid: credentials.Pid, authority: authority, initialized: true}
	if err := peer.Validate(ctx); err != nil {
		if closeErr := peer.Close(); closeErr != nil {
			return nil, errs.Wrap(errs.KindInternal, errors.Join(err, closeErr))
		}
		return nil, err
	}
	return peer, nil
}

func (peer *Peer) Validate(ctx context.Context) error {
	peer.mu.Lock()
	defer peer.mu.Unlock()
	if ctx == nil || peer.closed || !peer.initialized {
		return peerDenied()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if !peer.alive() {
		return peerDenied()
	}
	bootID, err := readProcessFile("/proc/sys/kernel/random/boot_id")
	if err != nil || strings.TrimSpace(bootID) != peer.authority.BootID {
		return peerDenied()
	}
	root := "/proc/" + strconv.FormatInt(int64(peer.pid), 10) + "/"
	before, err := processStart(root)
	if err != nil || peer.startTime != 0 && before != peer.startTime {
		return peerDenied()
	}
	status, err := readProcessFile(root + "status")
	if err != nil || !confinedProcessStatus(status, peer.authority.UID, peer.authority.GID) {
		return peerDenied()
	}
	cgroup, err := readProcessFile(root + "cgroup")
	if err != nil || cgroup != "0::"+peer.authority.Cgroup+"\n" {
		return peerDenied()
	}
	uidMap, err := readProcessFile(root + "uid_map")
	if err != nil || uidMap != peer.authority.UIDMap {
		return peerDenied()
	}
	gidMap, err := readProcessFile(root + "gid_map")
	if err != nil || gidMap != peer.authority.GIDMap {
		return peerDenied()
	}
	namespaces, err := processNamespaces(root)
	if err != nil || namespaces != peer.authority.Namespaces {
		return peerDenied()
	}
	after, err := processStart(root)
	if err != nil || before != after || !peer.alive() {
		return peerDenied()
	}
	peer.startTime = before
	return ctx.Err()
}

func (peer *Peer) alive() bool {
	polls := []unix.PollFd{{Fd: int32(peer.pidfd), Events: unix.POLLIN}}
	count, err := unix.Poll(polls, 0)
	return err == nil && count == 0 && polls[0].Revents == 0
}

func (peer *Peer) Close() error {
	peer.mu.Lock()
	defer peer.mu.Unlock()
	if peer.closed || !peer.initialized {
		return nil
	}
	peer.closed = true
	if err := unix.Close(peer.pidfd); err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	return nil
}

func validPeerAuthority(authority PeerAuthority) bool {
	if ids.Validate(ids.KindRunner, authority.RunnerID) != nil || authority.RuntimeEpoch == 0 ||
		len(authority.BootID) != 36 ||
		authority.UID == 0 || authority.GID == 0 || len(authority.ContainerID) != 64 {
		return false
	}
	for _, character := range authority.ContainerID {
		if character < '0' || character > '9' && character < 'a' || character > 'f' {
			return false
		}
	}
	if authority.Cgroup != "/system.slice/docker-"+authority.ContainerID+".scope" ||
		authority.UIDMap == "" || authority.GIDMap == "" || len(authority.UIDMap) > 4096 || len(authority.GIDMap) > 4096 {
		return false
	}
	for _, identity := range []NamespaceIdentity{
		authority.Namespaces.Mount, authority.Namespaces.PID, authority.Namespaces.User,
		authority.Namespaces.Network, authority.Namespaces.IPC, authority.Namespaces.UTS, authority.Namespaces.Cgroup,
	} {
		if identity.Device == 0 || identity.Inode == 0 {
			return false
		}
	}
	return true
}

func readProcessFile(name string) (string, error) {
	file, err := os.Open(name)
	if err != nil {
		return "", peerDenied()
	}
	defer file.Close()
	value, err := io.ReadAll(io.LimitReader(file, 16385))
	if err != nil || len(value) > 16384 {
		return "", peerDenied()
	}
	return string(value), nil
}

func processStart(root string) (uint64, error) {
	value, err := readProcessFile(root + "stat")
	if err != nil {
		return 0, err
	}
	end := strings.LastIndex(value, ") ")
	if end < 0 {
		return 0, peerDenied()
	}
	fields := strings.Fields(value[end+2:])
	if len(fields) < 20 {
		return 0, peerDenied()
	}
	start, err := strconv.ParseUint(fields[19], 10, 64)
	if err != nil || start == 0 {
		return 0, peerDenied()
	}
	return start, nil
}

func confinedProcessStatus(value string, uid, gid uint32) bool {
	wantUID := strconv.FormatUint(uint64(uid), 10)
	wantGID := strconv.FormatUint(uint64(gid), 10)
	seen := make(map[string]bool, 7)
	for _, line := range strings.Split(value, "\n") {
		name, content, found := strings.Cut(line, ":")
		if !found {
			continue
		}
		var expected []string
		switch name {
		case "Uid":
			expected = []string{wantUID, wantUID, wantUID, wantUID}
		case "Gid":
			expected = []string{wantGID, wantGID, wantGID, wantGID}
		case "NoNewPrivs":
			expected = []string{"1"}
		case "Seccomp":
			expected = []string{"2"}
		case "CapEff", "CapPrm", "CapBnd":
			expected = []string{"0000000000000000"}
		default:
			continue
		}
		actual := strings.Fields(content)
		if seen[name] || len(actual) != len(expected) {
			return false
		}
		for index := range expected {
			if actual[index] != expected[index] {
				return false
			}
		}
		seen[name] = true
	}
	return len(seen) == 7
}

func processNamespaces(root string) (ProcessNamespaces, error) {
	var result ProcessNamespaces
	for _, namespace := range []struct {
		name   string
		target *NamespaceIdentity
	}{
		{"mnt", &result.Mount}, {"pid", &result.PID}, {"user", &result.User},
		{"net", &result.Network}, {"ipc", &result.IPC}, {"uts", &result.UTS}, {"cgroup", &result.Cgroup},
	} {
		var stat unix.Stat_t
		if err := unix.Stat(root+"ns/"+namespace.name, &stat); err != nil {
			return ProcessNamespaces{}, peerDenied()
		}
		*namespace.target = NamespaceIdentity{Device: uint64(stat.Dev), Inode: stat.Ino}
	}
	return result, nil
}

func peerDenied() error {
	return errs.New(errs.KindScopeUnauthorized, "Runner process does not match its runtime authority")
}
