//go:build linux

// Package backupstage owns the Agent's descriptor-relative transient Backup staging boundary.
package backupstage

import (
	"crypto/sha256"
	"golang.org/x/sys/unix"
	"hash"
	"os"
	"sync"
	"time"
)

const (
	directoryMode         = uint32(0o700)
	fileMode              = uint32(0o600)
	reservationDirName    = ".backupstage-reservations"
	growthMarkerName      = ".exclusive-growth"
	ownerMarkerName       = ".backupstage-agent-owner"
	reservationValueLen   = 16
	defaultCleanupTimeout = 5 * time.Second
)

var rootResolvePolicy = uint64(
	unix.RESOLVE_BENEATH | unix.RESOLVE_NO_SYMLINKS | unix.RESOLVE_NO_MAGICLINKS,
)

var descendantResolvePolicy = uint64(
	unix.RESOLVE_BENEATH | unix.RESOLVE_NO_SYMLINKS | unix.RESOLVE_NO_MAGICLINKS | unix.RESOLVE_NO_XDEV,
)

type linuxOperations struct {
	openat2                  func(int, string, *unix.OpenHow) (int, error)
	renameat2                func(int, string, int, string, uint) error
	flock                    func(int, int) error
	fallocate                func(int, uint32, int64, int64) error
	fstatfs                  func(int, *unix.Statfs_t) error
	pwrite                   func(int, []byte, int64) (int, error)
	fdatasync                func(int) error
	write                    func(int, []byte) (int, error)
	fsync                    func(int) error
	beforeReserve            func()
	beforeReservationRelease func()
	cleanupTimeout           time.Duration
}

func defaultLinuxOperations() linuxOperations {
	return linuxOperations{
		openat2: unix.Openat2, renameat2: unix.Renameat2, flock: unix.Flock,
		fallocate: unix.Fallocate,
		fstatfs:   unix.Fstatfs, pwrite: unix.Pwrite, fdatasync: unix.Fdatasync,
		write: unix.Write, fsync: unix.Fsync,
		cleanupTimeout: defaultCleanupTimeout,
	}
}

// Config selects the pre-created, root-owned Agent task-stage root.
type Config struct{ Root string }

// IDs are the stable path components for one task step and recovery point.
type IDs struct{ Task, Step, Point string }

// CapacityMode is one of the two supported staging admission policies.
type CapacityMode struct {
	kind          capacityKind
	requiredBytes uint64
}

type capacityKind uint8

const (
	capacityBounded capacityKind = iota + 1
	capacityExclusiveUnknown
	capacityNoGrowth
)

// Bounded reserves a caller-computed conservative byte bound.
func Bounded(requiredBytes uint64) CapacityMode {
	return CapacityMode{kind: capacityBounded, requiredBytes: requiredBytes}
}

// ExclusiveUnknown admits one unknown-size growing stage on the filesystem.
func ExclusiveUnknown() CapacityMode {
	return CapacityMode{kind: capacityExclusiveUnknown}
}

// NoGrowth retains sealed recovered files without admitting another sink.
// It is accepted only by ResumePrepared, never by Prepare.
func NoGrowth() CapacityMode {
	return CapacityMode{kind: capacityNoGrowth}
}

// RecoveryID identifies one boot-inventory entry.
type RecoveryID string

// ArtifactEvidence binds a retained final name to its verified bytes.
type ArtifactEvidence struct {
	Name   string
	Size   uint64
	SHA256 [sha256.Size]byte
}

// ReservationState describes untrusted crash-era accounting metadata.
type ReservationState string

const (
	ReservationUntrusted ReservationState = "untrusted"
)

// Recovered is immutable startup inventory sent to Controller authority.
type Recovered struct {
	RecoveryID       RecoveryID
	IDs              IDs
	Files            []ArtifactEvidence
	ReservationState ReservationState
}

// ResumeDisposition is one Controller-persisted recovery decision.
type ResumeDisposition struct {
	RecoveryID      RecoveryID
	DispositionID   string
	ExpectedFiles   []ArtifactEvidence
	RemainingGrowth CapacityMode
}

// PreparedArtifact retains one verified recovered final descriptor.
type PreparedArtifact struct {
	Evidence ArtifactEvidence
	Artifact *Artifact
}

// PreparedStage is a resumed stage returned when recovery becomes Ready.
type PreparedStage struct {
	RecoveryID    RecoveryID
	DispositionID string
	IDs           IDs
	Stage         *Stage
	Files         []PreparedArtifact
}

// Stager anchors one configured root. leases is only an in-process fast path;
// the point-directory flock remains authoritative across Stagers and processes.
type Stager struct {
	mu            sync.Mutex
	reservationMu sync.Mutex
	rootFD        int
	ownerFD       int
	leases        map[IDs]*Stage
	ops           linuxOperations
}

// RecoverySession is the only pre-Ready staging surface.
type RecoverySession struct {
	mu                sync.Mutex
	stager            *Stager
	reservationRootFD int
	recovered         map[RecoveryID]*recoveredStage
	order             []RecoveryID
	prepared          []*PreparedStage
	reserved          uint64
	rootLocked        bool
	closed            bool
	completed         bool
}

type recoveredStage struct {
	entry             Recovered
	rootFD            int
	taskFD            int
	stepFD            int
	pointFD           int
	reservationRootFD int
	reservationTaskFD int
	reservationStepFD int
	reservationFD     int
	files             map[string]*os.File
	resolved          bool
	resumed           bool
	dispositionID     string
	resumeDisposition *ResumeDisposition
}

// Stage retains anchored descriptors, its kernel lease, and reservation until
// Cleanup or Close ends the stage lifetime.
type Stage struct {
	mu                   sync.Mutex
	owner                *Stager
	rootFD               int
	taskFD               int
	stepFD               int
	pointFD              int
	reservationRootFD    int
	reservationTaskFD    int
	reservationStepFD    int
	reservationFD        int
	growthFD             int
	reservationRemaining uint64
	capacity             CapacityMode
	growingFiles         uint64
	poisoned             bool
	ids                  IDs
	artifacts            map[*Artifact]struct{}
	closed               bool
	cleaned              bool
}

// Artifact retains its original O_RDWR descriptor through publication and the
// enclosing Stage lifetime; readers never reopen its pathname.
type Artifact struct {
	stage     *Stage
	file      *os.File
	temporary string
	final     string
	size      int64
	state     artifactState
	readers   map[*preadCloser]struct{}
	hasher    hash.Hash
	written   int64
	digest    [sha256.Size]byte
	growing   bool
}

type artifactState uint8

const (
	artifactOpen artifactState = iota + 1
	artifactPartial
	artifactLinked
	artifactPublished
	artifactRemoved
)
