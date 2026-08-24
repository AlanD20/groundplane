//go:build linux

// Package backupstage owns the Agent's descriptor-relative transient Backup staging boundary.
package backupstage

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/pkg/errs"
	"golang.org/x/sys/unix"
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

// OpenRecovery anchors and exclusively owns the configured root, inventories
// crash state, and returns only the pre-Ready recovery surface.
func OpenRecovery(ctx context.Context, config Config) (*RecoverySession, error) {
	return openRecoveryWithOperations(ctx, config, defaultLinuxOperations())
}

func openRecoveryWithOperations(
	ctx context.Context,
	config Config,
	ops linuxOperations,
) (*RecoverySession, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	if ops.openat2 == nil || ops.renameat2 == nil || ops.flock == nil || ops.fallocate == nil || ops.fstatfs == nil ||
		ops.pwrite == nil || ops.fdatasync == nil || ops.write == nil || ops.fsync == nil || ops.cleanupTimeout <= 0 {
		return nil, internalError("linux staging operations are required")
	}
	root, err := validateRootPath(config.Root)
	if err != nil {
		return nil, err
	}
	rootFD, err := openConfiguredRoot(ctx, root, ops)
	if err != nil {
		return nil, err
	}
	if err := probeRenameat2(ctx, ops); err != nil {
		return nil, closeWithPrimary(ctx, err, rootFD)
	}
	ownerFD, err := openOwnerMarker(ctx, rootFD, ops)
	if err != nil {
		return nil, closeWithPrimary(ctx, err, rootFD)
	}
	if err := ops.flock(ownerFD, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		primary := systemError("acquire staging Agent ownership", err)
		if flockWouldBlock(err) {
			primary = stateConflictError("another Agent owns the staging root")
		}
		return nil, closeWithPrimary(ctx, primary, ownerFD, rootFD)
	}
	stager := &Stager{rootFD: rootFD, ownerFD: ownerFD, leases: make(map[IDs]*Stage), ops: ops}
	if err := lockWithContext(ctx, rootFD, ops); err != nil {
		return nil, closeStagerWithPrimary(ctx, stager, err)
	}
	reservationRootFD, err := openOrCreateDirectory(ctx, rootFD, reservationDirName, ops)
	if err != nil {
		return nil, closeStagerWithPrimary(ctx, stager,
			unlockWithPrimary(ctx, err, rootFD, ops))
	}
	if err := probeGrowthMarker(ctx, reservationRootFD, ops); err != nil {
		primary := closeWithPrimary(ctx, err, reservationRootFD)
		return nil, closeStagerWithPrimary(ctx, stager,
			unlockWithPrimary(ctx, primary, rootFD, ops))
	}
	session := &RecoverySession{
		stager: stager, reservationRootFD: reservationRootFD,
		recovered: make(map[RecoveryID]*recoveredStage), rootLocked: true,
	}
	if err := session.inventoryLocked(ctx); err != nil {
		return nil, closeRecoveryWithPrimary(ctx, session, err)
	}
	return session, nil
}

// Close releases the lifetime Agent-owner lease and root descriptor only after
// every Stage has released its lease and capacity reservation.
func (stager *Stager) Close(ctx context.Context) error {
	callerErr := contextError(ctx)
	if stager == nil {
		return callerErr
	}
	stager.mu.Lock()
	defer stager.mu.Unlock()
	if stager.rootFD < 0 {
		return callerErr
	}
	if len(stager.leases) != 0 {
		return stateConflictError("stager still has live stages")
	}
	rootFD, ownerFD := stager.rootFD, stager.ownerFD
	stager.rootFD, stager.ownerFD = -1, -1
	unlockErr := rawOperationError("unlock staging Agent ownership", stager.ops.flock(ownerFD, unix.LOCK_UN))
	closeErr := closeFDs(ctx, ownerFD, rootFD)
	if unlockErr != nil {
		return errs.WrapJoined(errs.KindInternal, callerErr, unlockErr, closeErr)
	}
	return joinPrivate(callerErr, closeErr)
}

// Prepare leases one deterministic namespace under exactly one capacity mode.
// Bounded callers supply their source-specific conservative disk bound;
// ExclusiveUnknown serializes filesystem growth and allocates before writing.
func (stager *Stager) Prepare(ctx context.Context, ids IDs, capacity CapacityMode) (*Stage, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	if err := validateIDs(ids); err != nil {
		return nil, err
	}
	if capacity.kind != capacityBounded && capacity.kind != capacityExclusiveUnknown {
		return nil, validationError("capacity mode must be Bounded or ExclusiveUnknown")
	}
	if capacity.kind == capacityBounded &&
		(capacity.requiredBytes == 0 || capacity.requiredBytes > math.MaxInt64) {
		return nil, validationError("bounded required bytes must be between 1 and %d", int64(math.MaxInt64))
	}
	if stager == nil {
		return nil, internalError("stager is required")
	}
	stager.mu.Lock()
	stagerLocked := true
	defer func() {
		if stagerLocked {
			stager.mu.Unlock()
		}
	}()
	if stager.rootFD < 0 {
		return nil, internalError("stager is closed")
	}
	if _, exists := stager.leases[ids]; exists {
		return nil, stateConflictError("stage already has a live lease")
	}
	stager.reservationMu.Lock()
	if err := lockWithContext(ctx, stager.rootFD, stager.ops); err != nil {
		stager.reservationMu.Unlock()
		return nil, err
	}
	rootLocked := true
	releaseRoot := func(primary error) error {
		if !rootLocked {
			return primary
		}
		rootLocked = false
		result := unlockWithPrimary(ctx, primary, stager.rootFD, stager.ops)
		stager.reservationMu.Unlock()
		return result
	}
	taskFD, err := openOrCreateDirectory(ctx, stager.rootFD, ids.Task, stager.ops)
	if err != nil {
		return nil, releaseRoot(err)
	}
	stepFD, err := openOrCreateDirectory(ctx, taskFD, ids.Step, stager.ops)
	if err != nil {
		return nil, releaseRoot(closeWithPrimary(ctx, err, taskFD))
	}
	pointFD, err := openOrCreateDirectory(ctx, stepFD, ids.Point, stager.ops)
	if err != nil {
		return nil, releaseRoot(closeWithPrimary(ctx, err, stepFD, taskFD))
	}
	if err := stager.ops.flock(pointFD, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		primary := systemError("acquire stage kernel lease", err)
		if flockWouldBlock(err) {
			primary = stateConflictError("stage already has a live kernel lease")
		}
		return nil, releaseRoot(closeWithPrimary(ctx, primary, pointFD, stepFD, taskFD))
	}
	if stager.ops.beforeReserve != nil {
		stager.ops.beforeReserve()
	}
	if err := validatePointContents(ctx, pointFD, stager.ops); err != nil {
		return nil, releaseRoot(closeWithPrimary(ctx, err, pointFD, stepFD, taskFD))
	}
	if err := removePointContents(ctx, pointFD, stager.ops); err != nil {
		return nil, releaseRoot(closeWithPrimary(ctx, err, pointFD, stepFD, taskFD))
	}
	rootFD, err := unix.Dup(stager.rootFD)
	if err != nil {
		return nil, releaseRoot(closeWithPrimary(ctx, systemError("duplicate stage root descriptor", err),
			pointFD, stepFD, taskFD))
	}
	unix.CloseOnExec(rootFD)
	stage := &Stage{
		owner: stager, rootFD: rootFD, taskFD: taskFD, stepFD: stepFD, pointFD: pointFD,
		reservationRootFD: -1, reservationTaskFD: -1, reservationStepFD: -1, reservationFD: -1,
		growthFD: -1, ids: ids, capacity: capacity, artifacts: make(map[*Artifact]struct{}),
	}
	cleanupFailedStage := func(primary error) error {
		stager.leases[ids] = stage
		stager.mu.Unlock()
		stagerLocked = false
		return joinPrivate(primary, stage.Cleanup(ctx))
	}
	if err := stage.syncParents(ctx); err != nil {
		return nil, cleanupFailedStage(releaseRoot(err))
	}
	if err := stage.reserveCapacityLocked(ctx); err != nil {
		return nil, cleanupFailedStage(releaseRoot(err))
	}
	if err := releaseRoot(nil); err != nil {
		return nil, cleanupFailedStage(err)
	}
	stager.leases[ids] = stage
	return stage, nil
}

// Close performs the same mandatory destructive teardown as Cleanup.
func (stage *Stage) Close(ctx context.Context) error {
	return stage.Cleanup(ctx)
}

// CreateFile creates one descriptor-relative named partial with O_EXCL and
// retains its O_RDWR descriptor through publication.
func (stage *Stage) CreateFile(ctx context.Context, name string) (*Artifact, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	if stage == nil {
		return nil, internalError("stage is required")
	}
	if err := validateFinalArtifactName(name); err != nil {
		return nil, err
	}
	stage.mu.Lock()
	defer stage.mu.Unlock()
	if stage.closed || stage.cleaned {
		return nil, internalError("stage is closed")
	}
	if stage.capacity.kind == capacityNoGrowth {
		return nil, stateConflictError("recovered stage was resumed without future growth")
	}
	if err := rejectExistingEntry(ctx, stage.pointFD, name); err != nil {
		return nil, err
	}
	temporary := "." + name + ".partial"
	if err := rejectExistingEntry(ctx, stage.pointFD, temporary); err != nil {
		return nil, err
	}
	acquiredGrowth := false
	if stage.capacity.kind == capacityExclusiveUnknown && stage.growthFD < 0 {
		if err := stage.acquireExclusiveGrowth(ctx); err != nil {
			return nil, err
		}
		acquiredGrowth = true
	}
	fd, err := stage.owner.ops.openat2(stage.pointFD, temporary, &unix.OpenHow{
		Flags: uint64(unix.O_RDWR | unix.O_CREAT | unix.O_EXCL | unix.O_CLOEXEC | unix.O_NOFOLLOW),
		Mode:  uint64(fileMode), Resolve: descendantResolvePolicy,
	})
	if err != nil {
		if acquiredGrowth {
			return nil, joinPrivate(storageSystemError("create temporary artifact", err),
				stage.releaseGrowthMarker(ctx))
		}
		return nil, storageSystemError("create temporary artifact", err)
	}
	fail := func(primary error) error {
		closeErr := rawOperationError("close failed temporary artifact", unix.Close(fd))
		unlinkErr := rawOperationError("remove failed temporary artifact",
			unix.Unlinkat(stage.pointFD, temporary, 0))
		syncErr := storageOperationError("sync failed temporary artifact removal", unix.Fsync(stage.pointFD))
		var growthErr error
		if acquiredGrowth {
			growthErr = stage.releaseGrowthMarker(ctx)
		}
		return joinPrivate(primary, closeErr, unlinkErr, syncErr, growthErr)
	}
	if err := unix.Fchown(fd, 0, 0); err != nil {
		return nil, fail(systemError("set temporary artifact ownership", err))
	}
	if err := unix.Fchmod(fd, fileMode); err != nil {
		return nil, fail(systemError("set temporary artifact mode", err))
	}
	if err := validateRegularFD(ctx, fd, 0, 1); err != nil {
		return nil, fail(joinPrivate(internalError("temporary artifact metadata is invalid"), err))
	}
	file := os.NewFile(uintptr(fd), "backup-artifact")
	if file == nil {
		return nil, fail(internalError("wrap temporary artifact descriptor"))
	}
	artifact := &Artifact{
		stage: stage, file: file, temporary: temporary, final: name, state: artifactOpen,
		readers: make(map[*preadCloser]struct{}), hasher: sha256.New(),
		growing: stage.capacity.kind == capacityExclusiveUnknown,
	}
	if artifact.growing {
		stage.growingFiles++
	}
	stage.artifacts[artifact] = struct{}{}
	return artifact, nil
}

func (artifact *Artifact) Write(ctx context.Context, content []byte) (int, error) {
	if err := contextError(ctx); err != nil {
		return 0, err
	}
	if artifact == nil || artifact.stage == nil {
		return 0, internalError("artifact is required")
	}
	stage := artifact.stage
	stage.mu.Lock()
	defer stage.mu.Unlock()
	if artifact.state != artifactOpen || artifact.file == nil || stage.closed || stage.cleaned {
		return 0, internalError("artifact is not open")
	}
	if stage.poisoned {
		return 0, internalError("stage capacity reservation is poisoned")
	}
	if stage.capacity.kind == capacityBounded && uint64(len(content)) > stage.reservationRemaining {
		return 0, internalError("write exceeds caller-supplied required byte bound")
	}
	if int64(len(content)) > math.MaxInt64-artifact.written {
		return 0, artifact.failWriteLocked(ctx, internalError("staged artifact byte count overflow"))
	}
	if stage.capacity.kind == capacityExclusiveUnknown && len(content) != 0 {
		if err := stage.owner.ops.fallocate(
			int(artifact.file.Fd()), unix.FALLOC_FL_KEEP_SIZE, artifact.written, int64(len(content)),
		); err != nil {
			return 0, artifact.failWriteLocked(ctx, fallocateError("allocate staged artifact range", err))
		}
		if err := contextError(ctx); err != nil {
			return 0, artifact.failWriteLocked(ctx, err)
		}
	}
	written, writeErr := stage.owner.ops.write(int(artifact.file.Fd()), content)
	if written < 0 || written > len(content) {
		return 0, artifact.failWriteLocked(ctx, internalError("staged artifact write returned an invalid count"))
	}
	if written != 0 {
		hashed, hashErr := artifact.hasher.Write(content[:written])
		if hashErr != nil || hashed != written {
			return written, artifact.failWriteLocked(ctx, internalError("staged artifact hash count mismatch"))
		}
		artifact.written += int64(written)
	}
	if stage.capacity.kind == capacityExclusiveUnknown {
		if err := contextError(ctx); err != nil {
			return written, artifact.failWriteLocked(ctx, err)
		}
	}
	var reservationErr error
	if stage.capacity.kind == capacityBounded {
		reservationErr = stage.consumeReservation(ctx, uint64(written))
	}
	if writeErr != nil {
		return written, artifact.failWriteLocked(ctx,
			joinPrivate(storageSystemError("write staged artifact", writeErr), reservationErr))
	}
	if written != len(content) {
		return written, artifact.failWriteLocked(ctx,
			joinPrivate(storageSystemError("write staged artifact", io.ErrShortWrite), reservationErr))
	}
	if reservationErr != nil {
		return written, artifact.failWriteLocked(ctx, reservationErr)
	}
	return written, nil
}

// Publish renames without replacement while retaining and validating the
// original descriptor. Evidence is returned only after the final inode and
// parent namespace are durable; a successful replay returns the same evidence.
func (artifact *Artifact) Publish(ctx context.Context) (ArtifactEvidence, error) {
	if err := contextError(ctx); err != nil {
		return ArtifactEvidence{}, err
	}
	if artifact == nil || artifact.stage == nil {
		return ArtifactEvidence{}, internalError("artifact is required")
	}
	stage := artifact.stage
	stage.mu.Lock()
	defer stage.mu.Unlock()
	if artifact.file == nil || stage.closed || stage.cleaned {
		return ArtifactEvidence{}, internalError("artifact is not open")
	}
	if artifact.state == artifactPublished {
		return artifact.evidenceLocked(), nil
	}
	if artifact.state != artifactOpen {
		return ArtifactEvidence{}, internalError("artifact is not open")
	}
	if err := stage.owner.ops.fsync(int(artifact.file.Fd())); err != nil {
		return ArtifactEvidence{}, artifact.failWriteLocked(ctx, storageSystemError("sync temporary artifact", err))
	}
	var stat unix.Stat_t
	if err := unix.Fstat(int(artifact.file.Fd()), &stat); err != nil {
		return ArtifactEvidence{}, artifact.failWriteLocked(ctx, systemError("inspect temporary artifact", err))
	}
	if stat.Size != artifact.written {
		return ArtifactEvidence{}, artifact.failWriteLocked(
			ctx, internalError("staged artifact size does not match counted bytes"),
		)
	}
	if err := validateRegularFD(ctx, int(artifact.file.Fd()), artifact.written, 1); err != nil {
		return ArtifactEvidence{}, artifact.failWriteLocked(ctx,
			joinPrivate(internalError("temporary artifact metadata is invalid"), err))
	}
	if err := stage.syncParents(ctx); err != nil {
		return ArtifactEvidence{}, artifact.failWriteLocked(ctx, err)
	}
	if err := stage.owner.ops.renameat2(
		stage.pointFD, artifact.temporary, stage.pointFD, artifact.final, unix.RENAME_NOREPLACE,
	); err != nil {
		return ArtifactEvidence{}, artifact.failWriteLocked(
			ctx, storageSystemError("publish named partial artifact", err),
		)
	}
	artifact.state = artifactLinked
	if err := stage.syncParents(ctx); err != nil {
		return ArtifactEvidence{}, artifact.failWriteLocked(ctx, err)
	}
	if err := unix.Fstat(int(artifact.file.Fd()), &stat); err != nil {
		return ArtifactEvidence{}, artifact.failWriteLocked(ctx, systemError("inspect published artifact", err))
	}
	if stat.Size != artifact.written {
		return ArtifactEvidence{}, artifact.failWriteLocked(
			ctx, internalError("published artifact size does not match counted bytes"),
		)
	}
	if err := validateRegularFD(ctx, int(artifact.file.Fd()), artifact.written, 1); err != nil {
		return ArtifactEvidence{}, artifact.failWriteLocked(ctx,
			joinPrivate(internalError("published artifact metadata is invalid"), err))
	}
	copy(artifact.digest[:], artifact.hasher.Sum(nil))
	if err := artifact.sealGrowthLocked(ctx); err != nil {
		return ArtifactEvidence{}, artifact.failWriteLocked(ctx, err)
	}
	artifact.size, artifact.state = artifact.written, artifactPublished
	return artifact.evidenceLocked(), nil
}

func (artifact *Artifact) evidenceLocked() ArtifactEvidence {
	return ArtifactEvidence{Name: artifact.final, Size: uint64(artifact.size), SHA256: artifact.digest}
}

// Open returns a dup/pread reader over the retained inode; no name lookup occurs.
func (artifact *Artifact) Open(ctx context.Context) (io.ReadCloser, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	if artifact == nil || artifact.stage == nil {
		return nil, internalError("artifact is required")
	}
	stage := artifact.stage
	stage.mu.Lock()
	defer stage.mu.Unlock()
	if artifact.state != artifactPublished || artifact.file == nil || stage.closed || stage.cleaned {
		return nil, internalError("artifact is not published")
	}
	if err := validateRegularFD(ctx, int(artifact.file.Fd()), artifact.size, 1); err != nil {
		return nil, joinPrivate(internalError("published artifact metadata is invalid"), err)
	}
	fd, err := unix.FcntlInt(artifact.file.Fd(), unix.F_DUPFD_CLOEXEC, 0)
	if err != nil {
		return nil, systemError("duplicate published artifact descriptor", err)
	}
	if artifact.readers == nil {
		artifact.readers = make(map[*preadCloser]struct{})
	}
	reader := &preadCloser{fd: fd, size: artifact.size, ctx: ctx, artifact: artifact}
	artifact.readers[reader] = struct{}{}
	return reader, nil
}

type preadCloser struct {
	mu       sync.Mutex
	fd       int
	offset   int64
	size     int64
	closed   bool
	ctx      context.Context
	artifact *Artifact
}

// Read implements io.Reader; the fixed signature is the standards exception
// to ctx-first, so it uses the context captured by Artifact.Open.
func (reader *preadCloser) Read(content []byte) (int, error) {
	reader.mu.Lock()
	defer reader.mu.Unlock()
	if reader.closed {
		return 0, systemError("read closed published artifact", os.ErrClosed)
	}
	if err := contextError(reader.ctx); err != nil {
		return 0, err
	}
	if reader.offset >= reader.size {
		return 0, io.EOF
	}
	if int64(len(content)) > reader.size-reader.offset {
		content = content[:reader.size-reader.offset]
	}
	n, err := unix.Pread(reader.fd, content, reader.offset)
	reader.offset += int64(n)
	if contextErr := contextError(reader.ctx); contextErr != nil {
		return n, contextErr
	}
	if err != nil {
		return n, systemError("read published artifact", err)
	}
	if n == 0 {
		return 0, io.EOF
	}
	return n, nil
}

// Close implements io.Closer and therefore cannot take context. It follows
// the stage-mutex then reader-mutex order shared with cleanup.
func (reader *preadCloser) Close() error {
	if reader == nil {
		return nil
	}
	stage := reader.artifact.stage
	stage.mu.Lock()
	defer stage.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), stage.owner.ops.cleanupTimeout)
	defer cancel()
	return reader.closeLocked(ctx)
}

func (reader *preadCloser) closeLocked(ctx context.Context) error {
	reader.mu.Lock()
	defer reader.mu.Unlock()
	if reader.closed {
		return nil
	}
	reader.closed = true
	delete(reader.artifact.readers, reader)
	if err := unix.Close(reader.fd); err != nil {
		return systemError("close published artifact reader", err)
	}
	reader.fd = -1
	return nil
}

func (artifact *Artifact) Abort(ctx context.Context) error {
	if artifact == nil || artifact.stage == nil {
		return joinPrivate(contextError(ctx), internalError("artifact is required"))
	}
	return artifact.stage.Cleanup(ctx)
}

// Cleanup validates and removes only safe point contents and empty ancestors.
func (stage *Stage) Cleanup(ctx context.Context) error {
	callerErr := contextError(ctx)
	if stage == nil {
		return callerErr
	}
	stage.mu.Lock()
	if stage.cleaned {
		stage.mu.Unlock()
		return callerErr
	}
	if stage.closed {
		stage.mu.Unlock()
		return joinPrivate(callerErr, internalError("stage is closed"))
	}
	cleanupCtx, cancel := context.WithTimeout(context.Background(), stage.owner.ops.cleanupTimeout)
	defer cancel()
	artifactErr := stage.removeUnpublishedArtifactsLocked(cleanupCtx)
	removeErr := removePointContents(cleanupCtx, stage.pointFD, stage.owner.ops)
	if removeErr == nil {
		if err := removeDirectoryEntry(cleanupCtx, stage.stepFD, stage.ids.Point); err != nil &&
			!errors.Is(err, unix.ENOENT) {
			removeErr = systemError("remove point directory", err)
		}
	}
	if removeErr == nil {
		removeErr = syncFD(cleanupCtx, stage.stepFD)
	}
	if removeErr == nil {
		if err := removeDirectoryEntry(cleanupCtx, stage.taskFD, stage.ids.Step); err != nil &&
			!errors.Is(err, unix.ENOENT) && !errors.Is(err, unix.ENOTEMPTY) {
			removeErr = systemError("remove empty step directory", err)
		}
	}
	if removeErr == nil {
		removeErr = syncFD(cleanupCtx, stage.taskFD)
	}
	if removeErr == nil {
		if err := removeDirectoryEntry(cleanupCtx, stage.rootFD, stage.ids.Task); err != nil &&
			!errors.Is(err, unix.ENOENT) && !errors.Is(err, unix.ENOTEMPTY) {
			removeErr = systemError("remove empty task directory", err)
		}
	}
	if removeErr == nil {
		removeErr = syncFD(cleanupCtx, stage.rootFD)
	}
	artifactCloseErr := stage.closeArtifactsLocked(cleanupCtx)
	if stage.owner.ops.beforeReservationRelease != nil {
		stage.owner.ops.beforeReservationRelease()
	}
	reservationErr := stage.releaseReservation(cleanupCtx)
	closeErr := stage.closeDescriptors(cleanupCtx)
	stage.cleaned, stage.closed = true, true
	stage.mu.Unlock()
	stage.releaseLeaseMap()
	return joinPrivate(callerErr, artifactErr, removeErr, artifactCloseErr, reservationErr, closeErr)
}

// Inventory returns a detached, deterministic copy for Controller authority.
func (session *RecoverySession) Inventory() []Recovered {
	if session == nil {
		return nil
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	result := make([]Recovered, 0, len(session.order))
	for _, recoveryID := range session.order {
		recovered := session.recovered[recoveryID]
		entry := recovered.entry
		entry.Files = append([]ArtifactEvidence(nil), entry.Files...)
		result = append(result, entry)
	}
	return result
}

// ResumePrepared applies one explicit Controller disposition while Ready is closed.
func (session *RecoverySession) ResumePrepared(ctx context.Context, disposition ResumeDisposition) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	if session == nil {
		return internalError("recovery session is required")
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	if session.closed || session.completed {
		return internalError("recovery session is closed")
	}
	if strings.TrimSpace(disposition.DispositionID) == "" {
		return validationError("recovery disposition id is required")
	}
	recovered, exists := session.recovered[disposition.RecoveryID]
	if !exists {
		return validationError("recovery id is not in startup inventory")
	}
	if recovered.resolved {
		if recovered.resumed && recovered.dispositionID == disposition.DispositionID &&
			recoveryDispositionEqual(recovered.resumeDisposition, disposition) {
			return nil
		}
		return stateConflictError("recovery entry already has a disposition")
	}
	return session.resumePreparedLocked(ctx, recovered, disposition)
}

// DiscardRecovered applies explicit Controller authority and durably removes
// the namespace before its reservation marker.
func (session *RecoverySession) DiscardRecovered(
	ctx context.Context,
	recoveryID RecoveryID,
	dispositionID string,
) error {
	if session == nil {
		return joinPrivate(contextError(ctx), internalError("recovery session is required"))
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	if session.closed || session.completed {
		return joinPrivate(contextError(ctx), internalError("recovery session is closed"))
	}
	if strings.TrimSpace(dispositionID) == "" {
		return joinPrivate(contextError(ctx), validationError("recovery disposition id is required"))
	}
	recovered, exists := session.recovered[recoveryID]
	if !exists {
		return joinPrivate(contextError(ctx), validationError("recovery id is not in startup inventory"))
	}
	if recovered.resolved {
		if !recovered.resumed && recovered.dispositionID == dispositionID {
			return contextError(ctx)
		}
		return joinPrivate(contextError(ctx), stateConflictError("recovery entry already has a disposition"))
	}
	cleanupCtx, cancel := context.WithTimeout(context.Background(), session.stager.ops.cleanupTimeout)
	defer cancel()
	if err := session.discardRecoveredLocked(cleanupCtx, recovered); err != nil {
		return joinPrivate(contextError(ctx), err)
	}
	recovered.resolved = true
	recovered.dispositionID = dispositionID
	return contextError(ctx)
}

// Complete is the only transition that exposes assignment Prepare.
func (session *RecoverySession) Complete(ctx context.Context) (*Stager, []*PreparedStage, error) {
	if err := contextError(ctx); err != nil {
		return nil, nil, err
	}
	if session == nil {
		return nil, nil, internalError("recovery session is required")
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	if session.closed || session.completed {
		return nil, nil, internalError("recovery session is closed")
	}
	for _, recoveryID := range session.order {
		if !session.recovered[recoveryID].resolved {
			return nil, nil, stateConflictError("startup recovery has unresolved Controller dispositions")
		}
	}
	closeErr := closeFDs(ctx, session.reservationRootFD)
	session.reservationRootFD = -1
	unlockErr := rawOperationError("unlock startup recovery root",
		session.stager.ops.flock(session.stager.rootFD, unix.LOCK_UN))
	if unlockErr != nil {
		return nil, nil, errs.WrapJoined(errs.KindInternal, closeErr, unlockErr)
	}
	if closeErr != nil {
		return nil, nil, closeErr
	}
	session.rootLocked, session.completed = false, true
	prepared := append([]*PreparedStage(nil), session.prepared...)
	return session.stager, prepared, nil
}

// Close abandons the boot attempt without deleting retained finals.
func (session *RecoverySession) Close(ctx context.Context) error {
	if session == nil {
		return contextError(ctx)
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	if session.closed || session.completed {
		return contextError(ctx)
	}
	session.closed = true
	return closeRecoveryLocked(ctx, session)
}

func (session *RecoverySession) inventoryLocked(ctx context.Context) error {
	indexed := make(map[IDs]struct{})
	tasks, err := directoryNames(ctx, session.reservationRootFD, "recovery reservation tasks")
	if err != nil {
		return err
	}
	for _, task := range tasks {
		if task == growthMarkerName {
			continue
		}
		if err := validateManagedTaskComponent(task, "reservation task"); err != nil {
			return err
		}
		taskFD, err := openManagedReservationDirectory(ctx, session.reservationRootFD, task, session.stager.ops)
		if err != nil {
			return err
		}
		steps, err := directoryNames(ctx, taskFD, "recovery reservation steps")
		if err != nil {
			return closeWithPrimary(ctx, err, taskFD)
		}
		for _, step := range steps {
			if err := validateManagedReservationComponent(step, "step"); err != nil {
				return closeWithPrimary(ctx, err, taskFD)
			}
			stepFD, err := openManagedReservationDirectory(ctx, taskFD, step, session.stager.ops)
			if err != nil {
				return closeWithPrimary(ctx, err, taskFD)
			}
			points, err := directoryNames(ctx, stepFD, "recovery reservation points")
			if err != nil {
				return closeWithPrimary(ctx, err, stepFD, taskFD)
			}
			for _, point := range points {
				ids := IDs{Task: task, Step: step, Point: point}
				if err := validateIDs(ids); err != nil {
					return closeWithPrimary(ctx,
						joinPrivate(internalError("managed recovery reservation name is invalid"), err),
						stepFD, taskFD)
				}
				indexed[ids] = struct{}{}
				if err := session.captureRecoveredLocked(ctx, ids, taskFD, stepFD); err != nil {
					return closeWithPrimary(ctx, err, stepFD, taskFD)
				}
			}
			if err := unix.Close(stepFD); err != nil {
				return closeWithPrimary(ctx, systemError("close recovery reservation step", err), taskFD)
			}
			if err := removeEmptyReservationDirectory(ctx, taskFD, step, "step"); err != nil {
				return closeWithPrimary(ctx, err, taskFD)
			}
		}
		if err := unix.Close(taskFD); err != nil {
			return systemError("close recovery reservation task", err)
		}
		if err := removeEmptyReservationDirectory(ctx, session.reservationRootFD, task, "task"); err != nil {
			return err
		}
	}
	return session.cleanupUnindexedNamespacesLocked(ctx, indexed)
}

func (session *RecoverySession) captureRecoveredLocked(
	ctx context.Context,
	ids IDs,
	reservationTaskFD int,
	reservationStepFD int,
) error {
	reservationFD, err := session.stager.ops.openat2(reservationStepFD, ids.Point, &unix.OpenHow{
		Flags: uint64(unix.O_RDWR | unix.O_CLOEXEC | unix.O_NOFOLLOW), Resolve: descendantResolvePolicy,
	})
	if err != nil {
		return joinPrivate(internalError("managed recovery reservation leaf is ambiguous"),
			systemError("open recovery reservation leaf", err))
	}
	if err := session.stager.ops.flock(reservationFD, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		primary := systemError("lock recovery reservation leaf", err)
		if flockWouldBlock(err) {
			primary = stateConflictError("live recovery reservation violates Agent ownership")
		}
		return closeWithPrimary(ctx, primary, reservationFD)
	}
	if err := validateStaleReservationLeaf(ctx, reservationFD); err != nil {
		return closeWithPrimary(ctx, err, reservationFD)
	}
	taskFD, exists, err := openRecoveryDirectory(ctx, session.stager.rootFD, ids.Task, session.stager.ops)
	if err != nil {
		return closeWithPrimary(ctx, err, reservationFD)
	}
	if !exists {
		return removeOrphanReservation(ctx, reservationStepFD, ids.Point, reservationFD)
	}
	stepFD, exists, err := openRecoveryDirectory(ctx, taskFD, ids.Step, session.stager.ops)
	if err != nil {
		return closeWithPrimary(ctx, err, reservationFD, taskFD)
	}
	if !exists {
		return closeWithPrimary(ctx,
			removeOrphanReservation(ctx, reservationStepFD, ids.Point, reservationFD), taskFD)
	}
	pointFD, exists, err := openRecoveryDirectory(ctx, stepFD, ids.Point, session.stager.ops)
	if err != nil {
		return closeWithPrimary(ctx, err, reservationFD, stepFD, taskFD)
	}
	if !exists {
		return closeWithPrimary(ctx,
			removeOrphanReservation(ctx, reservationStepFD, ids.Point, reservationFD), stepFD, taskFD)
	}
	if err := session.stager.ops.flock(pointFD, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		primary := systemError("lock recovered point namespace", err)
		if flockWouldBlock(err) {
			primary = stateConflictError("live recovered point violates Agent ownership")
		}
		return closeWithPrimary(ctx, primary, reservationFD, pointFD, stepFD, taskFD)
	}
	files, evidence, err := scanRecoveredFiles(ctx, pointFD, session.stager.ops)
	if err != nil {
		return closeRecoveredCapture(ctx, err, files, reservationFD, pointFD, stepFD, taskFD)
	}
	rootFD, err := duplicateFD(ctx, session.stager.rootFD, "recovered stage root")
	if err != nil {
		return closeRecoveredCapture(ctx, err, files, reservationFD, pointFD, stepFD, taskFD)
	}
	reservationRootFD, err := duplicateFD(ctx, session.reservationRootFD, "recovery reservation root")
	if err != nil {
		return closeRecoveredCapture(ctx, err, files, rootFD, reservationFD, pointFD, stepFD, taskFD)
	}
	reservationTaskCopy, err := duplicateFD(ctx, reservationTaskFD, "recovery reservation task")
	if err != nil {
		return closeRecoveredCapture(ctx, err, files, reservationRootFD, rootFD,
			reservationFD, pointFD, stepFD, taskFD)
	}
	reservationStepCopy, err := duplicateFD(ctx, reservationStepFD, "recovery reservation step")
	if err != nil {
		return closeRecoveredCapture(ctx, err, files, reservationTaskCopy, reservationRootFD, rootFD,
			reservationFD, pointFD, stepFD, taskFD)
	}
	recovered := &recoveredStage{
		entry: Recovered{
			RecoveryID: recoveryID(ids), IDs: ids, Files: evidence,
			ReservationState: ReservationUntrusted,
		},
		rootFD: rootFD, taskFD: taskFD, stepFD: stepFD, pointFD: pointFD,
		reservationRootFD: reservationRootFD, reservationTaskFD: reservationTaskCopy,
		reservationStepFD: reservationStepCopy, reservationFD: reservationFD, files: files,
	}
	if len(evidence) == 0 {
		if err := session.discardRecoveredLocked(ctx, recovered); err != nil {
			return err
		}
		return nil
	}
	session.recovered[recovered.entry.RecoveryID] = recovered
	session.order = append(session.order, recovered.entry.RecoveryID)
	return nil
}

func scanRecoveredFiles(
	ctx context.Context,
	pointFD int,
	ops linuxOperations,
) (map[string]*os.File, []ArtifactEvidence, error) {
	names, err := directoryNames(ctx, pointFD, "recovered point contents")
	if err != nil {
		return nil, nil, err
	}
	files := make(map[string]*os.File)
	var evidence []ArtifactEvidence
	partialsRemoved := false
	for _, name := range names {
		fd, err := ops.openat2(pointFD, name, &unix.OpenHow{
			Flags: uint64(unix.O_RDWR | unix.O_CLOEXEC | unix.O_NOFOLLOW), Resolve: descendantResolvePolicy,
		})
		if err != nil {
			return files, evidence, joinPrivate(internalError("managed recovered artifact is ambiguous"),
				systemError("open recovered artifact", err))
		}
		if err := validateRegularFD(ctx, fd, -1, 1); err != nil {
			return files, evidence, closeWithPrimary(ctx,
				joinPrivate(internalError("managed recovered artifact metadata is invalid"), err), fd)
		}
		if isManagedPartial(name) {
			if err := unix.Close(fd); err != nil {
				return files, evidence, systemError("close recovered partial", err)
			}
			if err := unix.Unlinkat(pointFD, name, 0); err != nil {
				return files, evidence, systemError("remove recovered partial", err)
			}
			partialsRemoved = true
			continue
		}
		if err := validateFinalArtifactName(name); err != nil {
			return files, evidence, closeWithPrimary(ctx,
				joinPrivate(internalError("managed recovered artifact name is invalid"), err), fd)
		}
		item, err := evidenceFromFD(ctx, name, fd)
		if err != nil {
			return files, evidence, closeWithPrimary(ctx, err, fd)
		}
		file := os.NewFile(uintptr(fd), "recovered-backup-artifact")
		if file == nil {
			return files, evidence, closeWithPrimary(ctx,
				internalError("wrap recovered artifact descriptor"), fd)
		}
		files[name], evidence = file, append(evidence, item)
	}
	if partialsRemoved {
		if err := syncFD(ctx, pointFD); err != nil {
			return files, evidence, err
		}
	}
	return files, evidence, nil
}

func evidenceFromFD(ctx context.Context, name string, fd int) (ArtifactEvidence, error) {
	var before unix.Stat_t
	if err := unix.Fstat(fd, &before); err != nil {
		return ArtifactEvidence{}, systemError("inspect recovered artifact", err)
	}
	if before.Size < 0 {
		return ArtifactEvidence{}, internalError("recovered artifact has negative size")
	}
	hasher := sha256.New()
	buffer := make([]byte, 128*1024)
	for offset := int64(0); offset < before.Size; {
		if err := contextError(ctx); err != nil {
			return ArtifactEvidence{}, err
		}
		length := int64(len(buffer))
		if length > before.Size-offset {
			length = before.Size - offset
		}
		n, err := unix.Pread(fd, buffer[:length], offset)
		if err != nil {
			return ArtifactEvidence{}, systemError("hash recovered artifact", err)
		}
		if n == 0 {
			return ArtifactEvidence{}, internalError("recovered artifact changed while hashing")
		}
		if _, err := hasher.Write(buffer[:n]); err != nil {
			return ArtifactEvidence{}, internalError("hash recovered artifact bytes")
		}
		offset += int64(n)
	}
	if err := validateRegularFD(ctx, fd, before.Size, 1); err != nil {
		return ArtifactEvidence{}, joinPrivate(internalError("recovered artifact metadata changed"), err)
	}
	item := ArtifactEvidence{Name: name, Size: uint64(before.Size)}
	copy(item.SHA256[:], hasher.Sum(nil))
	return item, nil
}

func isManagedPartial(name string) bool {
	if !strings.HasPrefix(name, ".") || !strings.HasSuffix(name, ".partial") {
		return false
	}
	base := strings.TrimSuffix(strings.TrimPrefix(name, "."), ".partial")
	return validateComponent(base, "partial artifact name") == nil
}

func recoveryID(ids IDs) RecoveryID {
	return RecoveryID(ids.Task + "/" + ids.Step + "/" + ids.Point)
}

func duplicateFD(ctx context.Context, fd int, label string) (int, error) {
	if err := contextError(ctx); err != nil {
		return -1, err
	}
	duplicate, err := unix.FcntlInt(uintptr(fd), unix.F_DUPFD_CLOEXEC, 0)
	if err != nil {
		return -1, systemError("duplicate "+label+" descriptor", err)
	}
	return duplicate, nil
}

func removeOrphanReservation(ctx context.Context, parentFD int, name string, fd int) error {
	if err := unix.Unlinkat(parentFD, name, 0); err != nil && !errors.Is(err, unix.ENOENT) {
		return closeWithPrimary(ctx, systemError("remove orphan recovery reservation", err), fd)
	}
	if err := unix.Fsync(parentFD); err != nil {
		return closeWithPrimary(ctx, storageSystemError("sync orphan recovery reservation removal", err), fd)
	}
	return closeFDs(ctx, fd)
}

func closeRecoveredCapture(ctx context.Context, primary error, files map[string]*os.File, fds ...int) error {
	var causes []error
	for _, file := range files {
		if err := file.Close(); err != nil {
			causes = append(causes, systemError("close recovered artifact", err))
		}
	}
	causes = append(causes, closeFDs(ctx, fds...))
	return joinPrivate(primary, wrapPrivateCauses("close recovered capture", causes...))
}

func (session *RecoverySession) cleanupUnindexedNamespacesLocked(
	ctx context.Context,
	indexed map[IDs]struct{},
) error {
	tasks, err := directoryNames(ctx, session.stager.rootFD, "staging task namespaces")
	if err != nil {
		return err
	}
	for _, task := range tasks {
		if task == reservationDirName || task == ownerMarkerName {
			continue
		}
		if err := validateManagedTaskComponent(task, "unindexed task"); err != nil {
			return err
		}
		taskFD, err := openManagedReservationDirectory(ctx, session.stager.rootFD, task, session.stager.ops)
		if err != nil {
			return err
		}
		steps, err := directoryNames(ctx, taskFD, "unindexed staging steps")
		if err != nil {
			return closeWithPrimary(ctx, err, taskFD)
		}
		for _, step := range steps {
			if err := validateManagedReservationComponent(step, "unindexed step"); err != nil {
				return closeWithPrimary(ctx, err, taskFD)
			}
			stepFD, err := openManagedReservationDirectory(ctx, taskFD, step, session.stager.ops)
			if err != nil {
				return closeWithPrimary(ctx, err, taskFD)
			}
			points, err := directoryNames(ctx, stepFD, "unindexed staging points")
			if err != nil {
				return closeWithPrimary(ctx, err, stepFD, taskFD)
			}
			for _, point := range points {
				ids := IDs{Task: task, Step: step, Point: point}
				if err := validateIDs(ids); err != nil {
					return closeWithPrimary(ctx,
						joinPrivate(internalError("managed unindexed point name is invalid"), err),
						stepFD, taskFD)
				}
				if _, exists := indexed[ids]; exists {
					continue
				}
				pointFD, err := openManagedReservationDirectory(ctx, stepFD, point, session.stager.ops)
				if err != nil {
					return closeWithPrimary(ctx, err, stepFD, taskFD)
				}
				if err := session.stager.ops.flock(pointFD, unix.LOCK_EX|unix.LOCK_NB); err != nil {
					primary := systemError("lock unindexed staging point", err)
					if flockWouldBlock(err) {
						primary = stateConflictError("live unindexed point violates Agent ownership")
					}
					return closeWithPrimary(ctx, primary, pointFD, stepFD, taskFD)
				}
				files, evidence, err := scanRecoveredFiles(ctx, pointFD, session.stager.ops)
				if err != nil {
					return closeRecoveredCapture(ctx, err, files, pointFD, stepFD, taskFD)
				}
				if len(evidence) != 0 {
					return closeRecoveredCapture(ctx,
						internalError("unindexed final artifact violates durable publication ordering"),
						files, pointFD, stepFD, taskFD)
				}
				if err := removeDirectoryEntry(ctx, stepFD, point); err != nil && !errors.Is(err, unix.ENOENT) {
					return closeWithPrimary(ctx,
						systemError("remove unindexed point directory", err), pointFD, stepFD, taskFD)
				}
				if err := syncFD(ctx, stepFD); err != nil {
					return closeWithPrimary(ctx, err, pointFD, stepFD, taskFD)
				}
				if err := unix.Close(pointFD); err != nil {
					return closeWithPrimary(ctx,
						systemError("close unindexed point directory", err), stepFD, taskFD)
				}
			}
			if err := unix.Close(stepFD); err != nil {
				return closeWithPrimary(ctx, systemError("close unindexed step directory", err), taskFD)
			}
			if err := removeEmptyReservationDirectory(ctx, taskFD, step, "unindexed step"); err != nil {
				return closeWithPrimary(ctx, err, taskFD)
			}
		}
		if err := unix.Close(taskFD); err != nil {
			return systemError("close unindexed task directory", err)
		}
		if err := removeEmptyReservationDirectory(ctx, session.stager.rootFD, task, "unindexed task"); err != nil {
			return err
		}
	}
	return nil
}

func (session *RecoverySession) resumePreparedLocked(
	ctx context.Context,
	recovered *recoveredStage,
	disposition ResumeDisposition,
) error {
	if !validRecoveryCapacity(disposition.RemainingGrowth) {
		return validationError("recovered remaining growth must be NoGrowth, Bounded, or ExclusiveUnknown")
	}
	if len(disposition.ExpectedFiles) != len(recovered.entry.Files) {
		return internalError("controller recovery evidence file count does not match retained files")
	}
	verified := make([]ArtifactEvidence, 0, len(recovered.entry.Files))
	for index, expected := range disposition.ExpectedFiles {
		inventory := recovered.entry.Files[index]
		if expected != inventory {
			return internalError("controller recovery evidence does not match startup inventory")
		}
		file := recovered.files[expected.Name]
		if file == nil {
			return internalError("retained recovery artifact descriptor is missing")
		}
		actual, err := evidenceFromFD(ctx, expected.Name, int(file.Fd()))
		if err != nil {
			return err
		}
		if actual != expected {
			return internalError("retained recovery artifact evidence changed before resume")
		}
		verified = append(verified, actual)
	}
	capacity := disposition.RemainingGrowth
	var growthFD = -1
	if capacity.kind != capacityNoGrowth {
		var err error
		growthFD, err = openGrowthMarker(ctx, session.reservationRootFD, session.stager.ops, false)
		if err != nil {
			return err
		}
		lock := unix.LOCK_SH
		if capacity.kind == capacityExclusiveUnknown {
			lock = unix.LOCK_EX
		}
		if err := session.stager.ops.flock(growthFD, lock|unix.LOCK_NB); err != nil {
			primary := systemError("admit recovered stage growth", err)
			if flockWouldBlock(err) {
				primary = stateConflictError("recovered growth conflicts with another resumed stage")
			}
			return closeWithPrimary(ctx, primary, growthFD)
		}
	}
	remaining := capacity.requiredBytes
	if capacity.kind == capacityBounded {
		available, err := availableBytes(ctx, session.stager.rootFD, session.stager.ops)
		if err != nil {
			return closeWithPrimary(ctx, err, growthFD)
		}
		wanted, overflow := addUint64(session.reserved, remaining)
		if overflow {
			return closeWithPrimary(ctx, internalError("recovered capacity accounting overflow"), growthFD)
		}
		if wanted > available {
			return closeWithPrimary(ctx,
				errs.New(errs.KindStorageUnavailable, "recovered stage has insufficient free space"), growthFD)
		}
	}
	if err := unix.Fchown(recovered.reservationFD, 0, 0); err != nil {
		return closeWithPrimary(ctx, systemError("set recovered reservation ownership", err), growthFD)
	}
	if err := unix.Fchmod(recovered.reservationFD, fileMode); err != nil {
		return closeWithPrimary(ctx, systemError("set recovered reservation mode", err), growthFD)
	}
	if err := writeReservation(ctx, recovered.reservationFD, remaining, session.stager.ops); err != nil {
		return closeWithPrimary(ctx, err, growthFD)
	}
	if err := session.stager.ops.fsync(recovered.reservationFD); err != nil {
		return closeWithPrimary(ctx, storageSystemError("sync recovered capacity reservation", err), growthFD)
	}
	if err := session.stager.ops.fsync(recovered.reservationStepFD); err != nil {
		return closeWithPrimary(ctx,
			storageSystemError("sync recovered reservation directory", err), growthFD)
	}
	stage := &Stage{
		owner: session.stager, rootFD: recovered.rootFD, taskFD: recovered.taskFD,
		stepFD: recovered.stepFD, pointFD: recovered.pointFD,
		reservationRootFD: recovered.reservationRootFD, reservationTaskFD: recovered.reservationTaskFD,
		reservationStepFD: recovered.reservationStepFD, reservationFD: recovered.reservationFD,
		growthFD: growthFD, reservationRemaining: remaining, capacity: capacity,
		ids: recovered.entry.IDs, artifacts: make(map[*Artifact]struct{}),
	}
	prepared := &PreparedStage{
		RecoveryID: disposition.RecoveryID, DispositionID: disposition.DispositionID,
		IDs: recovered.entry.IDs, Stage: stage,
	}
	for _, item := range verified {
		file := recovered.files[item.Name]
		artifact := &Artifact{
			stage: stage, file: file, final: item.Name, size: int64(item.Size), written: int64(item.Size),
			state: artifactPublished, readers: make(map[*preadCloser]struct{}), digest: item.SHA256,
		}
		stage.artifacts[artifact] = struct{}{}
		prepared.Files = append(prepared.Files, PreparedArtifact{Evidence: item, Artifact: artifact})
		delete(recovered.files, item.Name)
	}
	recovered.rootFD, recovered.taskFD, recovered.stepFD, recovered.pointFD = -1, -1, -1, -1
	recovered.reservationRootFD, recovered.reservationTaskFD = -1, -1
	recovered.reservationStepFD, recovered.reservationFD = -1, -1
	recovered.resolved = true
	recovered.resumed = true
	recovered.dispositionID = disposition.DispositionID
	storedDisposition := disposition
	storedDisposition.ExpectedFiles = append([]ArtifactEvidence(nil), disposition.ExpectedFiles...)
	recovered.resumeDisposition = &storedDisposition
	session.stager.leases[stage.ids] = stage
	session.prepared = append(session.prepared, prepared)
	if capacity.kind == capacityBounded {
		session.reserved += remaining
	}
	return nil
}

func validRecoveryCapacity(capacity CapacityMode) bool {
	switch capacity.kind {
	case capacityNoGrowth, capacityExclusiveUnknown:
		return capacity.requiredBytes == 0
	case capacityBounded:
		return capacity.requiredBytes > 0 && capacity.requiredBytes <= math.MaxInt64
	default:
		return false
	}
}

func recoveryDispositionEqual(stored *ResumeDisposition, candidate ResumeDisposition) bool {
	if stored == nil || stored.RecoveryID != candidate.RecoveryID ||
		stored.DispositionID != candidate.DispositionID || stored.RemainingGrowth != candidate.RemainingGrowth ||
		len(stored.ExpectedFiles) != len(candidate.ExpectedFiles) {
		return false
	}
	for index := range stored.ExpectedFiles {
		if stored.ExpectedFiles[index] != candidate.ExpectedFiles[index] {
			return false
		}
	}
	return true
}

func (session *RecoverySession) discardRecoveredLocked(ctx context.Context, recovered *recoveredStage) error {
	var primary error
	for name, file := range recovered.files {
		if err := file.Close(); err != nil {
			primary = joinPrivate(primary, systemError("close discarded recovered artifact "+name, err))
		}
		delete(recovered.files, name)
	}
	if primary == nil {
		primary = removePointContents(ctx, recovered.pointFD, session.stager.ops)
	}
	if primary == nil {
		if err := removeDirectoryEntry(ctx, recovered.stepFD, recovered.entry.IDs.Point); err != nil &&
			!errors.Is(err, unix.ENOENT) {
			primary = systemError("remove discarded recovered point", err)
		}
	}
	if primary == nil {
		primary = syncFD(ctx, recovered.stepFD)
	}
	if primary == nil {
		if err := removeDirectoryEntry(ctx, recovered.taskFD, recovered.entry.IDs.Step); err != nil &&
			!errors.Is(err, unix.ENOENT) && !errors.Is(err, unix.ENOTEMPTY) {
			primary = systemError("remove empty discarded recovered step", err)
		}
	}
	if primary == nil {
		primary = syncFD(ctx, recovered.taskFD)
	}
	if primary == nil {
		if err := removeDirectoryEntry(ctx, recovered.rootFD, recovered.entry.IDs.Task); err != nil &&
			!errors.Is(err, unix.ENOENT) && !errors.Is(err, unix.ENOTEMPTY) {
			primary = systemError("remove empty discarded recovered task", err)
		}
	}
	if primary == nil {
		primary = syncFD(ctx, recovered.rootFD)
	}
	if primary == nil {
		if err := unix.Unlinkat(recovered.reservationStepFD, recovered.entry.IDs.Point, 0); err != nil &&
			!errors.Is(err, unix.ENOENT) {
			primary = systemError("remove discarded recovered reservation", err)
		}
	}
	if primary == nil {
		primary = syncFD(ctx, recovered.reservationStepFD)
	}
	if primary == nil {
		if err := removeDirectoryEntry(ctx, recovered.reservationTaskFD, recovered.entry.IDs.Step); err != nil &&
			!errors.Is(err, unix.ENOENT) && !errors.Is(err, unix.ENOTEMPTY) {
			primary = systemError("remove empty recovered reservation step", err)
		}
	}
	if primary == nil {
		primary = syncFD(ctx, recovered.reservationTaskFD)
	}
	if primary == nil {
		if err := removeDirectoryEntry(ctx, recovered.reservationRootFD, recovered.entry.IDs.Task); err != nil &&
			!errors.Is(err, unix.ENOENT) && !errors.Is(err, unix.ENOTEMPTY) {
			primary = systemError("remove empty recovered reservation task", err)
		}
	}
	if primary == nil {
		primary = syncFD(ctx, recovered.reservationRootFD)
	}
	closeErr := closeFDs(ctx, recovered.reservationFD, recovered.reservationStepFD,
		recovered.reservationTaskFD, recovered.reservationRootFD,
		recovered.pointFD, recovered.stepFD, recovered.taskFD, recovered.rootFD)
	recovered.reservationFD, recovered.reservationStepFD = -1, -1
	recovered.reservationTaskFD, recovered.reservationRootFD = -1, -1
	recovered.pointFD, recovered.stepFD, recovered.taskFD, recovered.rootFD = -1, -1, -1, -1
	return joinPrivate(primary, closeErr)
}

func (stage *Stage) reserveCapacityLocked(ctx context.Context) error {
	dirFD, err := openOrCreateDirectory(ctx, stage.rootFD, reservationDirName, stage.owner.ops)
	if err != nil {
		return err
	}
	growthFD, err := openGrowthMarker(ctx, dirFD, stage.owner.ops, false)
	if err != nil {
		return closeWithPrimary(ctx, err, dirFD)
	}
	growthLock := unix.LOCK_SH
	if stage.capacity.kind == capacityExclusiveUnknown {
		growthLock = unix.LOCK_EX
	}
	if err := stage.owner.ops.flock(growthFD, growthLock|unix.LOCK_NB); err != nil {
		primary := systemError("acquire staging growth admission", err)
		if flockWouldBlock(err) {
			primary = stateConflictError("staging filesystem has an incompatible live growth admission")
		}
		return closeWithPrimary(ctx, primary, growthFD, dirFD)
	}
	reserved, err := sweepReservations(ctx, stage.rootFD, dirFD, stage.owner.ops, false)
	if err != nil {
		return closeWithPrimary(ctx, err, growthFD, dirFD)
	}
	required := stage.capacity.requiredBytes
	if stage.capacity.kind == capacityBounded {
		available, err := availableBytes(ctx, stage.rootFD, stage.owner.ops)
		if err != nil {
			return closeWithPrimary(ctx, err, growthFD, dirFD)
		}
		wanted, overflow := addUint64(reserved, required)
		if overflow {
			return closeWithPrimary(ctx, internalError("capacity reservation accounting overflow"), growthFD, dirFD)
		}
		if wanted > available {
			return closeWithPrimary(ctx, errs.New(errs.KindStorageUnavailable,
				"backup stage has insufficient free space"), growthFD, dirFD)
		}
	} else if reserved != 0 {
		return closeWithPrimary(ctx,
			stateConflictError("bounded reservations exist during exclusive growth admission"), growthFD, dirFD)
	}
	taskFD, err := openOrCreateDirectory(ctx, dirFD, stage.ids.Task, stage.owner.ops)
	if err != nil {
		return closeWithPrimary(ctx, err, growthFD, dirFD)
	}
	stepFD, err := openOrCreateDirectory(ctx, taskFD, stage.ids.Step, stage.owner.ops)
	if err != nil {
		return closeWithPrimary(ctx, err, growthFD, taskFD, dirFD)
	}
	fd, err := stage.owner.ops.openat2(stepFD, stage.ids.Point, &unix.OpenHow{
		Flags: uint64(unix.O_RDWR | unix.O_CREAT | unix.O_EXCL | unix.O_CLOEXEC | unix.O_NOFOLLOW),
		Mode:  uint64(fileMode), Resolve: descendantResolvePolicy,
	})
	if err != nil {
		return closeWithPrimary(ctx, storageSystemError("create capacity reservation", err),
			growthFD, stepFD, taskFD, dirFD)
	}
	fail := func(primary error) error {
		unlinkErr := unix.Unlinkat(stepFD, stage.ids.Point, 0)
		if errors.Is(unlinkErr, unix.ENOENT) {
			unlinkErr = nil
		}
		syncErr := unix.Fsync(stepFD)
		return closeWithPrimary(ctx, joinPrivate(primary,
			rawOperationError("remove failed capacity reservation", unlinkErr),
			storageOperationError("sync failed reservation removal", syncErr)),
			fd, growthFD, stepFD, taskFD, dirFD)
	}
	if err := unix.Fchown(fd, 0, 0); err != nil {
		return fail(systemError("set capacity reservation ownership", err))
	}
	if err := unix.Fchmod(fd, fileMode); err != nil {
		return fail(systemError("set capacity reservation mode", err))
	}
	if err := stage.owner.ops.flock(fd, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		return fail(systemError("lock capacity reservation", err))
	}
	if err := writeReservation(ctx, fd, required, stage.owner.ops); err != nil {
		return fail(err)
	}
	if err := unix.Fsync(fd); err != nil {
		return fail(storageSystemError("sync capacity reservation", err))
	}
	if err := unix.Fsync(stepFD); err != nil {
		return fail(storageSystemError("sync capacity reservation directory", err))
	}
	stage.reservationRootFD, stage.reservationTaskFD = dirFD, taskFD
	stage.reservationStepFD, stage.reservationFD = stepFD, fd
	stage.growthFD = growthFD
	stage.reservationRemaining = required
	return nil
}

func (stage *Stage) consumeReservation(ctx context.Context, consumed uint64) error {
	if consumed == 0 || stage.reservationFD < 0 {
		return nil
	}
	stage.owner.reservationMu.Lock()
	defer stage.owner.reservationMu.Unlock()
	if err := lockWithContext(ctx, stage.rootFD, stage.owner.ops); err != nil {
		return err
	}
	remaining := stage.reservationRemaining - consumed
	if err := appendReservationConsumption(ctx, stage.reservationFD, consumed, stage.owner.ops); err != nil {
		stage.poisoned = true
		return unlockWithPrimary(ctx, err, stage.rootFD, stage.owner.ops)
	}
	stage.reservationRemaining = remaining
	return unlockWithPrimary(ctx, nil, stage.rootFD, stage.owner.ops)
}

func (stage *Stage) releaseReservation(ctx context.Context) error {
	if stage.reservationFD < 0 && stage.reservationStepFD < 0 {
		return nil
	}
	stage.owner.reservationMu.Lock()
	defer stage.owner.reservationMu.Unlock()
	primary := lockWithContext(ctx, stage.rootFD, stage.owner.ops)
	locked := primary == nil
	if locked && stage.reservationStepFD >= 0 {
		if err := unix.Unlinkat(stage.reservationStepFD, stage.ids.Point, 0); err != nil &&
			!errors.Is(err, unix.ENOENT) {
			primary = systemError("remove capacity reservation", err)
		}
		if err := unix.Fsync(stage.reservationStepFD); err != nil {
			primary = joinPrivate(primary, storageSystemError("sync capacity reservation removal", err))
		}
		if err := removeDirectoryEntry(ctx, stage.reservationTaskFD, stage.ids.Step); err != nil &&
			!errors.Is(err, unix.ENOENT) && !errors.Is(err, unix.ENOTEMPTY) {
			primary = joinPrivate(primary, systemError("remove empty reservation step directory", err))
		}
		if err := unix.Fsync(stage.reservationTaskFD); err != nil {
			primary = joinPrivate(primary, storageSystemError("sync reservation task directory", err))
		}
		if err := removeDirectoryEntry(ctx, stage.reservationRootFD, stage.ids.Task); err != nil &&
			!errors.Is(err, unix.ENOENT) && !errors.Is(err, unix.ENOTEMPTY) {
			primary = joinPrivate(primary, systemError("remove empty reservation task directory", err))
		}
		if err := unix.Fsync(stage.reservationRootFD); err != nil {
			primary = joinPrivate(primary, storageSystemError("sync reservation root directory", err))
		}
	}
	growthErr := stage.releaseGrowthMarker(ctx)
	closeErr := closeFDs(ctx, stage.reservationFD, stage.reservationStepFD,
		stage.reservationTaskFD, stage.reservationRootFD)
	stage.reservationFD, stage.reservationStepFD = -1, -1
	stage.reservationTaskFD, stage.reservationRootFD = -1, -1
	stage.reservationRemaining = 0
	if locked {
		unlockErr := rawOperationError("unlock capacity reservation root",
			stage.owner.ops.flock(stage.rootFD, unix.LOCK_UN))
		if unlockErr != nil {
			if primary == nil {
				primary = joinPrivate(unlockErr)
			} else {
				primary = errs.WrapJoined(errs.KindInternal, primary, unlockErr)
			}
		}
	}
	return joinPrivate(primary, growthErr, closeErr)
}

func (stage *Stage) releaseGrowthMarker(ctx context.Context) error {
	if stage.growthFD < 0 {
		return nil
	}
	fd := stage.growthFD
	stage.growthFD = -1
	unlockErr := rawOperationError("unlock staging growth admission", stage.owner.ops.flock(fd, unix.LOCK_UN))
	closeErr := rawOperationError("close staging growth admission", unix.Close(fd))
	if unlockErr != nil {
		return errs.WrapJoined(errs.KindInternal, unlockErr, closeErr)
	}
	return joinPrivate(closeErr)
}

func (stage *Stage) acquireExclusiveGrowth(ctx context.Context) error {
	stage.owner.reservationMu.Lock()
	defer stage.owner.reservationMu.Unlock()
	if err := lockWithContext(ctx, stage.rootFD, stage.owner.ops); err != nil {
		return err
	}
	fd, err := openGrowthMarker(ctx, stage.reservationRootFD, stage.owner.ops, false)
	if err != nil {
		return unlockWithPrimary(ctx, err, stage.rootFD, stage.owner.ops)
	}
	if err := stage.owner.ops.flock(fd, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		primary := systemError("reacquire exclusive staging growth admission", err)
		if flockWouldBlock(err) {
			primary = stateConflictError("staging filesystem has an incompatible live growth admission")
		}
		return closeAndUnlock(ctx, primary, stage.rootFD, stage.owner.ops, fd)
	}
	if err := stage.owner.ops.flock(stage.rootFD, unix.LOCK_UN); err != nil {
		return closeWithPrimary(ctx, systemError("unlock capacity reservation root", err), fd)
	}
	stage.growthFD = fd
	return nil
}

func probeGrowthMarker(ctx context.Context, reservationRootFD int, ops linuxOperations) error {
	fd, err := openGrowthMarker(ctx, reservationRootFD, ops, true)
	if err != nil {
		return err
	}
	if err := ops.flock(fd, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		primary := systemError("lock staging growth capability marker", err)
		if flockWouldBlock(err) {
			primary = stateConflictError("live staging growth admission violates singleton Agent startup")
		}
		return closeWithPrimary(ctx, primary, fd)
	}
	primary := fallocateError("probe staging filesystem allocation", ops.fallocate(
		fd, unix.FALLOC_FL_KEEP_SIZE, 0, 1,
	))
	if primary == nil {
		var stat unix.Stat_t
		if err := unix.Fstat(fd, &stat); err != nil {
			primary = systemError("inspect staging allocation probe", err)
		} else if stat.Size != 0 {
			primary = internalError("staging FALLOC_FL_KEEP_SIZE probe changed marker size")
		}
	}
	if primary == nil {
		primary = storageOperationError("sync staging allocation probe", ops.fsync(fd))
	}
	unlockErr := rawOperationError("unlock staging growth capability marker", ops.flock(fd, unix.LOCK_UN))
	if unlockErr != nil {
		if primary == nil {
			primary = joinPrivate(unlockErr)
		} else {
			primary = errs.WrapJoined(errs.KindInternal, primary, unlockErr)
		}
	}
	return closeWithPrimary(ctx, primary, fd)
}

func openOwnerMarker(ctx context.Context, rootFD int, ops linuxOperations) (int, error) {
	flags := uint64(unix.O_RDWR | unix.O_CLOEXEC | unix.O_NOFOLLOW)
	fd, err := ops.openat2(rootFD, ownerMarkerName, &unix.OpenHow{
		Flags: flags | uint64(unix.O_CREAT|unix.O_EXCL), Mode: uint64(fileMode),
		Resolve: descendantResolvePolicy,
	})
	created := err == nil
	if err != nil && !errors.Is(err, unix.EEXIST) {
		return -1, storageSystemError("create staging Agent owner marker", err)
	}
	if !created {
		fd, err = ops.openat2(rootFD, ownerMarkerName, &unix.OpenHow{
			Flags: flags, Resolve: descendantResolvePolicy,
		})
		if err != nil {
			return -1, joinPrivate(internalError("staging Agent owner marker is unavailable"),
				systemError("open staging Agent owner marker", err))
		}
	}
	if created {
		if err := unix.Fchown(fd, 0, 0); err != nil {
			return -1, closeWithPrimary(ctx, systemError("set staging Agent owner ownership", err), fd)
		}
		if err := unix.Fchmod(fd, fileMode); err != nil {
			return -1, closeWithPrimary(ctx, systemError("set staging Agent owner mode", err), fd)
		}
		if err := unix.Fsync(rootFD); err != nil {
			return -1, closeWithPrimary(ctx, storageSystemError("sync staging Agent owner creation", err), fd)
		}
	}
	if err := validateRegularFD(ctx, fd, 0, 1); err != nil {
		return -1, closeWithPrimary(ctx,
			joinPrivate(internalError("staging Agent owner marker is ambiguous"), err), fd)
	}
	return fd, nil
}

func closeStagerWithPrimary(ctx context.Context, stager *Stager, primary error) error {
	if stager == nil {
		return primary
	}
	unlockErr := rawOperationError("unlock staging Agent ownership",
		stager.ops.flock(stager.ownerFD, unix.LOCK_UN))
	closeErr := closeFDs(ctx, stager.ownerFD, stager.rootFD)
	stager.ownerFD, stager.rootFD = -1, -1
	if unlockErr != nil {
		return errs.WrapJoined(errs.KindInternal, primary, unlockErr, closeErr)
	}
	return joinPrivate(primary, closeErr)
}

func closeRecoveryWithPrimary(ctx context.Context, session *RecoverySession, primary error) error {
	return joinPrivate(primary, closeRecoveryLocked(ctx, session))
}

func closeRecoveryLocked(ctx context.Context, session *RecoverySession) error {
	if session == nil || session.stager == nil {
		return nil
	}
	var causes []error
	for _, recovered := range session.recovered {
		for name, file := range recovered.files {
			if err := file.Close(); err != nil {
				causes = append(causes, systemError("close retained recovery artifact "+name, err))
			}
			delete(recovered.files, name)
		}
		if err := closeFDs(ctx, recovered.reservationFD, recovered.reservationStepFD,
			recovered.reservationTaskFD, recovered.reservationRootFD,
			recovered.pointFD, recovered.stepFD, recovered.taskFD, recovered.rootFD); err != nil {
			causes = append(causes, err)
		}
	}
	for _, prepared := range session.prepared {
		stage := prepared.Stage
		stage.mu.Lock()
		if err := stage.closeArtifactsLocked(ctx); err != nil {
			causes = append(causes, err)
		}
		if err := stage.releaseGrowthMarker(ctx); err != nil {
			causes = append(causes, err)
		}
		if err := closeFDs(ctx, stage.reservationFD, stage.reservationStepFD,
			stage.reservationTaskFD, stage.reservationRootFD,
			stage.pointFD, stage.stepFD, stage.taskFD, stage.rootFD); err != nil {
			causes = append(causes, err)
		}
		stage.closed = true
		stage.mu.Unlock()
	}
	if session.rootLocked {
		if err := session.stager.ops.flock(session.stager.rootFD, unix.LOCK_UN); err != nil {
			causes = append(causes, rawOperationError("unlock startup recovery root", err))
		}
		session.rootLocked = false
	}
	if err := closeFDs(ctx, session.reservationRootFD); err != nil {
		causes = append(causes, err)
	}
	session.reservationRootFD = -1
	return joinPrivate(wrapPrivateCauses("close recovery descriptors", causes...),
		closeStagerWithPrimary(ctx, session.stager, nil))
}

func openGrowthMarker(
	ctx context.Context,
	reservationRootFD int,
	ops linuxOperations,
	create bool,
) (int, error) {
	flags := uint64(unix.O_RDWR | unix.O_CLOEXEC | unix.O_NOFOLLOW)
	created := false
	var fd int
	var err error
	if create {
		fd, err = ops.openat2(reservationRootFD, growthMarkerName, &unix.OpenHow{
			Flags: flags | uint64(unix.O_CREAT|unix.O_EXCL), Mode: uint64(fileMode),
			Resolve: descendantResolvePolicy,
		})
		if err == nil {
			created = true
		} else if !errors.Is(err, unix.EEXIST) {
			return -1, storageSystemError("create staging growth marker", err)
		}
	}
	if !created {
		fd, err = ops.openat2(reservationRootFD, growthMarkerName, &unix.OpenHow{
			Flags: flags, Resolve: descendantResolvePolicy,
		})
		if err != nil {
			return -1, joinPrivate(internalError("staging growth marker is unavailable"),
				systemError("open staging growth marker", err))
		}
	}
	if created {
		if err := unix.Fchown(fd, 0, 0); err != nil {
			return -1, closeWithPrimary(ctx, systemError("set staging growth marker ownership", err), fd)
		}
		if err := unix.Fchmod(fd, fileMode); err != nil {
			return -1, closeWithPrimary(ctx, systemError("set staging growth marker mode", err), fd)
		}
		if err := unix.Fsync(reservationRootFD); err != nil {
			return -1, closeWithPrimary(ctx,
				storageSystemError("sync staging growth marker creation", err), fd)
		}
	}
	if err := validateRegularFD(ctx, fd, 0, 1); err != nil {
		return -1, closeWithPrimary(ctx,
			joinPrivate(internalError("staging growth marker is ambiguous"), err), fd)
	}
	return fd, nil
}

func fallocateError(operation string, err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, unix.EOPNOTSUPP) || errors.Is(err, unix.ENOSYS) {
		return requiredLinuxError("fallocate with FALLOC_FL_KEEP_SIZE")
	}
	return storageSystemError(operation, err)
}

func sweepReservations(
	ctx context.Context,
	stageRootFD int,
	reservationRootFD int,
	ops linuxOperations,
	startup bool,
) (uint64, error) {
	tasks, err := directoryNames(ctx, reservationRootFD, "reservation tasks")
	if err != nil {
		return 0, err
	}
	var total uint64
	for _, task := range tasks {
		if task == growthMarkerName {
			continue
		}
		if err := validateManagedTaskComponent(task, "reservation task"); err != nil {
			return 0, err
		}
		value, err := sweepReservationTask(ctx, stageRootFD, reservationRootFD, task, ops, startup)
		if err != nil {
			return 0, err
		}
		var overflow bool
		total, overflow = addUint64(total, value)
		if overflow {
			return 0, internalError("capacity reservation accounting overflow")
		}
	}
	return total, nil
}

func sweepReservationTask(
	ctx context.Context,
	stageRootFD int,
	reservationRootFD int,
	task string,
	ops linuxOperations,
	startup bool,
) (uint64, error) {
	taskFD, err := openManagedReservationDirectory(ctx, reservationRootFD, task, ops)
	if err != nil {
		return 0, err
	}
	steps, err := directoryNames(ctx, taskFD, "reservation steps")
	if err != nil {
		return 0, closeWithPrimary(ctx, err, taskFD)
	}
	var total uint64
	for _, step := range steps {
		if err := validateManagedReservationComponent(step, "step"); err != nil {
			return 0, closeWithPrimary(ctx, err, taskFD)
		}
		value, err := sweepReservationStep(ctx, stageRootFD, taskFD, IDs{Task: task, Step: step}, ops, startup)
		if err != nil {
			return 0, closeWithPrimary(ctx, err, taskFD)
		}
		var overflow bool
		total, overflow = addUint64(total, value)
		if overflow {
			return 0, closeWithPrimary(ctx,
				internalError("capacity reservation accounting overflow"), taskFD)
		}
	}
	if err := unix.Close(taskFD); err != nil {
		return 0, systemError("close reservation task directory", err)
	}
	if err := removeEmptyReservationDirectory(ctx, reservationRootFD, task, "task"); err != nil {
		return 0, err
	}
	return total, nil
}

func sweepReservationStep(
	ctx context.Context,
	stageRootFD int,
	reservationTaskFD int,
	ids IDs,
	ops linuxOperations,
	startup bool,
) (uint64, error) {
	stepFD, err := openManagedReservationDirectory(ctx, reservationTaskFD, ids.Step, ops)
	if err != nil {
		return 0, err
	}
	points, err := directoryNames(ctx, stepFD, "reservation points")
	if err != nil {
		return 0, closeWithPrimary(ctx, err, stepFD)
	}
	var total uint64
	for _, point := range points {
		pointIDs := IDs{Task: ids.Task, Step: ids.Step, Point: point}
		if err := validateIDs(pointIDs); err != nil {
			return 0, closeWithPrimary(ctx,
				joinPrivate(internalError("managed reservation point name is invalid"), err), stepFD)
		}
		value, err := sweepReservationLeaf(ctx, stageRootFD, stepFD, pointIDs, ops, startup)
		if err != nil {
			return 0, closeWithPrimary(ctx, err, stepFD)
		}
		var overflow bool
		total, overflow = addUint64(total, value)
		if overflow {
			return 0, closeWithPrimary(ctx,
				internalError("capacity reservation accounting overflow"), stepFD)
		}
	}
	if err := unix.Close(stepFD); err != nil {
		return 0, systemError("close reservation step directory", err)
	}
	if err := removeEmptyReservationDirectory(ctx, reservationTaskFD, ids.Step, "step"); err != nil {
		return 0, err
	}
	return total, nil
}

func sweepReservationLeaf(
	ctx context.Context,
	_ int,
	reservationStepFD int,
	ids IDs,
	ops linuxOperations,
	startup bool,
) (uint64, error) {
	fd, err := ops.openat2(reservationStepFD, ids.Point, &unix.OpenHow{
		Flags: uint64(unix.O_RDWR | unix.O_CLOEXEC | unix.O_NOFOLLOW), Resolve: descendantResolvePolicy,
	})
	if err != nil {
		return 0, joinPrivate(internalError("managed reservation leaf is ambiguous"),
			systemError("open capacity reservation", err))
	}
	lockErr := ops.flock(fd, unix.LOCK_EX|unix.LOCK_NB)
	if lockErr == nil {
		if err := validateStaleReservationLeaf(ctx, fd); err != nil {
			return 0, closeWithPrimary(ctx, err, fd)
		}
		return 0, closeWithPrimary(ctx,
			internalError("stale reservation appeared after mandatory startup recovery"), fd)
	}
	if !flockWouldBlock(lockErr) {
		return 0, closeWithPrimary(ctx,
			systemError("inspect capacity reservation lock", lockErr), fd)
	}
	if startup {
		return 0, closeWithPrimary(ctx,
			stateConflictError("live capacity reservation violates singleton Agent startup"), fd)
	}
	if err := validateRegularFD(ctx, fd, -1, 1); err != nil {
		return 0, closeWithPrimary(ctx,
			joinPrivate(internalError("capacity reservation state is corrupt"), err), fd)
	}
	value, readErr := readReservation(ctx, fd)
	closeErr := unix.Close(fd)
	if readErr != nil {
		return 0, joinPrivate(readErr, rawOperationError("close capacity reservation", closeErr))
	}
	if closeErr != nil {
		return 0, systemError("close capacity reservation", closeErr)
	}
	return value, nil
}

func validateStaleReservationLeaf(ctx context.Context, fd int) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		return joinPrivate(internalError("managed reservation leaf is ambiguous"),
			systemError("inspect stale capacity reservation", err))
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFREG || stat.Nlink != 1 {
		return internalError("managed reservation leaf is ambiguous")
	}
	return nil
}

func validateManagedReservationComponent(value, label string) error {
	if err := validateComponent(value, "reservation "+label); err != nil {
		return joinPrivate(internalError("managed reservation "+label+" name is invalid"), err)
	}
	return nil
}

func validateManagedTaskComponent(value, label string) error {
	if err := validateTaskComponent(value); err != nil {
		return joinPrivate(internalError("managed "+label+" name is invalid"), err)
	}
	return nil
}

func openManagedReservationDirectory(
	ctx context.Context,
	parentFD int,
	name string,
	ops linuxOperations,
) (int, error) {
	fd, err := openDirectoryAt(ctx, parentFD, name, descendantResolvePolicy, ops)
	if err != nil {
		return -1, joinPrivate(internalError("managed reservation directory is ambiguous"),
			systemError("open managed reservation directory", err))
	}
	if err := validateDirectoryFD(ctx, fd); err != nil {
		return -1, closeWithPrimary(ctx,
			joinPrivate(internalError("managed reservation directory is ambiguous"), err), fd)
	}
	return fd, nil
}

func removeEmptyReservationDirectory(ctx context.Context, parentFD int, name, label string) error {
	err := removeDirectoryEntry(ctx, parentFD, name)
	if errors.Is(err, unix.ENOENT) || errors.Is(err, unix.ENOTEMPTY) {
		return nil
	}
	if err != nil {
		return systemError("remove empty reservation "+label+" directory", err)
	}
	return syncFD(ctx, parentFD)
}

func openRecoveryDirectory(
	ctx context.Context,
	parentFD int,
	name string,
	ops linuxOperations,
) (int, bool, error) {
	fd, err := openDirectoryAt(ctx, parentFD, name, descendantResolvePolicy, ops)
	if errors.Is(err, unix.ENOENT) {
		return -1, false, nil
	}
	if err != nil {
		return -1, false, joinPrivate(internalError("managed stage namespace is ambiguous"),
			systemError("open abandoned stage directory", err))
	}
	if err := validateDirectoryFD(ctx, fd); err != nil {
		return -1, false, closeWithPrimary(ctx,
			joinPrivate(internalError("managed stage namespace is ambiguous"), err), fd)
	}
	return fd, true, nil
}

func availableBytes(ctx context.Context, rootFD int, ops linuxOperations) (uint64, error) {
	var stat unix.Statfs_t
	if err := ops.fstatfs(rootFD, &stat); err != nil {
		return 0, systemError("inspect staging filesystem capacity", err)
	}
	if stat.Bsize <= 0 {
		return 0, internalError("staging filesystem reported an invalid block size")
	}
	blockSize := uint64(stat.Bsize)
	if stat.Bavail > math.MaxUint64/blockSize {
		return 0, internalError("staging filesystem capacity overflow")
	}
	return stat.Bavail * blockSize, nil
}

func writeReservation(ctx context.Context, fd int, value uint64, ops linuxOperations) error {
	var encoded [reservationValueLen]byte
	binary.BigEndian.PutUint64(encoded[:], value)
	binary.BigEndian.PutUint64(encoded[8:], ^value)
	if err := unix.Ftruncate(fd, reservationValueLen); err != nil {
		return systemError("truncate capacity reservation", err)
	}
	return pwriteFull(ctx, fd, encoded[:], 0, ops)
}

func readReservation(ctx context.Context, fd int) (uint64, error) {
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		return 0, systemError("inspect capacity reservation", err)
	}
	if stat.Size < reservationValueLen || stat.Size%reservationValueLen != 0 {
		return 0, internalError("capacity reservation state is corrupt")
	}
	remaining := uint64(0)
	for offset := int64(0); offset < stat.Size; offset += reservationValueLen {
		if err := contextError(ctx); err != nil {
			return 0, err
		}
		var encoded [reservationValueLen]byte
		n, err := unix.Pread(fd, encoded[:], offset)
		if err != nil {
			return 0, systemError("read capacity reservation", err)
		}
		value, complement := binary.BigEndian.Uint64(encoded[:8]), binary.BigEndian.Uint64(encoded[8:])
		if n != len(encoded) || complement != ^value {
			return 0, internalError("capacity reservation state is corrupt")
		}
		if offset == 0 {
			remaining = value
			continue
		}
		if value > remaining {
			return 0, internalError("capacity reservation consumption exceeds its bound")
		}
		remaining -= value
	}
	return remaining, nil
}

func appendReservationConsumption(ctx context.Context, fd int, consumed uint64, ops linuxOperations) error {
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		return systemError("inspect capacity reservation before update", err)
	}
	if stat.Size < reservationValueLen || stat.Size%reservationValueLen != 0 {
		return internalError("capacity reservation state is corrupt before update")
	}
	var encoded [reservationValueLen]byte
	binary.BigEndian.PutUint64(encoded[:], consumed)
	binary.BigEndian.PutUint64(encoded[8:], ^consumed)
	writeErr := pwriteFull(ctx, fd, encoded[:], stat.Size, ops)
	if writeErr == nil {
		writeErr = storageOperationError("sync consumed capacity reservation", ops.fdatasync(fd))
	}
	if writeErr == nil {
		return nil
	}
	poisonErr := rawOperationError("poison failed capacity reservation",
		unix.Ftruncate(fd, stat.Size+reservationValueLen+1))
	if poisonErr == nil {
		poisonErr = storageOperationError("sync poisoned capacity reservation", ops.fdatasync(fd))
	}
	return joinPrivate(writeErr, poisonErr)
}

func pwriteFull(ctx context.Context, fd int, content []byte, offset int64, ops linuxOperations) error {
	for len(content) != 0 {
		if err := contextError(ctx); err != nil {
			return err
		}
		written, err := ops.pwrite(fd, content, offset)
		if written < 0 || written > len(content) {
			return internalError("capacity reservation pwrite returned an invalid count")
		}
		if written != 0 {
			content = content[written:]
			offset += int64(written)
		}
		if err != nil {
			return storageSystemError("write capacity reservation", err)
		}
		if written == 0 {
			return internalError("capacity reservation pwrite made no progress")
		}
	}
	return nil
}

func lockWithContext(ctx context.Context, fd int, ops linuxOperations) error {
	backoff := time.Millisecond
	for {
		if err := contextError(ctx); err != nil {
			return err
		}
		if err := ops.flock(fd, unix.LOCK_EX|unix.LOCK_NB); err != nil {
			if errors.Is(err, unix.EINTR) {
				continue
			}
			if !flockWouldBlock(err) {
				return systemError("lock capacity reservation root", err)
			}
			timer := time.NewTimer(backoff)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
			if backoff < 100*time.Millisecond {
				backoff *= 2
			}
			continue
		}
		return nil
	}
}

func unlockWithPrimary(ctx context.Context, primary error, fd int, ops linuxOperations) error {
	unlockErr := rawOperationError("unlock capacity reservation root", ops.flock(fd, unix.LOCK_UN))
	if unlockErr == nil {
		return primary
	}
	if primary == nil {
		return joinPrivate(unlockErr)
	}
	return errs.WrapJoined(errs.KindInternal, primary, unlockErr)
}

func closeAndUnlock(ctx context.Context, primary error, rootFD int, ops linuxOperations, fds ...int) error {
	closeErr := closeFDs(ctx, fds...)
	unlockErr := rawOperationError("unlock capacity reservation root", ops.flock(rootFD, unix.LOCK_UN))
	if unlockErr != nil {
		if primary == nil {
			return joinPrivate(unlockErr, closeErr)
		}
		return errs.WrapJoined(errs.KindInternal, primary, closeErr, unlockErr)
	}
	return joinPrivate(primary, closeErr)
}

func addUint64(left, right uint64) (uint64, bool) {
	if right > math.MaxUint64-left {
		return 0, true
	}
	return left + right, false
}

func flockWouldBlock(err error) bool {
	return errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN)
}

func openConfiguredRoot(ctx context.Context, root string, ops linuxOperations) (int, error) {
	current, err := unix.Open("/", unix.O_PATH|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return -1, systemError("open filesystem root", err)
	}
	for _, component := range strings.Split(strings.TrimPrefix(root, "/"), "/") {
		if err := contextError(ctx); err != nil {
			return -1, closeWithPrimary(ctx, err, current)
		}
		next, openErr := openDirectoryAt(ctx, current, component, rootResolvePolicy, ops)
		if openErr != nil {
			if errors.Is(openErr, unix.ENOSYS) {
				return -1, closeWithPrimary(ctx, requiredLinuxError("openat2"), current)
			}
			return -1, closeWithPrimary(ctx, systemError("resolve configured stage root", openErr), current)
		}
		if closeErr := unix.Close(current); closeErr != nil {
			return -1, closeWithPrimary(ctx,
				systemError("close traversed stage root descriptor", closeErr), next)
		}
		current = next
	}
	if err := validateDirectoryFD(ctx, current); err != nil {
		return -1, closeWithPrimary(ctx, err, current)
	}
	return current, nil
}

func openOrCreateDirectory(ctx context.Context, parentFD int, name string, ops linuxOperations) (int, error) {
	if err := contextError(ctx); err != nil {
		return -1, err
	}
	fd, err := openDirectoryAt(ctx, parentFD, name, descendantResolvePolicy, ops)
	created := false
	if errors.Is(err, unix.ENOENT) {
		mkdirErr := unix.Mkdirat(parentFD, name, directoryMode)
		if mkdirErr != nil && !errors.Is(mkdirErr, unix.EEXIST) {
			return -1, storageSystemError("create staging directory", mkdirErr)
		}
		created = mkdirErr == nil
		fd, err = openDirectoryAt(ctx, parentFD, name, descendantResolvePolicy, ops)
	}
	if err != nil {
		return -1, systemError("open staging directory", err)
	}
	if created {
		if err := unix.Fchown(fd, 0, 0); err != nil {
			return -1, closeWithPrimary(ctx, systemError("set staging directory ownership", err), fd)
		}
		if err := unix.Fchmod(fd, directoryMode); err != nil {
			return -1, closeWithPrimary(ctx, systemError("set staging directory mode", err), fd)
		}
		if err := unix.Fsync(fd); err != nil {
			return -1, closeWithPrimary(ctx, storageSystemError("sync staging directory", err), fd)
		}
		if err := unix.Fsync(parentFD); err != nil {
			return -1, closeWithPrimary(ctx, storageSystemError("sync staging directory parent", err), fd)
		}
	}
	if err := validateDirectoryFD(ctx, fd); err != nil {
		return -1, closeWithPrimary(ctx,
			joinPrivate(internalError("managed staging directory namespace is ambiguous"), err), fd)
	}
	return fd, nil
}

func openDirectoryAt(
	ctx context.Context, parentFD int, name string, resolve uint64, ops linuxOperations,
) (int, error) {
	return ops.openat2(parentFD, name, &unix.OpenHow{
		Flags: uint64(unix.O_RDONLY | unix.O_DIRECTORY | unix.O_CLOEXEC | unix.O_NOFOLLOW), Resolve: resolve,
	})
}

func openPathAt(ctx context.Context, parentFD int, name string, ops linuxOperations) (int, error) {
	return ops.openat2(parentFD, name, &unix.OpenHow{
		Flags: uint64(unix.O_PATH | unix.O_CLOEXEC | unix.O_NOFOLLOW), Resolve: descendantResolvePolicy,
	})
}

func validateRootPath(root string) (string, error) {
	if root == "" || !filepath.IsAbs(root) || strings.ContainsRune(root, 0) {
		return "", validationError("stage root must be an absolute path")
	}
	clean := filepath.Clean(root)
	if clean == "/" {
		return "", validationError("stage root cannot be filesystem root")
	}
	if root != clean || !utf8.ValidString(root) {
		return "", validationError("stage root must be clean UTF-8 without a trailing separator")
	}
	return clean, nil
}

func validateIDs(ids IDs) error {
	if err := validateTaskComponent(ids.Task); err != nil {
		return err
	}
	if err := validateComponent(ids.Step, "step id"); err != nil {
		return err
	}
	return validateComponent(ids.Point, "point id")
}

func validateTaskComponent(value string) error {
	if err := validateComponent(value, "task id"); err != nil {
		return err
	}
	if isPackageInternalName(value) || isManagedPartial(value) {
		return validationError("task id is reserved for backup staging")
	}
	return nil
}

// validateFinalArtifactName is the sole admission check for caller-provided
// final names. No accepted final may match recovery's managed partial grammar.
func validateFinalArtifactName(value string) error {
	if err := validateComponent(value, "artifact name"); err != nil {
		return err
	}
	if isPackageInternalName(value) || isManagedPartial(value) {
		return validationError("artifact name is reserved for backup staging")
	}
	return nil
}

func isPackageInternalName(value string) bool {
	switch value {
	case reservationDirName, growthMarkerName, ownerMarkerName:
		return true
	default:
		return false
	}
}

func validateComponent(value, label string) error {
	if value == "" || value == "." || value == ".." || strings.ContainsRune(value, 0) ||
		strings.ContainsAny(value, `/\`) || filepath.IsAbs(value) || len(value) > 255 {
		return validationError("%s is not a safe path component", label)
	}
	return nil
}

func validateDirectoryFD(ctx context.Context, fd int) error {
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		return systemError("inspect staging directory", err)
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFDIR || stat.Uid != 0 || stat.Gid != 0 || stat.Mode&0o7777 != directoryMode {
		return validationError("staging directory must be root-owned with exact mode 0700")
	}
	return nil
}

func validateRegularFD(ctx context.Context, fd int, expectedSize int64, expectedLinks uint64) error {
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		return systemError("inspect staging file", err)
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFREG || stat.Nlink != expectedLinks || stat.Uid != 0 || stat.Gid != 0 ||
		stat.Mode&0o7777 != fileMode {
		return validationError("staging file has unsafe metadata")
	}
	if expectedSize >= 0 && stat.Size != expectedSize {
		return validationError("staging file size changed")
	}
	return nil
}

func validatePointContents(ctx context.Context, fd int, ops linuxOperations) error {
	names, err := directoryNames(ctx, fd, "point directory")
	if err != nil {
		return err
	}
	for _, name := range names {
		if err := contextError(ctx); err != nil {
			return err
		}
		entryFD, err := openPathAt(ctx, fd, name, ops)
		if err != nil {
			return systemError("inspect pre-existing point entry", err)
		}
		metadataErr := validateRegularFD(ctx, entryFD, -1, 1)
		closeErr := unix.Close(entryFD)
		if metadataErr != nil {
			return joinPrivate(internalError("managed point namespace is ambiguous"), metadataErr,
				rawOperationError("close pre-existing point entry descriptor", closeErr))
		}
		if closeErr != nil {
			return systemError("close pre-existing point entry descriptor", closeErr)
		}
	}
	return nil
}

func directoryNames(ctx context.Context, fd int, label string) ([]string, error) {
	duplicate, err := unix.Dup(fd)
	if err != nil {
		return nil, systemError("duplicate "+label+" descriptor", err)
	}
	directory := os.NewFile(uintptr(duplicate), "backup-stage-directory")
	if directory == nil {
		return nil, closeWithPrimary(ctx, internalError("wrap "+label+" descriptor"), duplicate)
	}
	if _, err := unix.Seek(duplicate, 0, 0); err != nil {
		return nil, closeWithPrimary(ctx, systemError("rewind "+label, err), duplicate)
	}
	names, readErr := directory.Readdirnames(-1)
	closeErr := directory.Close()
	if readErr != nil {
		return nil, joinPrivate(systemError("read "+label, readErr), rawOperationError("close "+label, closeErr))
	}
	if closeErr != nil {
		return nil, systemError("close "+label, closeErr)
	}
	sort.Strings(names)
	return names, nil
}

func rejectExistingEntry(ctx context.Context, parentFD int, name string) error {
	var stat unix.Stat_t
	if err := unix.Fstatat(parentFD, name, &stat, unix.AT_SYMLINK_NOFOLLOW); err == nil {
		return internalError("managed staging namespace is ambiguous")
	} else if !errors.Is(err, unix.ENOENT) {
		return systemError("inspect existing staging entry", err)
	}
	return nil
}

func removePointContents(ctx context.Context, fd int, ops linuxOperations) error {
	names, err := directoryNames(ctx, fd, "point directory for cleanup")
	if err != nil {
		return err
	}
	for _, name := range names {
		if err := contextError(ctx); err != nil {
			return err
		}
		entryFD, err := openPathAt(ctx, fd, name, ops)
		if err != nil {
			return systemError("inspect point entry for cleanup", err)
		}
		metadataErr := validateRegularFD(ctx, entryFD, -1, 1)
		closeErr := unix.Close(entryFD)
		if metadataErr != nil {
			return joinPrivate(internalError("managed point namespace is ambiguous during cleanup"), metadataErr,
				rawOperationError("close point entry cleanup descriptor", closeErr))
		}
		if closeErr != nil {
			return systemError("close point entry cleanup descriptor", closeErr)
		}
		if err := unix.Unlinkat(fd, name, 0); err != nil {
			return systemError("remove point entry", err)
		}
	}
	if err := unix.Fsync(fd); err != nil {
		return systemError("sync point directory after cleanup", err)
	}
	return nil
}

func probeRenameat2(ctx context.Context, ops linuxOperations) error {
	err := ops.renameat2(-1, ".", -1, ".", unix.RENAME_NOREPLACE)
	if errors.Is(err, unix.ENOSYS) {
		return requiredLinuxError("renameat2")
	}
	if !errors.Is(err, unix.EBADF) {
		return systemError("probe renameat2 with deliberately invalid descriptors", err)
	}
	return nil
}

func (artifact *Artifact) removeUnpublishedLocked(ctx context.Context) error {
	var closeErr error
	if artifact.file != nil {
		if err := artifact.file.Close(); err != nil {
			closeErr = systemError("close unpublished artifact", err)
		}
		artifact.file = nil
	}
	if artifact.state == artifactOpen {
		artifact.state = artifactPartial
	}
	name := artifact.temporary
	if artifact.state == artifactLinked {
		name = artifact.final
	}
	var unlinkErr error
	if name != "" {
		if err := unix.Unlinkat(artifact.stage.pointFD, name, 0); err != nil && !errors.Is(err, unix.ENOENT) {
			unlinkErr = systemError("remove unpublished artifact", err)
		}
	}
	artifact.state = artifactRemoved
	delete(artifact.stage.artifacts, artifact)
	return joinPrivate(closeErr, unlinkErr)
}

func (artifact *Artifact) failWriteLocked(_ context.Context, primary error) error {
	cleanupCtx, cancel := context.WithTimeout(context.Background(), artifact.stage.owner.ops.cleanupTimeout)
	defer cancel()
	cleanupErr := artifact.removeUnpublishedLocked(cleanupCtx)
	syncErr := syncFD(cleanupCtx, artifact.stage.pointFD)
	growthErr := artifact.sealGrowthLocked(cleanupCtx)
	return joinPrivate(primary, cleanupErr, syncErr, growthErr)
}

func (artifact *Artifact) sealGrowthLocked(ctx context.Context) error {
	if !artifact.growing {
		return nil
	}
	artifact.growing = false
	if artifact.stage.growingFiles == 0 {
		return internalError("exclusive growth file accounting underflow")
	}
	artifact.stage.growingFiles--
	if artifact.stage.growingFiles != 0 {
		return nil
	}
	return artifact.stage.releaseGrowthMarker(ctx)
}

func (stage *Stage) closeArtifactsLocked(ctx context.Context) error {
	var causes []error
	for artifact := range stage.artifacts {
		for reader := range artifact.readers {
			if err := reader.closeLocked(ctx); err != nil {
				causes = append(causes, err)
			}
		}
		if artifact.file == nil {
			continue
		}
		if err := artifact.file.Close(); err != nil {
			causes = append(causes, err)
		}
		artifact.file = nil
		if artifact.state == artifactOpen {
			artifact.state = artifactPartial
		}
	}
	return wrapPrivateCauses("close staged artifact descriptors", causes...)
}

func (stage *Stage) removeUnpublishedArtifactsLocked(ctx context.Context) error {
	var result error
	for artifact := range stage.artifacts {
		if artifact.state != artifactPublished && artifact.state != artifactRemoved {
			result = joinPrivate(result, artifact.removeUnpublishedLocked(ctx))
		}
	}
	return result
}

func (stage *Stage) releaseLeaseMap() {
	if stage.owner == nil {
		return
	}
	stage.owner.mu.Lock()
	defer stage.owner.mu.Unlock()
	if stage.owner.leases[stage.ids] == stage {
		delete(stage.owner.leases, stage.ids)
	}
}

func removeDirectoryEntry(ctx context.Context, parentFD int, name string) error {
	return unix.Unlinkat(parentFD, name, unix.AT_REMOVEDIR)
}

func (stage *Stage) syncParents(ctx context.Context) error {
	for _, fd := range []int{stage.pointFD, stage.stepFD, stage.taskFD, stage.rootFD} {
		if err := syncFD(ctx, fd); err != nil {
			return err
		}
	}
	return nil
}

func syncFD(ctx context.Context, fd int) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	if err := unix.Fsync(fd); err != nil {
		return storageSystemError("sync staging directory", err)
	}
	return nil
}

func (stage *Stage) closeDescriptors(ctx context.Context) error {
	var causes []error
	for _, fd := range []*int{&stage.pointFD, &stage.stepFD, &stage.taskFD, &stage.rootFD} {
		if *fd >= 0 {
			if err := unix.Close(*fd); err != nil {
				causes = append(causes, err)
			}
			*fd = -1
		}
	}
	return wrapPrivateCauses("close staging descriptors", causes...)
}

func closeFDs(ctx context.Context, fds ...int) error {
	var causes []error
	for _, fd := range fds {
		if fd >= 0 {
			if err := unix.Close(fd); err != nil {
				causes = append(causes, err)
			}
		}
	}
	return wrapPrivateCauses("close staging descriptors", causes...)
}

func closeWithPrimary(ctx context.Context, primary error, fds ...int) error {
	var cleanup []error
	for _, fd := range fds {
		if fd >= 0 {
			if err := unix.Close(fd); err != nil {
				cleanup = append(cleanup, err)
			}
		}
	}
	if len(cleanup) == 0 {
		return primary
	}
	kind := errs.KindInternal
	if value, ok := errs.KindOf(primary); ok {
		kind = value
	}
	return errs.WrapJoined(kind, primary, cleanup...)
}

func joinPrivate(primary error, secondary ...error) error {
	causes := make([]error, 0, 1+len(secondary))
	if primary != nil {
		causes = append(causes, primary)
	}
	for _, cause := range secondary {
		if cause != nil {
			causes = append(causes, cause)
		}
	}
	if len(causes) == 0 {
		return nil
	}
	if len(causes) == 1 {
		if _, ok := errs.KindOf(causes[0]); ok {
			return causes[0]
		}
		return errs.Wrap(errs.KindInternal, causes[0])
	}
	kind := errs.KindInternal
	if value, ok := errs.KindOf(causes[0]); ok {
		kind = value
	}
	return errs.WrapJoined(kind, causes[0], causes[1:]...)
}

func wrapPrivateCauses(operation string, causes ...error) error {
	filtered := make([]error, 0, len(causes))
	for _, cause := range causes {
		if cause != nil {
			filtered = append(filtered, cause)
		}
	}
	if len(filtered) == 0 {
		return nil
	}
	primary := fmt.Errorf("backup stage: %s: %w", operation, filtered[0])
	if len(filtered) == 1 {
		return errs.Wrap(errs.KindInternal, primary)
	}
	return errs.WrapJoined(errs.KindInternal, primary, filtered[1:]...)
}

func rawOperationError(operation string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("backup stage: %s: %w", operation, err)
}

func contextError(ctx context.Context) error {
	if ctx == nil {
		return internalError("context is required")
	}
	return ctx.Err()
}

func validationError(format string, args ...any) error {
	return errs.Newf(errs.KindValidationFailed, "backup stage: "+format, args...)
}

func stateConflictError(message string) error {
	return errs.New(errs.KindStateConflict, "backup stage: "+message)
}

func requiredLinuxError(syscall string) error {
	return errs.Newf(errs.KindNotImplemented, "backup stage requires Linux %s support", syscall)
}

func internalError(message string) error {
	return errs.New(errs.KindInternal, "backup stage: "+message)
}

func systemError(operation string, err error) error {
	return errs.Wrap(errs.KindInternal, fmt.Errorf("backup stage: %s: %w", operation, err))
}

func storageSystemError(operation string, err error) error {
	if errors.Is(err, unix.ENOSPC) || errors.Is(err, unix.EDQUOT) {
		return errs.Wrap(errs.KindStorageUnavailable, fmt.Errorf("backup stage: %s: %w", operation, err))
	}
	return systemError(operation, err)
}

func storageOperationError(operation string, err error) error {
	if err == nil {
		return nil
	}
	return storageSystemError(operation, err)
}
