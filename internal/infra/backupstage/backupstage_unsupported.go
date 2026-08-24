//go:build !linux

package backupstage

import (
	"context"
	"crypto/sha256"
	"io"

	"github.com/AlanD20/groundplane/pkg/errs"
)

type Config struct{ Root string }
type IDs struct{ Task, Step, Point string }
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

func Bounded(requiredBytes uint64) CapacityMode {
	return CapacityMode{kind: capacityBounded, requiredBytes: requiredBytes}
}

func ExclusiveUnknown() CapacityMode {
	return CapacityMode{kind: capacityExclusiveUnknown}
}

func NoGrowth() CapacityMode { return CapacityMode{kind: capacityNoGrowth} }

type RecoveryID string
type ArtifactEvidence struct {
	Name   string
	Size   uint64
	SHA256 [sha256.Size]byte
}
type ReservationState string

const ReservationUntrusted ReservationState = "untrusted"

type Recovered struct {
	RecoveryID       RecoveryID
	IDs              IDs
	Files            []ArtifactEvidence
	ReservationState ReservationState
}
type ResumeDisposition struct {
	RecoveryID      RecoveryID
	DispositionID   string
	ExpectedFiles   []ArtifactEvidence
	RemainingGrowth CapacityMode
}
type PreparedArtifact struct {
	Evidence ArtifactEvidence
	Artifact *Artifact
}
type PreparedStage struct {
	RecoveryID    RecoveryID
	DispositionID string
	IDs           IDs
	Stage         *Stage
	Files         []PreparedArtifact
}

type Stager struct{}
type RecoverySession struct{}
type Stage struct{}
type Artifact struct{}

func OpenRecovery(context.Context, Config) (*RecoverySession, error) { return nil, unsupported() }
func (*RecoverySession) Inventory() []Recovered                      { return nil }
func (*RecoverySession) ResumePrepared(context.Context, ResumeDisposition) error {
	return unsupported()
}
func (*RecoverySession) DiscardRecovered(context.Context, RecoveryID, string) error {
	return unsupported()
}
func (*RecoverySession) Complete(context.Context) (*Stager, []*PreparedStage, error) {
	return nil, nil, unsupported()
}
func (*RecoverySession) Close(context.Context) error { return unsupported() }
func (*Stager) Prepare(context.Context, IDs, CapacityMode) (*Stage, error) {
	return nil, unsupported()
}
func (*Stager) Close(context.Context) error                          { return unsupported() }
func (*Stage) Close(context.Context) error                           { return unsupported() }
func (*Stage) CreateFile(context.Context, string) (*Artifact, error) { return nil, unsupported() }
func (*Stage) Cleanup(context.Context) error                         { return unsupported() }
func (*Artifact) Write(context.Context, []byte) (int, error)         { return 0, unsupported() }
func (*Artifact) Publish(context.Context) (ArtifactEvidence, error) {
	return ArtifactEvidence{}, unsupported()
}
func (*Artifact) Open(context.Context) (io.ReadCloser, error) { return nil, unsupported() }
func (*Artifact) Abort(context.Context) error                 { return unsupported() }

func unsupported() error {
	return errs.New(errs.KindNotImplemented, "backup stage is only supported on Linux")
}
