//go:build !linux

package backupvolumefs

import (
	"context"
	"crypto/sha256"
	"io"

	"github.com/AlanD20/groundplane/internal/common/backupvolume"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type Volume struct{}
type Tree struct {
	Entries        []backupvolume.Entry
	FullTreeSHA256 [sha256.Size]byte
}
type Replacement struct{}
type MutationKind string

const (
	MutationConstruction MutationKind = "construction"
	MutationFinalization MutationKind = "finalization"
	MutationExchange     MutationKind = "exchange"
	MutationDelete       MutationKind = "delete"
)

type Mutation struct {
	Kind       MutationKind
	Ordinal    uint64
	Entry      backupvolume.Entry
	OldTreeSHA [sha256.Size]byte
	NewTreeSHA [sha256.Size]byte
	OldInode   uint64
	NewInode   uint64
}

type Journal interface {
	Intent(context.Context, Mutation) error
	Completed(context.Context, Mutation) error
}

func Open(context.Context, string, string, string) (*Volume, error) { return nil, unsupported() }
func (*Volume) Close() error                                        { return nil }
func (*Volume) Snapshot(context.Context) (Tree, error)              { return Tree{}, unsupported() }
func (*Volume) Capture(context.Context, io.Writer,
	func([]backupvolume.Entry) ([]backupvolume.ManifestEntryBytes, error),
) (Tree, []backupvolume.ManifestEntryBytes, backupvolume.ArtifactEvidence, error) {
	return Tree{}, nil, backupvolume.ArtifactEvidence{}, unsupported()
}
func TreeFromEntries([]backupvolume.Entry) (Tree, error) { return Tree{}, unsupported() }
func InspectCapturedArchive(context.Context, io.ReaderAt, backupvolume.ArtifactEvidence,
	func([]backupvolume.Entry) ([]backupvolume.ManifestEntryBytes, error),
) (backupvolume.ValidatedArchive, []backupvolume.ManifestEntryBytes, error) {
	return backupvolume.ValidatedArchive{}, nil, unsupported()
}
func (*Volume) CreateReplacement(context.Context, string, backupvolume.Entry, Journal) (*Replacement, error) {
	return nil, unsupported()
}
func (*Volume) ResumeCreatedReplacement(context.Context, string, backupvolume.Entry, Journal) (*Replacement, error) {
	return nil, unsupported()
}
func (*Volume) OpenReplacement(context.Context, string, uint64) (*Replacement, error) {
	return nil, unsupported()
}
func (*Volume) OpenExchanged(context.Context, string, uint64, uint64) (*Replacement, error) {
	return nil, unsupported()
}
func (*Volume) ResumeRootDeletion(context.Context, string, uint64, uint64, uint64,
	[32]byte, [32]byte, backupvolume.Entry, Journal,
) error {
	return unsupported()
}
func (*Replacement) Close() error { return nil }
func (*Replacement) Construct(context.Context, uint64, backupvolume.Entry, io.Reader, bool, Journal) error {
	return unsupported()
}
func (*Replacement) ConstructArchive(context.Context, io.ReaderAt, backupvolume.ValidatedArchive,
	[]backupvolume.ManifestEntryBytes, uint64, bool, Journal,
) error {
	return unsupported()
}
func (*Replacement) Finalize(context.Context, uint64, backupvolume.Entry, bool, Journal) error {
	return unsupported()
}
func (*Replacement) Verify(context.Context, []backupvolume.Entry) ([sha256.Size]byte, error) {
	return [sha256.Size]byte{}, unsupported()
}
func (*Replacement) Exchange(context.Context, Tree, []backupvolume.Entry, bool, Journal) error {
	return unsupported()
}
func (*Replacement) SnapshotOld(context.Context) (Tree, error)             { return Tree{}, unsupported() }
func DeletionOrder(Tree) ([]backupvolume.Entry, error)                     { return nil, unsupported() }
func FinalizationOrder([]backupvolume.Entry) ([]backupvolume.Entry, error) { return nil, unsupported() }
func (*Replacement) DeleteOld(context.Context, uint64, backupvolume.Entry, [32]byte, [32]byte, bool, Journal) error {
	return unsupported()
}

func unsupported() error {
	return errs.New(errs.KindNotImplemented, "managed Volume backup filesystem is only supported on Linux")
}
