// Package agentvolumejournal owns the Agent's private crash-durable managed
// Volume restore mutation journal. Controller checkpoints remain authoritative.
package agentvolumejournal

import (
	"crypto/sha256"
	"path/filepath"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/agentprotocol"
	"github.com/AlanD20/groundplane/internal/common/backupvolume"
	"github.com/AlanD20/groundplane/internal/common/environmentpath"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/backupvolumefs"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/oklog/ulid/v2"
)

const (
	AgentRoot           = agentprotocol.StatePath + "/volume-restores"
	MaximumJournalBytes = backupvolume.RestoreHeadroom
	MaximumJournals     = 1024
)

// Config binds every local mutation to one sealed assignment, exact Volume
// path, and authenticated old/new manifests. JournalRoot is AgentRoot in the
// production Agent; it is not selected by a Task payload.
type Config struct {
	JournalRoot         string
	TaskID              string
	AssignmentID        string
	StepID              string
	PointID             string
	RestoreGenerationID string
	StepSHA256          [sha256.Size]byte
	OldManifestSHA256   [sha256.Size]byte
	NewManifestSHA256   [sha256.Size]byte
	OldFullTreeSHA256   [sha256.Size]byte
	NewFullTreeSHA256   [sha256.Size]byte
	OldEntryCount       uint64
	NewEntryCount       uint64
	VolumeRoot          string
	AuthorizedVolumeDir string
	ComposeKey          string
	SiblingName         string
}

// State is reconstructed only from complete, hash-chained records. Pending
// means an intent is durable while completion remains unknown. UncommittedPrefix
// means a crash left an exact but incomplete next frame; it blocks Ready until
// the same assignment repairs the prefix or a Controller disposition handles it.
type State struct {
	Completed            []backupvolumefs.Mutation
	Pending              *backupvolumefs.Mutation
	ConstructionCursor   uint64
	FinalizationCursor   uint64
	DeletionCursor       uint64
	LastCompletedOrdinal uint64
	RootNewInode         uint64
	ExchangeOldInode     uint64
	ExchangeNewInode     uint64
	Exchanged            bool
	RootDeleted          bool
	UncommittedPrefix    bool
	RecordCount          uint64
	ChainSHA256          [sha256.Size]byte
}

// Recovered is one startup inventory row. Completed is omitted from its State;
// Open/ReadState replays the full exact history after Controller disposition.
type Recovered struct {
	DirectoryName string
	Config        Config
	State         State
	Unbound       bool
	Retiring      bool
	Discarding    bool
}

func (config Config) validate() error {
	if config.JournalRoot != AgentRoot || filepath.Clean(config.JournalRoot) != config.JournalRoot ||
		ids.Validate(ids.KindTask, config.TaskID) != nil ||
		ids.Validate(ids.KindAssignment, config.AssignmentID) != nil ||
		ids.Validate(ids.KindStep, config.StepID) != nil ||
		ids.Validate(ids.KindRecoveryPoint, config.PointID) != nil ||
		len(config.RestoreGenerationID) != 26 ||
		config.RestoreGenerationID != strings.ToUpper(config.RestoreGenerationID) ||
		config.SiblingName != ".gp-restore-"+config.RestoreGenerationID ||
		config.OldEntryCount < 1 || config.OldEntryCount > backupvolume.MaxEntries ||
		config.NewEntryCount < 1 || config.NewEntryCount > backupvolume.MaxEntries {
		return invalid("Volume journal sealed authority is invalid")
	}
	if _, err := ulid.ParseStrict(config.RestoreGenerationID); err != nil {
		return invalid("Volume journal restore generation is invalid")
	}
	if _, err := environmentpath.Parse(config.VolumeRoot, config.AuthorizedVolumeDir); err != nil {
		return err
	}
	if !validComposeKey(config.ComposeKey) || config.ComposeKey == config.SiblingName ||
		config.StepSHA256 == ([sha256.Size]byte{}) ||
		config.OldManifestSHA256 == ([sha256.Size]byte{}) ||
		config.NewManifestSHA256 == ([sha256.Size]byte{}) ||
		config.OldFullTreeSHA256 == ([sha256.Size]byte{}) ||
		config.NewFullTreeSHA256 == ([sha256.Size]byte{}) {
		return invalid("Volume journal path or digest authority is invalid")
	}
	return nil
}

func validComposeKey(value string) bool {
	if value == "" || value == "." || value == ".." || len(value) > 255 {
		return false
	}
	for _, ch := range value {
		if ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch >= '0' && ch <= '9' ||
			ch == '.' || ch == '_' || ch == '-' {
			continue
		}
		return false
	}
	return true
}

func invalid(message string) error  { return errs.New(errs.KindValidationFailed, message) }
func conflict(message string) error { return errs.New(errs.KindStateConflict, message) }
func storage(err error) error {
	if err == nil {
		return nil
	}
	return errs.Wrap(errs.KindStorageUnavailable, err)
}
