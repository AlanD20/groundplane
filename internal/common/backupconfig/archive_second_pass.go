package backupconfig

import (
	"bytes"
	"context"
	"crypto/sha256"
	"github.com/AlanD20/groundplane/pkg/errs"
	"io"
	"sync"
)

type PassTwo struct {
	mu        sync.Mutex
	artifact  *ValidatedArtifact
	layout    Layout
	metadata  []MetadataFrame
	nextIndex int
	state     passTwoValueState
}

type passTwoValueState uint8

const (
	passTwoValueIdle passTwoValueState = iota
	passTwoValueInFlight
	passTwoValueAwaitingCommit
)

// BeginPassTwo rewinds and revalidates the full owned spool and source digest
// before any consumer can request selected bytes.
func (artifact *ValidatedArtifact) BeginPassTwo(ctx context.Context) (*PassTwo, error) {
	if artifact == nil {
		return nil, archiveError("validated artifact is nil")
	}
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	artifact.mu.Lock()
	defer artifact.mu.Unlock()
	if artifact.closed || artifact.spool == nil {
		return nil, archiveError("validated artifact is closed")
	}
	evidence := SourceEvidence{
		SizeBytes: artifact.layout.Authority.SourceSizeBytes,
		SHA256:    artifact.sourceSHA256,
	}
	layout, digest, err := validateOwnedSource(ctx, artifact.spool, evidence)
	if err != nil {
		return nil, err
	}
	sameLayout, compareErr := sameLayoutContent(ctx, layout, artifact.layout)
	if compareErr != nil {
		return nil, compareErr
	}
	if digest != artifact.sourceSHA256 || !sameLayout {
		return nil, archiveError("pass-two revalidation differs from durable parsed authority")
	}
	return &PassTwo{
		artifact: artifact,
		layout:   cloneLayout(layout),
		metadata: cloneMetadataFrames(artifact.metadata),
	}, nil
}

func (pass *PassTwo) MetadataFrames() []MetadataFrame {
	if pass == nil {
		return nil
	}
	return cloneMetadataFrames(pass.metadata)
}

// OpenValue fully reads and digest-authenticates one value from the owned
// immutable spool before returning a reader. The returned reader clears its
// private plaintext buffer at EOF or Close.
func (pass *PassTwo) OpenValue(ctx context.Context, index int) (Entry, io.ReadCloser, error) {
	if pass == nil || pass.artifact == nil {
		return Entry{}, nil, archiveError("pass-two value index is invalid")
	}
	if err := checkContext(ctx); err != nil {
		return Entry{}, nil, err
	}
	if err := pass.reserveValue(index); err != nil {
		return Entry{}, nil, err
	}
	reserved := true
	defer func() {
		if reserved {
			pass.releaseValue(index, false)
		}
	}()
	artifact := pass.artifact
	artifact.mu.Lock()
	defer artifact.mu.Unlock()
	if artifact.closed || artifact.spool == nil {
		return Entry{}, nil, archiveError("validated artifact is closed")
	}
	entry := cloneEntries([]Entry{pass.layout.Entries[index]})[0]
	value := make([]byte, int(entry.Value.SizeBytes))
	keep := false
	defer func() {
		if !keep {
			clearBytes(value)
		}
	}()
	if err := readAtContext(
		ctx,
		artifact.spool,
		value,
		pass.layout.ValuePayloadOffsets[index],
	); err != nil {
		return Entry{}, nil, err
	}
	validator := selectedValueValidator{
		requireUTF8: entry.Source.Kind == SourceLiteral ||
			entry.Metadata.Kind == MetadataEnvironment,
		rejectNUL: entry.Metadata.Kind == MetadataEnvironment,
	}
	if !validator.consume(value) || !validator.finish() {
		validator.clear()
		return Entry{}, nil, archiveError("pass-two selected value violates its semantic policy")
	}
	validator.clear()
	digest := sha256.Sum256(value)
	if digest != entry.Value.SHA256 {
		return Entry{}, nil, archiveError("pass-two selected value digest changed after validation")
	}
	keep = true
	reserved = false
	return entry, &authenticatedValueReader{
		ctx:     ctx,
		value:   value,
		onClose: func(consumed bool) { pass.releaseValue(index, consumed) },
	}, nil
}

func (pass *PassTwo) reserveValue(index int) error {
	pass.mu.Lock()
	defer pass.mu.Unlock()
	if pass.state != passTwoValueIdle {
		return errs.New(
			errs.KindStateConflict,
			"config pass-two value is in flight or awaiting durable commit",
		)
	}
	if index != pass.nextIndex || index < 0 || index >= len(pass.layout.Entries) {
		return errs.New(errs.KindStateConflict, "config pass-two value ordinal is out of order")
	}
	pass.state = passTwoValueInFlight
	return nil
}

func (pass *PassTwo) releaseValue(index int, consumed bool) {
	pass.mu.Lock()
	defer pass.mu.Unlock()
	if pass.state != passTwoValueInFlight || index != pass.nextIndex {
		return
	}
	if consumed {
		pass.state = passTwoValueAwaitingCommit
	} else {
		pass.state = passTwoValueIdle
	}
}

// CommitValue advances the pass-two cursor only after the caller has durably
// committed the matching EntryEnd. Consumption alone is never publication
// authority.
func (pass *PassTwo) CommitValue(ctx context.Context, index int) error {
	if pass == nil {
		return archiveError("pass-two value commit target is nil")
	}
	if err := checkContext(ctx); err != nil {
		return err
	}
	pass.mu.Lock()
	defer pass.mu.Unlock()
	if pass.state != passTwoValueAwaitingCommit || index != pass.nextIndex {
		return errs.New(
			errs.KindStateConflict,
			"config pass-two value is not awaiting this durable commit",
		)
	}
	pass.nextIndex++
	pass.state = passTwoValueIdle
	return nil
}
