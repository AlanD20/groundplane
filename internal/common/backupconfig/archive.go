package backupconfig

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	headerNameOffset     = 0
	headerNameLength     = 100
	headerModeOffset     = 100
	headerUIDOffset      = 108
	headerGIDOffset      = 116
	headerSizeOffset     = 124
	headerMTimeOffset    = 136
	headerChecksumOffset = 148
	headerTypeOffset     = 156
	headerMagicOffset    = 257
	headerVersionOffset  = 263
	headerDevMajorOffset = 329
	headerDevMinorOffset = 337
	headerNumericLength  = 8
	headerSizeLength     = 12
	headerChecksumLength = 8
	maximumUSTARSize     = uint64(1<<33 - 1)
)

// ValidatedArtifact is durable parsed-manifest restore authority over an owned,
// unlinked private spool. No caller-owned source, partial digest, or transfer
// cursor is retained here.
type ValidatedArtifact struct {
	mu           sync.Mutex
	spool        ownedSpool
	layout       Layout
	metadata     []MetadataFrame
	sourceSHA256 [32]byte
	closed       bool
	closeErr     error
}

type ownedSpool interface {
	io.ReaderAt
	io.Writer
	io.WriterAt
	Stat() (os.FileInfo, error)
	Sync() error
	Truncate(int64) error
	Close() error
}

type SourceEvidence struct {
	SizeBytes uint64
	SHA256    [32]byte
}

// ValidateArtifact performs restore pass one over every exact source byte.
// It returns only after headers, payloads, padding, footer, manifest evidence,
// source size, and optional source digest all agree.
func ValidateArtifact(
	ctx context.Context,
	source io.ReaderAt,
	evidence SourceEvidence,
	spoolDirectory string,
	encoder MetadataEncoder,
) (artifact *ValidatedArtifact, resultErr error) {
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	if source == nil || evidence.SizeBytes < 2*TarBlockBytes ||
		evidence.SizeBytes > MaxSourceBytes ||
		evidence.SHA256 == ([32]byte{}) {
		return nil, archiveError("artifact source or exact size is invalid")
	}
	if err := probeExactEOF(ctx, source, evidence.SizeBytes); err != nil {
		return nil, err
	}
	spool, err := os.CreateTemp(spoolDirectory, ".groundplane-config-validated-*")
	if err != nil {
		return nil, archiveCause("private validated spool creation failed", err)
	}
	name := spool.Name()
	keep := false
	defer func() {
		if !keep {
			resultErr = cleanupOwnedSpool(resultErr, spool)
		}
	}()
	if err := os.Remove(name); err != nil {
		return nil, archiveCause("private validated spool unlink failed", err)
	}
	if err := copyOwnedSource(ctx, spool, source, evidence); err != nil {
		return nil, err
	}
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	if err := spool.Sync(); err != nil {
		return nil, archiveCause("private validated spool fsync failed", err)
	}
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	layout, sourceDigest, err := validateOwnedSource(ctx, spool, evidence)
	if err != nil {
		return nil, err
	}
	metadata, err := encodeMetadata(ctx, TransferRestore, layout.Entries, encoder)
	if err != nil {
		return nil, err
	}
	keep = true
	return &ValidatedArtifact{
		spool:        spool,
		layout:       cloneLayout(layout),
		metadata:     cloneMetadataFrames(metadata),
		sourceSHA256: sourceDigest,
	}, nil
}

func validateOwnedSource(
	ctx context.Context,
	source ownedSpool,
	evidence SourceEvidence,
) (Layout, [32]byte, error) {
	if err := proveOwnedExactSize(ctx, source, evidence.SizeBytes); err != nil {
		return Layout{}, [32]byte{}, err
	}
	section := io.NewSectionReader(source, 0, int64(evidence.SizeBytes))
	sourceHasher := sha256.New()
	reader := io.TeeReader(section, sourceHasher)

	var header [TarBlockBytes]byte
	if err := readFull(ctx, reader, header[:]); err != nil {
		return Layout{}, [32]byte{}, err
	}
	manifestSize, err := parseCanonicalHeader(header, manifestMemberName, manifestMemberMode)
	if err != nil || manifestSize > MaxManifestBytes {
		return Layout{}, [32]byte{}, archiveError("manifest header is not canonical")
	}
	manifest := make([]byte, int(manifestSize))
	defer clearBytes(manifest)
	if err := readFullContext(ctx, reader, manifest); err != nil {
		return Layout{}, [32]byte{}, err
	}
	manifestRounded, _ := roundTar(manifestSize)
	if err := readZeroBytes(ctx, reader, manifestRounded-manifestSize); err != nil {
		return Layout{}, [32]byte{}, archiveError("manifest padding is not canonical zero padding")
	}
	entries, err := parseManifest(ctx, manifest)
	if err != nil {
		return Layout{}, [32]byte{}, err
	}
	canonical, authority, err := buildManifestPayload(ctx, entries)
	if err != nil {
		return Layout{}, [32]byte{}, err
	}
	clearBytes(canonical)
	layout, err := ComputeLayout(ctx, authority, entries)
	if err != nil {
		return Layout{}, [32]byte{}, err
	}
	if authority.SourceSizeBytes != evidence.SizeBytes {
		return Layout{}, [32]byte{}, archiveError(
			"declared source size, layout, and exact source length disagree",
		)
	}

	for _, entry := range entries {
		if err := checkContext(ctx); err != nil {
			return Layout{}, [32]byte{}, err
		}
		if err := readFull(ctx, reader, header[:]); err != nil {
			return Layout{}, [32]byte{}, err
		}
		mode := uint32(0444)
		if entry.Secret {
			mode = 0600
		}
		valueSize, headerErr := parseCanonicalHeader(header, entry.Value.Path, mode)
		if headerErr != nil || valueSize != entry.Value.SizeBytes {
			return Layout{}, [32]byte{}, archiveError("selected value header is not canonical")
		}
		digest, streamErr := streamSelectedValue(ctx, reader, entry, false, nil)
		if streamErr != nil {
			return Layout{}, [32]byte{}, streamErr
		}
		if digest != entry.Value.SHA256 {
			return Layout{}, [32]byte{}, archiveError(
				"selected value digest does not match manifest evidence",
			)
		}
		rounded, _ := roundTar(entry.Value.SizeBytes)
		if err := readZeroBytes(ctx, reader, rounded-entry.Value.SizeBytes); err != nil {
			return Layout{}, [32]byte{}, archiveError(
				"selected value padding is not canonical zero padding",
			)
		}
	}
	if err := readZeroBytes(ctx, reader, 2*TarBlockBytes); err != nil {
		return Layout{}, [32]byte{}, archiveError("artifact footer is missing or nonzero")
	}
	var trailing [1]byte
	if err := checkContext(ctx); err != nil {
		return Layout{}, [32]byte{}, err
	}
	count, trailingErr := reader.Read(trailing[:])
	if count != 0 || trailingErr == nil {
		return Layout{}, [32]byte{}, archiveError("artifact has bytes after its exact footer")
	}
	if trailingErr != io.EOF {
		return Layout{}, [32]byte{}, archiveCause("artifact footer EOF probe failed", trailingErr)
	}

	var sourceDigest [32]byte
	copy(sourceDigest[:], sourceHasher.Sum(nil))
	if sourceDigest != evidence.SHA256 {
		return Layout{}, [32]byte{}, archiveError(
			"artifact source digest does not match durable evidence",
		)
	}
	if err := proveOwnedExactSize(ctx, source, evidence.SizeBytes); err != nil {
		return Layout{}, [32]byte{}, err
	}
	return layout, sourceDigest, nil
}

// Layout returns a defensive copy of the parsed durable content authority and
// member index.
func (artifact *ValidatedArtifact) Layout() Layout {
	if artifact == nil {
		return Layout{}
	}
	artifact.mu.Lock()
	defer artifact.mu.Unlock()
	return cloneLayout(artifact.layout)
}

func (artifact *ValidatedArtifact) SourceSHA256() [32]byte {
	if artifact == nil {
		return [32]byte{}
	}
	artifact.mu.Lock()
	defer artifact.mu.Unlock()
	return artifact.sourceSHA256
}

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

// Close clears the private spool authority. Cleanup is attempted even when
// ctx is already canceled so plaintext staging is never retained by mistake.
func (artifact *ValidatedArtifact) Close(ctx context.Context) error {
	if artifact == nil {
		return nil
	}
	artifact.mu.Lock()
	defer artifact.mu.Unlock()
	if artifact.closed {
		return artifact.closeErr
	}
	artifact.closed = true
	spool := artifact.spool
	artifact.spool = nil
	for index := range artifact.metadata {
		clearBytes(artifact.metadata[index].CanonicalEntry)
	}
	artifact.metadata = nil
	artifact.closeErr = cleanupOwnedSpool(checkContext(ctx), spool)
	return artifact.closeErr
}

func cleanupOwnedSpool(primary error, spool ownedSpool) error {
	if spool == nil {
		return primary
	}
	causes := make([]error, 0, 4)
	if primary != nil {
		causes = append(causes, primary)
	}
	if err := spool.Truncate(0); err != nil {
		causes = append(causes, fmt.Errorf("private validated spool truncate: %w", err))
	}
	if err := spool.Sync(); err != nil {
		causes = append(causes, fmt.Errorf("private validated spool cleanup fsync: %w", err))
	}
	// os.File.Close documents that a returned error does not make a second
	// Close a valid recovery operation. This is the one terminal close attempt.
	if err := spool.Close(); err != nil {
		causes = append(causes, fmt.Errorf("private validated spool close: %w", err))
	}
	if len(causes) == 0 {
		return nil
	}
	if len(causes) == 1 && primary != nil {
		return primary
	}
	if primary == nil {
		causes = append([]error{errors.New("private validated spool cleanup failed")}, causes...)
	}
	return errs.WrapJoined(errs.KindInternal, causes[0], causes[1:]...)
}

func canonicalHeader(name string, mode uint32, size uint64) ([TarBlockBytes]byte, error) {
	var header [TarBlockBytes]byte
	if len(name) == 0 || len(name) > headerNameLength || size > maximumUSTARSize {
		return header, archiveError("USTAR member name or size is outside the canonical profile")
	}
	for index := 0; index < len(name); index++ {
		if name[index] == 0 || name[index] > 0x7f {
			return header, archiveError("USTAR member name is not canonical ASCII")
		}
	}
	copy(header[headerNameOffset:headerNameOffset+headerNameLength], name)
	if !encodeOctal(header[headerModeOffset:headerModeOffset+headerNumericLength], uint64(mode)) ||
		!encodeOctal(header[headerUIDOffset:headerUIDOffset+headerNumericLength], 0) ||
		!encodeOctal(header[headerGIDOffset:headerGIDOffset+headerNumericLength], 0) ||
		!encodeOctal(header[headerSizeOffset:headerSizeOffset+headerSizeLength], size) ||
		!encodeOctal(header[headerMTimeOffset:headerMTimeOffset+headerSizeLength], 0) ||
		!encodeOctal(header[headerDevMajorOffset:headerDevMajorOffset+headerNumericLength], 0) ||
		!encodeOctal(header[headerDevMinorOffset:headerDevMinorOffset+headerNumericLength], 0) {
		return header, archiveError("USTAR numeric field exceeds the canonical octal profile")
	}
	for index := 0; index < headerChecksumLength; index++ {
		header[headerChecksumOffset+index] = ' '
	}
	header[headerTypeOffset] = '0'
	copy(header[headerMagicOffset:headerMagicOffset+6], []byte{'u', 's', 't', 'a', 'r', 0})
	copy(header[headerVersionOffset:headerVersionOffset+2], "00")
	var checksum uint64
	for _, value := range header {
		checksum += uint64(value)
	}
	if checksum >= 1<<18 {
		return header, archiveError("USTAR checksum exceeds six octal digits")
	}
	for index := 5; index >= 0; index-- {
		header[headerChecksumOffset+index] = byte('0' + checksum&7)
		checksum >>= 3
	}
	header[headerChecksumOffset+6] = 0
	header[headerChecksumOffset+7] = ' '
	return header, nil
}

func parseCanonicalHeader(header [TarBlockBytes]byte, name string, mode uint32) (uint64, error) {
	sizeField := header[headerSizeOffset : headerSizeOffset+headerSizeLength]
	if sizeField[len(sizeField)-1] != 0 {
		return 0, archiveError("USTAR size terminator is invalid")
	}
	var size uint64
	for _, character := range sizeField[:len(sizeField)-1] {
		if character < '0' || character > '7' {
			return 0, archiveError("USTAR size is not canonical unsigned octal")
		}
		size = size<<3 | uint64(character-'0')
	}
	expected, err := canonicalHeader(name, mode, size)
	if err != nil || header != expected {
		return 0, archiveError("USTAR header bytes are not canonical")
	}
	return size, nil
}

func encodeOctal(destination []byte, value uint64) bool {
	if len(destination) < 2 {
		return false
	}
	for index := len(destination) - 2; index >= 0; index-- {
		destination[index] = byte('0' + value&7)
		value >>= 3
	}
	if value != 0 {
		return false
	}
	destination[len(destination)-1] = 0
	return true
}

type valueSink func([]byte, uint64) error

func streamSelectedValue(
	ctx context.Context,
	reader io.Reader,
	entry Entry,
	requireReaderEOF bool,
	sink valueSink,
) ([32]byte, error) {
	if reader == nil {
		return [32]byte{}, archiveError("selected value reader is nil")
	}
	hasher := sha256.New()
	validator := selectedValueValidator{
		requireUTF8: entry.Source.Kind == SourceLiteral ||
			entry.Metadata.Kind == MetadataEnvironment,
		rejectNUL: entry.Metadata.Kind == MetadataEnvironment,
	}
	buffer := make([]byte, TransferChunkBytes)
	defer clearBytes(buffer)
	defer validator.clear()
	remaining := entry.Value.SizeBytes
	var offset uint64
	for remaining != 0 {
		if err := checkContext(ctx); err != nil {
			return [32]byte{}, err
		}
		length := uint64(len(buffer))
		if length > remaining {
			length = remaining
		}
		chunk := buffer[:int(length)]
		if err := readFull(ctx, reader, chunk); err != nil {
			return [32]byte{}, err
		}
		if !validator.consume(chunk) {
			return [32]byte{}, archiveError("selected value violates its UTF-8 or NUL policy")
		}
		// hash.Hash.Write is specified to consume all bytes and never return an error.
		_, _ = hasher.Write(chunk)
		if sink != nil {
			if err := sink(chunk, offset); err != nil {
				return [32]byte{}, err
			}
		}
		offset += length
		remaining -= length
	}
	if !validator.finish() {
		return [32]byte{}, archiveError("selected value ends with incomplete UTF-8")
	}
	if requireReaderEOF {
		if err := checkContext(ctx); err != nil {
			return [32]byte{}, err
		}
		var extra [1]byte
		count, err := reader.Read(extra[:])
		if count != 0 || err == nil {
			return [32]byte{}, archiveError("selected value is longer than its declared size")
		}
		if err != io.EOF {
			return [32]byte{}, archiveCause("selected value EOF probe failed", err)
		}
	}
	var digest [32]byte
	copy(digest[:], hasher.Sum(nil))
	return digest, nil
}

type selectedValueValidator struct {
	requireUTF8 bool
	rejectNUL   bool
	pending     []byte
}

func (validator *selectedValueValidator) consume(value []byte) bool {
	if validator.rejectNUL && bytes.IndexByte(value, 0) >= 0 {
		return false
	}
	if !validator.requireUTF8 {
		return true
	}
	combined := make([]byte, len(validator.pending)+len(value))
	defer clearBytes(combined)
	copy(combined, validator.pending)
	copy(combined[len(validator.pending):], value)
	clearBytes(validator.pending)
	validator.pending = validator.pending[:0]
	index := 0
	for index < len(combined) {
		if !utf8.FullRune(combined[index:]) {
			validator.pending = append(validator.pending, combined[index:]...)
			break
		}
		character, size := utf8.DecodeRune(combined[index:])
		if character == utf8.RuneError && size == 1 {
			return false
		}
		index += size
	}
	return true
}

func (validator *selectedValueValidator) finish() bool {
	return !validator.requireUTF8 || len(validator.pending) == 0
}

func (validator *selectedValueValidator) clear() {
	clearBytes(validator.pending)
	validator.pending = nil
}

type authenticatedValueReader struct {
	mu       sync.Mutex
	ctx      context.Context
	value    []byte
	offset   int
	closed   bool
	consumed bool
	onClose  func(bool)
}

func (reader *authenticatedValueReader) Read(destination []byte) (int, error) {
	reader.mu.Lock()
	defer reader.mu.Unlock()
	if reader.closed {
		return 0, io.EOF
	}
	if err := checkContext(reader.ctx); err != nil {
		reader.closeLocked()
		return 0, err
	}
	if reader.offset == len(reader.value) {
		reader.consumed = true
		clearBytes(reader.value)
		return 0, io.EOF
	}
	count := copy(destination, reader.value[reader.offset:])
	reader.offset += count
	if reader.offset == len(reader.value) {
		reader.consumed = true
		clearBytes(reader.value)
	}
	return count, nil
}

func (reader *authenticatedValueReader) Close() error {
	if reader == nil {
		return nil
	}
	reader.mu.Lock()
	defer reader.mu.Unlock()
	if reader.closed {
		return nil
	}
	reader.closeLocked()
	return nil
}

func (reader *authenticatedValueReader) closeLocked() {
	clearBytes(reader.value)
	reader.closed = true
	reader.offset = len(reader.value)
	closed := reader.onClose
	reader.onClose = nil
	if closed != nil {
		closed(reader.consumed)
	}
}

func readFull(ctx context.Context, source io.Reader, destination []byte) error {
	if err := checkContext(ctx); err != nil {
		return err
	}
	_, err := io.ReadFull(source, destination)
	if err != nil {
		return archiveCause("artifact source read failed", err)
	}
	return checkContext(ctx)
}

func readFullContext(ctx context.Context, source io.Reader, destination []byte) error {
	for len(destination) != 0 {
		if err := checkContext(ctx); err != nil {
			return err
		}
		length := len(destination)
		if length > TransferChunkBytes {
			length = TransferChunkBytes
		}
		if err := readFull(ctx, source, destination[:length]); err != nil {
			return err
		}
		destination = destination[length:]
	}
	return nil
}

func readZeroBytes(ctx context.Context, source io.Reader, length uint64) error {
	var buffer [TarBlockBytes]byte
	for length != 0 {
		count := uint64(len(buffer))
		if count > length {
			count = length
		}
		chunk := buffer[:int(count)]
		if err := readFull(ctx, source, chunk); err != nil {
			return err
		}
		if !allZero(chunk) {
			return archiveError("nonzero byte appears in canonical zero region")
		}
		length -= count
	}
	return nil
}

func allZero(value []byte) bool {
	for _, character := range value {
		if character != 0 {
			return false
		}
	}
	return true
}

func sameOffsets(left, right Layout) bool {
	if left.FooterOffset != right.FooterOffset ||
		len(left.ValueHeaderOffsets) != len(right.ValueHeaderOffsets) ||
		len(left.ValuePayloadOffsets) != len(right.ValuePayloadOffsets) {
		return false
	}
	for index := range left.ValueHeaderOffsets {
		if left.ValueHeaderOffsets[index] != right.ValueHeaderOffsets[index] ||
			left.ValuePayloadOffsets[index] != right.ValuePayloadOffsets[index] {
			return false
		}
	}
	return true
}

func sameLayoutContent(ctx context.Context, left, right Layout) (bool, error) {
	if left.Authority != right.Authority || !sameOffsets(left, right) ||
		len(left.Entries) != len(right.Entries) {
		return false, nil
	}
	leftManifest, leftAuthority, err := buildManifestPayload(ctx, left.Entries)
	if err != nil {
		return false, err
	}
	defer clearBytes(leftManifest)
	rightManifest, rightAuthority, err := buildManifestPayload(ctx, right.Entries)
	if err != nil {
		return false, err
	}
	defer clearBytes(rightManifest)
	return leftAuthority == rightAuthority && bytes.Equal(leftManifest, rightManifest), nil
}

func probeExactEOF(ctx context.Context, source io.ReaderAt, exactSize uint64) error {
	if err := checkContext(ctx); err != nil {
		return err
	}
	var extra [1]byte
	count, err := source.ReadAt(extra[:], int64(exactSize))
	if count != 0 || err == nil {
		return archiveError(
			"artifact source contains a hidden trailing byte or cannot prove exact EOF",
		)
	}
	if err != io.EOF {
		return archiveCause("artifact source EOF probe failed", err)
	}
	return nil
}

func proveOwnedExactSize(ctx context.Context, source ownedSpool, exactSize uint64) error {
	if err := checkContext(ctx); err != nil {
		return err
	}
	info, err := source.Stat()
	if err != nil {
		return archiveCause("private validated spool fstat failed", err)
	}
	if info.Size() < 0 || uint64(info.Size()) != exactSize {
		return archiveError("private validated spool length differs from mandatory source size")
	}
	return probeExactEOF(ctx, source, exactSize)
}

func copyOwnedSource(
	ctx context.Context,
	destination ownedSpool,
	source io.ReaderAt,
	evidence SourceEvidence,
) error {
	hasher := sha256.New()
	buffer := make([]byte, TransferChunkBytes)
	defer clearBytes(buffer)
	var offset uint64
	for offset < evidence.SizeBytes {
		if err := checkContext(ctx); err != nil {
			return err
		}
		length := uint64(len(buffer))
		if remaining := evidence.SizeBytes - offset; length > remaining {
			length = remaining
		}
		chunk := buffer[:int(length)]
		if err := readAtContext(ctx, source, chunk, offset); err != nil {
			return err
		}
		// hash.Hash.Write is specified to consume all bytes and never return an error.
		_, _ = hasher.Write(chunk)
		if err := writeFileContext(ctx, destination, chunk); err != nil {
			return err
		}
		offset += length
	}
	if err := probeExactEOF(ctx, source, evidence.SizeBytes); err != nil {
		return err
	}
	var digest [32]byte
	copy(digest[:], hasher.Sum(nil))
	if digest != evidence.SHA256 {
		return archiveError("artifact source digest does not match mandatory evidence")
	}
	return nil
}

func readAtContext(
	ctx context.Context,
	source io.ReaderAt,
	destination []byte,
	offset uint64,
) error {
	for len(destination) != 0 {
		if err := checkContext(ctx); err != nil {
			return err
		}
		requested := len(destination)
		count, err := source.ReadAt(destination, int64(offset))
		if count > 0 {
			offset += uint64(count)
			destination = destination[count:]
		}
		if err != nil && !(err == io.EOF && count == requested) {
			return archiveCause("artifact source read failed", err)
		}
		if count == 0 {
			return archiveError("artifact source read was short")
		}
	}
	return nil
}

func writeFileContext(ctx context.Context, destination io.Writer, value []byte) error {
	for len(value) != 0 {
		if err := checkContext(ctx); err != nil {
			return err
		}
		count, err := destination.Write(value)
		if count > 0 {
			value = value[count:]
		}
		if err != nil {
			return archiveCause("private validated spool write failed", err)
		}
		if count == 0 {
			return archiveError("private validated spool write was short")
		}
	}
	return nil
}

func cloneLayout(layout Layout) Layout {
	return Layout{
		Authority:           layout.Authority,
		Entries:             cloneEntries(layout.Entries),
		ValueHeaderOffsets:  append([]uint64(nil), layout.ValueHeaderOffsets...),
		ValuePayloadOffsets: append([]uint64(nil), layout.ValuePayloadOffsets...),
		FooterOffset:        layout.FooterOffset,
	}
}
