// Package backupconfig implements the canonical environment-config-v1 backup
// artifact. It is deliberately independent of the Agent protocol: callers
// provide protocol metadata bytes separately, while this package owns the
// byte-complete manifest, USTAR layout, and content authority.
package backupconfig

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/internal/common/backupformat"
	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	Format = "environment-config-v1"

	MaxEntries                    = 4096
	MaxSelectedValueBytes         = 262144
	MaxTotalSelectedValueBytes    = 1073741824
	MaxManifestBytes              = 67108864
	MaxCanonicalEntryBytes        = 7936
	MaxEntryHeaderEnvelopeBytes   = 8118
	MaxDurableMetadataBytes       = 32505856
	MaxSourceBytes                = 1142949376
	MaxStoredAgeBytes             = 1143228616
	MaxPlainAndStoredAgePeakBytes = 2286177992

	TarBlockBytes      = 512
	TransferChunkBytes = 32768
)

// ContentAuthority is the complete Config authority sealed before transfer.
// SourceSHA256 is intentionally separate because it authenticates the entire
// USTAR byte stream rather than the semantic content described here.
type ContentAuthority struct {
	ManifestSHA256          [32]byte
	EntryCount              uint32
	TotalSelectedValueBytes uint64
	ManifestSizeBytes       uint64
	SourceSizeBytes         uint64
}

type MetadataKind uint8

const (
	MetadataEnvironment MetadataKind = 1
	MetadataFile        MetadataKind = 2
)

type EnvironmentMetadata struct {
	Key string
}

type FileMetadata struct {
	Path string
	Mode uint32
	UID  uint32
	GID  uint32
}

// Metadata is a closed union selected by Kind. The inactive member must be
// zero-valued.
type Metadata struct {
	Kind        MetadataKind
	Environment EnvironmentMetadata
	File        FileMetadata
}

type ExposureKind uint8

const (
	ExposureAll      ExposureKind = 1
	ExposureServices ExposureKind = 2
)

// Exposure is a closed union. ServiceIDs is populated only for
// ExposureServices and is in ascending raw UTF-8 stable-ID order.
type Exposure struct {
	Kind       ExposureKind
	ServiceIDs []string
}

type SourceKind uint8

const (
	SourceLiteral         SourceKind = 1
	SourceSecretReference SourceKind = 2
	SourceFact            SourceKind = 3
)

type SecretReference struct {
	AuthoredKey string
}

type FactReference struct {
	AttachID      string
	Fact          string
	GrantAttachID string
}

// Source is a closed union selected by Kind. The inactive member must be
// zero-valued. Stable revisions belong to protocol metadata, not the archive.
type Source struct {
	Kind            SourceKind
	SecretReference SecretReference
	Fact            FactReference
}

type ValueEvidence struct {
	Path      string
	SizeBytes uint64
	SHA256    [32]byte
}

type Entry struct {
	ID       string
	Metadata Metadata
	Exposure Exposure
	Source   Source
	Secret   bool
	Value    ValueEvidence
}

// Layout contains exact member offsets. ValueHeaderOffsets and
// ValuePayloadOffsets use the same ordinal as Entries.
type Layout struct {
	Authority           ContentAuthority
	Entries             []Entry
	ValueHeaderOffsets  []uint64
	ValuePayloadOffsets []uint64
	FooterOffset        uint64
}

// BuildManifest returns the exact RFC 8785 payload and its complete content
// authority. Entries must already be in canonical stable-ID order.
func BuildManifest(
	ctx context.Context,
	direction TransferDirection,
	entries []Entry,
	encoder MetadataEncoder,
) ([]byte, ContentAuthority, []MetadataFrame, error) {
	payload, authority, err := buildManifestPayload(ctx, entries)
	if err != nil {
		return nil, ContentAuthority{}, nil, err
	}
	metadata, err := encodeMetadata(ctx, direction, entries, encoder)
	if err != nil {
		clearBytes(payload)
		return nil, ContentAuthority{}, nil, err
	}
	return payload, authority, metadata, nil
}

func buildManifestPayload(ctx context.Context, entries []Entry) ([]byte, ContentAuthority, error) {
	if err := checkContext(ctx); err != nil {
		return nil, ContentAuthority{}, err
	}
	if err := validateEntries(entries); err != nil {
		return nil, ContentAuthority{}, err
	}

	payload := make([]byte, 0, len(entries)*512+len(Format)+32)
	payload = append(payload, `{"entries":[`...)
	for index, entry := range entries {
		if err := checkContext(ctx); err != nil {
			clearBytes(payload)
			return nil, ContentAuthority{}, err
		}
		if index != 0 {
			payload = append(payload, ',')
		}
		payload = appendManifestEntry(payload, entry)
		if len(payload) > MaxManifestBytes {
			clearBytes(payload)
			return nil, ContentAuthority{}, archiveError(
				"canonical Config manifest exceeds its byte limit",
			)
		}
	}
	payload = append(payload, `],"format":"environment-config-v1"}`...)
	if len(payload) > MaxManifestBytes {
		clearBytes(payload)
		return nil, ContentAuthority{}, archiveError(
			"canonical Config manifest exceeds its byte limit",
		)
	}

	authority := ContentAuthority{
		ManifestSHA256:    sha256.Sum256(payload),
		EntryCount:        uint32(len(entries)),
		ManifestSizeBytes: uint64(len(payload)),
	}
	for _, entry := range entries {
		authority.TotalSelectedValueBytes += entry.Value.SizeBytes
	}
	geometry, err := computeGeometry(uint64(len(payload)), entries)
	if err != nil {
		clearBytes(payload)
		return nil, ContentAuthority{}, err
	}
	authority.SourceSizeBytes = geometry.sourceSize
	return payload, authority, nil
}

// ComputeLayout verifies authority against regenerated canonical manifest
// bytes and returns checked USTAR offsets.
func ComputeLayout(
	ctx context.Context,
	authority ContentAuthority,
	entries []Entry,
) (Layout, error) {
	payload, expected, err := buildManifestPayload(ctx, entries)
	if err != nil {
		return Layout{}, err
	}
	clearBytes(payload)
	if authority != expected {
		return Layout{}, archiveError("Config content authority does not match canonical entries")
	}
	geometry, err := computeGeometry(authority.ManifestSizeBytes, entries)
	if err != nil {
		return Layout{}, err
	}
	return Layout{
		Authority:           authority,
		Entries:             cloneEntries(entries),
		ValueHeaderOffsets:  append([]uint64(nil), geometry.headerOffsets...),
		ValuePayloadOffsets: append([]uint64(nil), geometry.payloadOffsets...),
		FooterOffset:        geometry.footerOffset,
	}, nil
}

// AgeStoredSize applies the Config source bound before delegating universal
// canonical age geometry to backupformat.
func AgeStoredSize(sourceSize uint64) (uint64, error) {
	if sourceSize > MaxSourceBytes {
		return 0, archiveError("config source size exceeds its byte limit")
	}
	return backupformat.AgeStoredSize(sourceSize)
}

type geometry struct {
	headerOffsets  []uint64
	payloadOffsets []uint64
	footerOffset   uint64
	sourceSize     uint64
}

func computeGeometry(manifestSize uint64, entries []Entry) (geometry, error) {
	manifestRounded, ok := roundTar(manifestSize)
	if !ok {
		return geometry{}, archiveError("Config manifest padding arithmetic overflow")
	}
	offset, ok := checkedAdd(TarBlockBytes, manifestRounded)
	if !ok {
		return geometry{}, archiveError("Config layout arithmetic overflow")
	}
	result := geometry{
		headerOffsets:  make([]uint64, len(entries)),
		payloadOffsets: make([]uint64, len(entries)),
	}
	for index, entry := range entries {
		result.headerOffsets[index] = offset
		payloadOffset, addOK := checkedAdd(offset, TarBlockBytes)
		if !addOK {
			return geometry{}, archiveError("Config value offset arithmetic overflow")
		}
		result.payloadOffsets[index] = payloadOffset
		valueRounded, roundOK := roundTar(entry.Value.SizeBytes)
		if !roundOK {
			return geometry{}, archiveError("Config value padding arithmetic overflow")
		}
		offset, addOK = checkedAdd(payloadOffset, valueRounded)
		if !addOK {
			return geometry{}, archiveError("Config layout arithmetic overflow")
		}
	}
	result.footerOffset = offset
	result.sourceSize, ok = checkedAdd(offset, 2*TarBlockBytes)
	if !ok || result.sourceSize > MaxSourceBytes {
		return geometry{}, archiveError("Config source size exceeds its byte limit")
	}
	return result, nil
}

func validateEntries(entries []Entry) error {
	if len(entries) > MaxEntries {
		return archiveError("Config entry count exceeds its limit")
	}
	var total uint64
	for index := range entries {
		entry := entries[index]
		if index > 0 && entries[index-1].ID >= entry.ID {
			return archiveError("Config entries are not in unique raw-byte stable-ID order")
		}
		if err := validateEntry(entry); err != nil {
			return err
		}
		if total > MaxTotalSelectedValueBytes-entry.Value.SizeBytes {
			return archiveError("Config selected value total exceeds its byte limit")
		}
		total += entry.Value.SizeBytes
	}
	return nil
}

func validateEntry(entry Entry) error {
	if ids.Validate(ids.KindEnvEntry, entry.ID) != nil {
		return archiveError("Config Entry has an invalid stable ID")
	}
	if entry.Value.Path != "values/"+entry.ID {
		return archiveError("Config Entry value path does not match its stable ID")
	}
	if entry.Value.SizeBytes > MaxSelectedValueBytes {
		return archiveError("Config Entry selected value exceeds its byte limit")
	}

	switch entry.Metadata.Kind {
	case MetadataEnvironment:
		if !validEnvironmentKey(entry.Metadata.Environment.Key) ||
			entry.Metadata.File != (FileMetadata{}) {
			return archiveError("Config Entry environment metadata is invalid")
		}
	case MetadataFile:
		if entry.Metadata.Environment != (EnvironmentMetadata{}) ||
			len(entry.Metadata.File.Path) > 240 ||
			entrymaterialization.ValidateDesiredDestination(entry.Metadata.File.Path) != nil {
			return archiveError("Config Entry file metadata is invalid")
		}
		wantMode := uint32(0444)
		if entry.Secret {
			wantMode = 0600
		}
		if entry.Metadata.File.Mode != wantMode {
			return archiveError("Config Entry file mode does not match its secret classification")
		}
	default:
		return archiveError("Config Entry metadata kind is invalid")
	}

	switch entry.Exposure.Kind {
	case ExposureAll:
		if len(entry.Exposure.ServiceIDs) != 0 {
			return archiveError("Config Entry all-services exposure carries service IDs")
		}
	case ExposureServices:
		if len(entry.Exposure.ServiceIDs) == 0 || len(entry.Exposure.ServiceIDs) > 128 {
			return archiveError("Config Entry services exposure is empty")
		}
		for index, serviceID := range entry.Exposure.ServiceIDs {
			if ids.Validate(ids.KindService, serviceID) != nil ||
				(index > 0 && entry.Exposure.ServiceIDs[index-1] >= serviceID) {
				return archiveError("Config Entry service exposure is invalid or unsorted")
			}
		}
	default:
		return archiveError("Config Entry exposure kind is invalid")
	}

	switch entry.Source.Kind {
	case SourceLiteral:
		if entry.Source.SecretReference != (SecretReference{}) ||
			entry.Source.Fact != (FactReference{}) {
			return archiveError("Config literal source is invalid")
		}
	case SourceSecretReference:
		if !entry.Secret || entry.Source.Fact != (FactReference{}) ||
			entry.Source.SecretReference.AuthoredKey == "" ||
			!utf8.ValidString(entry.Source.SecretReference.AuthoredKey) ||
			bytes.IndexByte([]byte(entry.Source.SecretReference.AuthoredKey), 0) >= 0 {
			return archiveError("Config secret-reference source is invalid")
		}
	case SourceFact:
		if entry.Source.SecretReference != (SecretReference{}) ||
			ids.Validate(ids.KindAttach, entry.Source.Fact.AttachID) != nil ||
			entry.Source.Fact.Fact == "" || !utf8.ValidString(entry.Source.Fact.Fact) ||
			bytes.IndexByte([]byte(entry.Source.Fact.Fact), 0) >= 0 {
			return archiveError("Config fact source is invalid")
		}
		if entry.Source.Fact.GrantAttachID != "" &&
			ids.Validate(ids.KindAttach, entry.Source.Fact.GrantAttachID) != nil {
			return archiveError("Config fact grant stable ID is invalid")
		}
	default:
		return archiveError("Config Entry source kind is invalid")
	}
	return nil
}

func validEnvironmentKey(value string) bool {
	if len(value) == 0 || len(value) > 255 {
		return false
	}
	for index := 0; index < len(value); index++ {
		character := value[index]
		if index == 0 {
			if character != '_' && (character < 'A' || character > 'Z') &&
				(character < 'a' || character > 'z') {
				return false
			}
			continue
		}
		if character != '_' && (character < 'A' || character > 'Z') &&
			(character < 'a' || character > 'z') && (character < '0' || character > '9') {
			return false
		}
	}
	return true
}

func appendManifestEntry(destination []byte, entry Entry) []byte {
	destination = append(destination, `{"exposure":`...)
	destination = appendExposure(destination, entry.Exposure)
	destination = append(destination, `,"id":`...)
	destination = appendJSONString(destination, entry.ID)
	destination = append(destination, `,"metadata":`...)
	destination = appendMetadata(destination, entry.Metadata)
	destination = append(destination, `,"secret":`...)
	destination = strconv.AppendBool(destination, entry.Secret)
	destination = append(destination, `,"source":`...)
	destination = appendSource(destination, entry.Source)
	destination = append(destination, `,"value":{"path":`...)
	destination = appendJSONString(destination, entry.Value.Path)
	destination = append(destination, `,"sha256":`...)
	destination = appendJSONString(destination, hex.EncodeToString(entry.Value.SHA256[:]))
	destination = append(destination, `,"size_bytes":`...)
	destination = strconv.AppendUint(destination, entry.Value.SizeBytes, 10)
	destination = append(destination, "}}"...)
	return destination
}

func appendMetadata(destination []byte, metadata Metadata) []byte {
	if metadata.Kind == MetadataEnvironment {
		destination = append(destination, `{"key":`...)
		destination = appendJSONString(destination, metadata.Environment.Key)
		return append(destination, `,"type":"env"}`...)
	}
	destination = append(destination, `{"gid":`...)
	destination = strconv.AppendUint(destination, uint64(metadata.File.GID), 10)
	destination = append(destination, `,"mode":`...)
	destination = strconv.AppendUint(destination, uint64(metadata.File.Mode), 10)
	destination = append(destination, `,"path":`...)
	destination = appendJSONString(destination, metadata.File.Path)
	destination = append(destination, `,"type":"file","uid":`...)
	destination = strconv.AppendUint(destination, uint64(metadata.File.UID), 10)
	return append(destination, '}')
}

func appendExposure(destination []byte, exposure Exposure) []byte {
	if exposure.Kind == ExposureAll {
		return append(destination, `{"kind":"all"}`...)
	}
	destination = append(destination, `{"kind":"services","service_ids":[`...)
	for index, serviceID := range exposure.ServiceIDs {
		if index != 0 {
			destination = append(destination, ',')
		}
		destination = appendJSONString(destination, serviceID)
	}
	return append(destination, "]}"...)
}

func appendSource(destination []byte, source Source) []byte {
	switch source.Kind {
	case SourceLiteral:
		return append(destination, `{"kind":"literal"}`...)
	case SourceSecretReference:
		destination = append(destination, `{"kind":"secret_ref","secret_ref":`...)
		destination = appendJSONString(destination, source.SecretReference.AuthoredKey)
		return append(destination, '}')
	default:
		destination = append(destination, `{"attach_id":`...)
		destination = appendJSONString(destination, source.Fact.AttachID)
		destination = append(destination, `,"fact":`...)
		destination = appendJSONString(destination, source.Fact.Fact)
		if source.Fact.GrantAttachID != "" {
			destination = append(destination, `,"grant_attach_id":`...)
			destination = appendJSONString(destination, source.Fact.GrantAttachID)
		}
		return append(destination, `,"kind":"fact"}`...)
	}
}

func appendJSONString(destination []byte, value string) []byte {
	destination = append(destination, '"')
	for _, character := range value {
		switch character {
		case '"', '\\':
			destination = append(destination, '\\', byte(character))
		case '\b':
			destination = append(destination, `\b`...)
		case '\t':
			destination = append(destination, `\t`...)
		case '\n':
			destination = append(destination, `\n`...)
		case '\f':
			destination = append(destination, `\f`...)
		case '\r':
			destination = append(destination, `\r`...)
		default:
			if character < 0x20 {
				destination = append(destination, `\u00`...)
				const hexadecimal = "0123456789abcdef"
				destination = append(
					destination,
					hexadecimal[byte(character)>>4],
					hexadecimal[byte(character)&15],
				)
			} else {
				destination = utf8.AppendRune(destination, character)
			}
		}
	}
	return append(destination, '"')
}

type manifestDocument struct {
	Entries []manifestEntry `json:"entries"`
	Format  string          `json:"format"`
}

type manifestEntry struct {
	Exposure json.RawMessage `json:"exposure"`
	ID       string          `json:"id"`
	Metadata json.RawMessage `json:"metadata"`
	Secret   bool            `json:"secret"`
	Source   json.RawMessage `json:"source"`
	Value    manifestValue   `json:"value"`
}

type manifestValue struct {
	Path      string `json:"path"`
	SHA256    string `json:"sha256"`
	SizeBytes uint64 `json:"size_bytes"`
}

func parseManifest(ctx context.Context, payload []byte) ([]Entry, error) {
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	if len(payload) > MaxManifestBytes {
		return nil, archiveError("Config manifest exceeds its byte limit")
	}
	var document manifestDocument
	if json.Unmarshal(payload, &document) != nil || document.Format != Format {
		return nil, archiveError("Config manifest JSON or format is invalid")
	}
	if len(document.Entries) > MaxEntries {
		return nil, archiveError("Config manifest entry count exceeds its limit")
	}
	entries := make([]Entry, len(document.Entries))
	for index, encoded := range document.Entries {
		if err := checkContext(ctx); err != nil {
			return nil, err
		}
		entry, err := parseManifestEntry(encoded)
		if err != nil {
			return nil, err
		}
		entries[index] = entry
	}
	canonical, _, err := buildManifestPayload(ctx, entries)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(payload, canonical) {
		return nil, archiveError("Config manifest is not exact canonical JCS")
	}
	return entries, nil
}

func parseManifestEntry(encoded manifestEntry) (Entry, error) {
	digestBytes, err := hex.DecodeString(encoded.Value.SHA256)
	if err != nil || len(digestBytes) != sha256.Size ||
		hex.EncodeToString(digestBytes) != encoded.Value.SHA256 {
		return Entry{}, archiveError(
			"Config manifest value digest is not canonical lowercase SHA-256",
		)
	}
	entry := Entry{
		ID:     encoded.ID,
		Secret: encoded.Secret,
		Value: ValueEvidence{
			Path:      encoded.Value.Path,
			SizeBytes: encoded.Value.SizeBytes,
		},
	}
	copy(entry.Value.SHA256[:], digestBytes)
	if err := parseMetadata(encoded.Metadata, &entry); err != nil {
		return Entry{}, err
	}
	if err := parseExposure(encoded.Exposure, &entry); err != nil {
		return Entry{}, err
	}
	if err := parseSource(encoded.Source, &entry); err != nil {
		return Entry{}, err
	}
	return entry, nil
}

func cloneEntries(entries []Entry) []Entry {
	result := make([]Entry, len(entries))
	copy(result, entries)
	for index := range result {
		result[index].Exposure.ServiceIDs = append(
			[]string(nil),
			entries[index].Exposure.ServiceIDs...)
	}
	return result
}

func roundTar(value uint64) (uint64, bool) {
	remainder := value % TarBlockBytes
	if remainder == 0 {
		return value, true
	}
	return checkedAdd(value, TarBlockBytes-remainder)
}

func checkedAdd(left, right uint64) (uint64, bool) {
	if left > ^uint64(0)-right {
		return 0, false
	}
	return left + right, true
}

func archiveError(message string) error {
	return errs.New(errs.KindInternal, fmt.Sprintf("backup Config archive: %s", message))
}

func checkContext(ctx context.Context) error {
	if ctx == nil {
		return archiveError("operation context is nil")
	}
	select {
	case <-ctx.Done():
		return archiveCause("operation was canceled", ctx.Err())
	default:
		return nil
	}
}

func archiveCause(message string, cause error) error {
	return errs.Wrap(errs.KindInternal, fmt.Errorf("backup Config archive: %s: %w", message, cause))
}

func clearBytes(value []byte) {
	for index := range value {
		value[index] = 0
	}
}
