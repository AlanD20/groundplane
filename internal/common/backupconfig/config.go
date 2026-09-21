// Package backupconfig implements the canonical environment-config-v1 backup
// artifact. It is deliberately independent of the Agent protocol: callers
// provide protocol metadata bytes separately, while this package owns the
// byte-complete manifest, USTAR layout, and content authority.
package backupconfig

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
