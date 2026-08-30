// Package backupobject defines the provider-neutral immutable Backup object
// contract. Provider SDK types belong in infrastructure implementations.
package backupobject

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/internal/common/backupformat"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	MaxObjectSize         uint64 = backupformat.MaxStoredBytes
	maxDiscriminatorBytes        = 1024

	metadataFormatVersion = "groundplane-format-version"
	metadataEnvironmentID = "groundplane-environment-id"
	metadataSourceID      = "groundplane-source-id"
	metadataRecoveryPoint = "groundplane-recovery-point-id"
	metadataSourceFormat  = "groundplane-source-format"
	metadataEncryption    = "groundplane-encryption"
	metadataKeyEra        = "groundplane-key-era"
	metadataSourceSize    = "groundplane-source-size-bytes"
	metadataSourceSHA256  = "groundplane-source-sha256"
	metadataStoredSize    = "groundplane-stored-size-bytes"
	metadataStoredSHA256  = "groundplane-stored-sha256"
	artifactFilename      = "artifact.bin"
)

type SourceFormat string

const (
	SourceFormatEnvironmentConfig SourceFormat = "environment-config-v1"
	SourceFormatVolumeTar         SourceFormat = "volume-tar-v1"
	SourceFormatPostgresCustom    SourceFormat = "postgres-custom-v1"
)

type Encryption string

const (
	EncryptionAge  Encryption = "age"
	EncryptionNone Encryption = "none"
)

// Evidence is the four-part source/stored artifact evidence fixed by ADR 0047.
type Evidence struct {
	SourceSizeBytes uint64
	SourceSHA256    [sha256.Size]byte
	StoredSizeBytes uint64
	StoredSHA256    [sha256.Size]byte
}

// Artifact is the complete provider-neutral immutable object expectation.
// Key is private execution data and must equal the deterministic key derived
// from the configured Connector prefix and the three stable ids.
type Artifact struct {
	Key             string
	EnvironmentID   string
	SourceID        string
	RecoveryPointID string
	SourceFormat    SourceFormat
	Encryption      Encryption
	KeyEra          *uint64
	Evidence        Evidence
}

func (artifact Artifact) Validate() error {
	if ids.Validate(ids.KindEnvironment, artifact.EnvironmentID) != nil ||
		ids.Validate(ids.KindBackupSource, artifact.SourceID) != nil ||
		ids.Validate(ids.KindRecoveryPoint, artifact.RecoveryPointID) != nil {
		return errs.New(errs.KindValidationFailed, "backup object stable identity is invalid")
	}
	if artifact.Key == "" {
		return errs.New(errs.KindValidationFailed, "backup object key is required")
	}
	if err := validateKey(artifact.Key); err != nil {
		return err
	}
	switch artifact.SourceFormat {
	case SourceFormatEnvironmentConfig, SourceFormatVolumeTar, SourceFormatPostgresCustom:
	default:
		return errs.New(errs.KindValidationFailed, "backup object source format is invalid")
	}
	switch artifact.Encryption {
	case EncryptionAge:
		if artifact.KeyEra == nil {
			return errs.New(errs.KindValidationFailed, "backup object age encryption requires a key era")
		}
		if *artifact.KeyEra == 0 {
			return errs.New(errs.KindValidationFailed, "backup object age encryption requires a positive key era")
		}
	case EncryptionNone:
		if artifact.KeyEra != nil {
			return errs.New(errs.KindValidationFailed, "backup object without encryption must omit the key era")
		}
		if artifact.Evidence.SourceSizeBytes != artifact.Evidence.StoredSizeBytes ||
			artifact.Evidence.SourceSHA256 != artifact.Evidence.StoredSHA256 {
			return errs.New(
				errs.KindValidationFailed,
				"backup object without encryption requires equal source and stored evidence",
			)
		}
	default:
		return errs.New(errs.KindValidationFailed, "backup object encryption is invalid")
	}
	if artifact.Evidence.StoredSizeBytes > MaxObjectSize {
		return errs.New(errs.KindValidationFailed, "backup object exceeds the 5 TiB maximum")
	}
	return nil
}

func validateKey(key string) error {
	if !utf8.ValidString(key) {
		return errs.New(errs.KindValidationFailed, "backup object key must be valid UTF-8")
	}
	if len(key) > 1024 {
		return errs.New(errs.KindValidationFailed, "backup object key exceeds 1024 bytes")
	}
	if strings.ContainsRune(key, '\x00') || strings.ContainsRune(key, '\\') ||
		strings.HasPrefix(key, "/") || strings.HasSuffix(key, "/") {
		return errs.New(errs.KindValidationFailed, "backup object key is not canonical")
	}
	for _, segment := range strings.Split(key, "/") {
		if segment == "" || segment == "." || segment == ".." {
			return errs.New(errs.KindValidationFailed, "backup object key is not canonical")
		}
	}
	return nil
}

// ExpectedKey derives the sole object key authorized for the artifact. Prefix
// is already normalized by the Connector contract: empty or slash-terminated.
func (artifact Artifact) ExpectedKey(prefix string) string {
	return prefix + artifact.EnvironmentID + "/" + artifact.SourceID + "/" +
		artifact.RecoveryPointID + "/" + artifactFilename
}

// Metadata returns a fresh map containing the complete version-1 S3 user
// metadata set. There is no compatibility key for the former ambiguous digest.
func (artifact Artifact) Metadata() map[string]string {
	metadata := map[string]string{
		metadataFormatVersion: "1",
		metadataEnvironmentID: artifact.EnvironmentID,
		metadataSourceID:      artifact.SourceID,
		metadataRecoveryPoint: artifact.RecoveryPointID,
		metadataSourceFormat:  string(artifact.SourceFormat),
		metadataEncryption:    string(artifact.Encryption),
		metadataSourceSize:    strconv.FormatUint(artifact.Evidence.SourceSizeBytes, 10),
		metadataSourceSHA256:  hex.EncodeToString(artifact.Evidence.SourceSHA256[:]),
		metadataStoredSize:    strconv.FormatUint(artifact.Evidence.StoredSizeBytes, 10),
		metadataStoredSHA256:  hex.EncodeToString(artifact.Evidence.StoredSHA256[:]),
	}
	if artifact.KeyEra != nil {
		metadata[metadataKeyEra] = strconv.FormatUint(*artifact.KeyEra, 10)
	}
	return metadata
}

type DiscriminatorKind string

const (
	DiscriminatorVersionID DiscriminatorKind = "version_id"
	DiscriminatorETag      DiscriminatorKind = "etag"
)

// Discriminator preserves VersionId presence separately from its literal
// value. In particular, version_id:"null" is not an absent VersionId.
type Discriminator struct {
	Kind  DiscriminatorKind
	Value string
}

func (discriminator Discriminator) Validate() error {
	switch discriminator.Kind {
	case DiscriminatorVersionID:
		if !utf8.ValidString(discriminator.Value) || len(discriminator.Value) > maxDiscriminatorBytes {
			return errs.New(errs.KindValidationFailed, "backup object VersionId discriminator is invalid")
		}
		return nil
	case DiscriminatorETag:
		if discriminator.Value == "" || !utf8.ValidString(discriminator.Value) ||
			len(discriminator.Value) > maxDiscriminatorBytes {
			return errs.New(errs.KindValidationFailed, "backup object ETag discriminator is invalid")
		}
		return nil
	default:
		return errs.New(errs.KindValidationFailed, "backup object discriminator kind is invalid")
	}
}

type Object struct {
	Artifact      Artifact
	Discriminator Discriminator
}

func (object Object) Validate() error {
	if err := object.Artifact.Validate(); err != nil {
		return err
	}
	return object.Discriminator.Validate()
}

// HeadResult represents authoritative absence without introducing another
// error type. Object is populated only when Present is true.
type HeadResult struct {
	Present bool
	Object  Object
}

// Store is the narrow Agent-side Backup object data-plane contract.
type Store interface {
	PutExact(ctx context.Context, artifact Artifact, source io.ReaderAt) (Object, error)
	HeadExact(ctx context.Context, artifact Artifact, expected *Discriminator) (HeadResult, error)
	GetExact(ctx context.Context, object Object, destination io.Writer) error
	DeleteExact(ctx context.Context, object Object) error
}
