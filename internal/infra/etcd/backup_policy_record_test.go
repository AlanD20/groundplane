package etcd

import (
	"bytes"
	"errors"
	"slices"
	"testing"
	"time"

	"filippo.io/age"

	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: persistence round trips must preserve every configured policy
// field and the source order used by public projections.
func TestBackupPolicyRecordRoundTripsCompleteEnabledState(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 8, 23, 12, 0, 0, 0, time.UTC)
	record := BackupPolicyRecord{
		EnvironmentID: "env_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		Enabled:       true, Frequency: "*-*-* 03:15:00", Keep: 7, Encryption: "age",
		ConnectorID: "con_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		SourceIDs: []string{
			"spt_01ARZ3NDEKTSV4RRFFQ69G5FAV",
			"spt_01ARZ3NDEKTSV4RRFFQ69G5FAW",
		},
		UpdatedAt: at,
	}
	encoded, err := encodeBackupPolicyRecord(record)
	if err != nil {
		t.Fatalf("encodeBackupPolicyRecord() error = %v", err)
	}
	decoded, err := decodeBackupPolicyRecord(encoded)
	if err != nil {
		t.Fatalf("decodeBackupPolicyRecord() error = %v", err)
	}
	if decoded.EnvironmentID != record.EnvironmentID || decoded.Enabled != record.Enabled ||
		decoded.Frequency != record.Frequency || decoded.Keep != record.Keep ||
		decoded.Encryption != record.Encryption || decoded.ConnectorID != record.ConnectorID ||
		!slices.Equal(decoded.SourceIDs, record.SourceIDs) || !decoded.UpdatedAt.Equal(record.UpdatedAt) {
		t.Fatalf("decoded = %#v, want %#v", decoded, record)
	}
}

// Rationale: invalid enabled records and duplicate source identities must never
// become durable policy state.
func TestBackupPolicyRecordRejectsIncompleteEnabledAndDuplicateSources(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 8, 23, 12, 0, 0, 0, time.UTC)
	sourceID := "spt_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	for name, record := range map[string]BackupPolicyRecord{
		"incomplete": {
			EnvironmentID: "env_01ARZ3NDEKTSV4RRFFQ69G5FAV", Enabled: true, UpdatedAt: at,
		},
		"duplicate source": {
			EnvironmentID: "env_01ARZ3NDEKTSV4RRFFQ69G5FAV",
			Enabled:       true, Frequency: "daily", Keep: 1, Encryption: "none",
			ConnectorID: "con_01ARZ3NDEKTSV4RRFFQ69G5FAV",
			SourceIDs:   []string{sourceID, sourceID}, UpdatedAt: at,
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if err := validateBackupPolicyRecord(record); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
				t.Fatalf("validateBackupPolicyRecord() error = %v, want validation failure", err)
			}
		})
	}
}

// Rationale: each persisted source kind must retain its exact stable target
// identity grammar, including Environment identity for config.
func TestBackupSourceRecordEnforcesKindSpecificStableTarget(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 8, 23, 12, 0, 0, 0, time.UTC)
	base := BackupSourceRecord{
		ID: "spt_01ARZ3NDEKTSV4RRFFQ69G5FAV", EnvironmentID: "env_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		CreatedAt: at,
	}
	for name, source := range map[string]BackupSourceRecord{
		"attach": func() BackupSourceRecord {
			source := base
			source.Kind = core.BackupSourceAttach
			source.TargetID = "att_01ARZ3NDEKTSV4RRFFQ69G5FAV"
			return source
		}(),
		"volume": func() BackupSourceRecord {
			source := base
			source.Kind = core.BackupSourceVolume
			source.TargetID = "vol_01ARZ3NDEKTSV4RRFFQ69G5FAV"
			return source
		}(),
		"config": func() BackupSourceRecord {
			source := base
			source.Kind = core.BackupSourceConfig
			source.TargetID = source.EnvironmentID
			return source
		}(),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if err := validateBackupSourceRecord(source); err != nil {
				t.Fatalf("validateBackupSourceRecord() error = %v", err)
			}
		})
	}
	invalid := base
	invalid.Kind = core.BackupSourceConfig
	invalid.TargetID = "env_01ARZ3NDEKTSV4RRFFQ69G5FAW"
	if err := validateBackupSourceRecord(invalid); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("validateBackupSourceRecord(invalid config) error = %v", err)
	}
}

// Rationale: public key metadata and encrypted private material must remain
// separate records with lossless ciphertext encoding.
func TestBackupKeyRecordsSeparatePublicMetadataFromCiphertext(t *testing.T) {
	t.Parallel()
	identity, err := age.GenerateX25519Identity()
	if err != nil {
		t.Fatalf("age.GenerateX25519Identity() error = %v", err)
	}
	at := time.Date(2026, 8, 23, 12, 0, 0, 0, time.UTC)
	metadata := BackupKeyRecord{
		EnvironmentID: "env_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		Recipient:     identity.Recipient().String(), KeyEra: 1, CreatedAt: at, RotatedAt: at,
	}
	if _, err := encodeBackupKeyRecord(metadata); err != nil {
		t.Fatalf("encodeBackupKeyRecord() error = %v", err)
	}
	value := BackupKeyEncryptedValue{
		EnvironmentID: metadata.EnvironmentID, KeyEra: metadata.KeyEra,
		Ciphertext: []byte("controller-key-wrapped-identity"),
	}
	encoded, err := encodeBackupKeyEncryptedValue(value)
	if err != nil {
		t.Fatalf("encodeBackupKeyEncryptedValue() error = %v", err)
	}
	decoded, err := decodeBackupKeyEncryptedValue(encoded)
	if err != nil {
		t.Fatalf("decodeBackupKeyEncryptedValue() error = %v", err)
	}
	defer clear(decoded.Ciphertext)
	if decoded.EnvironmentID != value.EnvironmentID || decoded.KeyEra != value.KeyEra ||
		!bytes.Equal(decoded.Ciphertext, value.Ciphertext) {
		t.Fatalf("decoded = %#v, want %#v", decoded, value)
	}
}
