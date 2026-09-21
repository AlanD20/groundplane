package backuppolicy_test

import (
	"bytes"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"filippo.io/age"

	"github.com/AlanD20/groundplane/internal/core"
	testbackuppolicy "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: persistence round trips must preserve every configured policy
// field and the source order used by public projections.
func TestBackupPolicyRecordRoundTripsCompleteEnabledState(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 8, 23, 12, 0, 0, 0, time.UTC)
	record := testbackuppolicy.BackupPolicyRecord{
		EnvironmentID: "env_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		Enabled:       true, Frequency: "*-*-* 03:15:00", Keep: 7, Encryption: "age",
		ConnectorID: "con_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		SourceIDs: []string{
			"spt_01ARZ3NDEKTSV4RRFFQ69G5FAV",
			"spt_01ARZ3NDEKTSV4RRFFQ69G5FAW",
		},
		UpdatedAt: at,
	}
	encoded, err := testbackuppolicy.EncodeBackupPolicyRecord(record)
	if err != nil {
		t.Fatalf("encodeBackupPolicyRecord() error = %v", err)
	}
	decoded, err := testbackuppolicy.DecodeBackupPolicyRecord(encoded)
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

// Rationale: durable policy JSON must preserve the public retention maximum
// without depending on machine word size and reject the next integer.
func TestBackupPolicyRecordEnforcesMaximumPublicKeep(t *testing.T) {
	t.Parallel()
	record := testbackuppolicy.BackupPolicyRecord{
		EnvironmentID: "env_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		Enabled:       true,
		Frequency:     "*-*-* 03:15:00",
		Keep:          testbackuppolicy.MaximumBackupPolicyKeep,
		Encryption:    "none",
		ConnectorID:   "con_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		SourceIDs:     []string{"spt_01ARZ3NDEKTSV4RRFFQ69G5FAV"},
		UpdatedAt:     time.Date(2026, 8, 23, 12, 0, 0, 0, time.UTC),
	}
	encoded, err := testbackuppolicy.EncodeBackupPolicyRecord(record)
	if err != nil {
		t.Fatalf("encodeBackupPolicyRecord(maximum Keep) error = %v", err)
	}
	decoded, err := testbackuppolicy.DecodeBackupPolicyRecord(encoded)
	if err != nil {
		t.Fatalf("decodeBackupPolicyRecord(maximum Keep) error = %v", err)
	}
	if decoded.Keep != testbackuppolicy.MaximumBackupPolicyKeep {
		t.Fatalf("decoded Keep = %d, want %d", decoded.Keep, testbackuppolicy.MaximumBackupPolicyKeep)
	}
	record.Keep++
	if _, err := testbackuppolicy.EncodeBackupPolicyRecord(record); !errors.Is(
		err, errs.New(errs.KindValidationFailed, ""),
	) {
		t.Fatalf("encodeBackupPolicyRecord(Keep above maximum) error = %v", err)
	}
}

// Rationale: a disabled durable singleton is either wholly unconfigured or a
// complete retained configuration; every partial presence shape is corrupt.
func TestBackupPolicyRecordRejectsEveryPartialDisabledConfiguration(t *testing.T) {
	t.Parallel()
	const (
		frequencyField = 1 << iota
		keepField
		encryptionField
		connectorField
		sourcesField
		allConfiguredFields = frequencyField | keepField | encryptionField | connectorField | sourcesField
	)
	at := time.Date(2026, 8, 23, 12, 0, 0, 0, time.UTC)
	base := testbackuppolicy.BackupPolicyRecord{
		EnvironmentID: "env_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		UpdatedAt:     at,
	}
	if err := testbackuppolicy.ValidateBackupPolicyRecord(base); err != nil {
		t.Fatalf("validateBackupPolicyRecord(unconfigured disabled) error = %v", err)
	}
	for fields := 1; fields < allConfiguredFields; fields++ {
		fields := fields
		t.Run(fmt.Sprintf("fields_%05b", fields), func(t *testing.T) {
			t.Parallel()
			record := base
			if fields&frequencyField != 0 {
				record.Frequency = "*-*-* 03:15:00"
			}
			if fields&keepField != 0 {
				record.Keep = 1
			}
			if fields&encryptionField != 0 {
				record.Encryption = "none"
			}
			if fields&connectorField != 0 {
				record.ConnectorID = "con_01ARZ3NDEKTSV4RRFFQ69G5FAV"
			}
			if fields&sourcesField != 0 {
				record.SourceIDs = []string{"spt_01ARZ3NDEKTSV4RRFFQ69G5FAV"}
			}
			if fields == allConfiguredFields&^sourcesField {
				if err := testbackuppolicy.ValidateBackupPolicyRecord(record); err != nil {
					t.Fatalf("retained disabled configuration without sources: %v", err)
				}
				return
			}
			if err := testbackuppolicy.ValidateBackupPolicyRecord(record); !errors.Is(
				err,
				errs.New(errs.KindValidationFailed, ""),
			) {
				t.Fatalf("validateBackupPolicyRecord(partial fields %05b) error = %v", fields, err)
			}
		})
	}
	configured := base
	configured.Frequency = "*-*-* 03:15:00"
	configured.Keep = 1
	configured.Encryption = "none"
	configured.ConnectorID = "con_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	configured.SourceIDs = []string{"spt_01ARZ3NDEKTSV4RRFFQ69G5FAV"}
	if err := testbackuppolicy.ValidateBackupPolicyRecord(configured); err != nil {
		t.Fatalf("validateBackupPolicyRecord(configured disabled) error = %v", err)
	}
}

// Rationale: ADR0049 retains policy configuration after the last Volume source
// is removed, but an enabled policy still requires a selected source.
func TestBackupPolicyRecordRetainsConfigurationAfterLastVolumeSource(t *testing.T) {
	t.Parallel()
	record := testbackuppolicy.BackupPolicyRecord{
		EnvironmentID: "env_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		Frequency:     "*-*-* 03:15:00", Keep: 7, Encryption: "age",
		ConnectorID: "con_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		UpdatedAt:   time.Date(2026, 9, 9, 12, 0, 0, 0, time.UTC),
	}
	encoded, err := testbackuppolicy.EncodeBackupPolicyRecord(record)
	if err != nil {
		t.Fatalf("encode retained policy after last source removal: %v", err)
	}
	defer clear(encoded)
	decoded, err := testbackuppolicy.DecodeBackupPolicyRecord(encoded)
	if err != nil || decoded.Enabled || len(decoded.SourceIDs) != 0 ||
		decoded.Frequency != record.Frequency || decoded.Keep != record.Keep ||
		decoded.Encryption != record.Encryption || decoded.ConnectorID != record.ConnectorID ||
		decoded.EnvironmentID != record.EnvironmentID || !decoded.UpdatedAt.Equal(record.UpdatedAt) {
		t.Fatalf("retained policy did not survive persistence: %v", err)
	}
	record.Enabled = true
	if _, err := testbackuppolicy.EncodeBackupPolicyRecord(record); !errors.Is(
		err,
		errs.New(errs.KindValidationFailed, ""),
	) {
		t.Fatalf("enabled policy without a source accepted: %v", err)
	}
}

// Rationale: negative retention, invalid enabled records, and duplicate source
// identities must never become durable policy state.
func TestBackupPolicyRecordRejectsIncompleteEnabledAndDuplicateSources(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 8, 23, 12, 0, 0, 0, time.UTC)
	sourceID := "spt_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	for name, record := range map[string]testbackuppolicy.BackupPolicyRecord{
		"negative retention": {
			EnvironmentID: "env_01ARZ3NDEKTSV4RRFFQ69G5FAV", Keep: -1, UpdatedAt: at,
		},
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
			if err := testbackuppolicy.ValidateBackupPolicyRecord(record); !errors.Is(
				err,
				errs.New(errs.KindValidationFailed, ""),
			) {
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
	base := testbackuppolicy.BackupSourceRecord{
		ID: "spt_01ARZ3NDEKTSV4RRFFQ69G5FAV", EnvironmentID: "env_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		CreatedAt: at,
	}
	for name, source := range map[string]testbackuppolicy.BackupSourceRecord{
		"attach": func() testbackuppolicy.BackupSourceRecord {
			source := base
			source.Kind = core.BackupSourceAttach
			source.TargetID = "att_01ARZ3NDEKTSV4RRFFQ69G5FAV"
			return source
		}(),
		"volume": func() testbackuppolicy.BackupSourceRecord {
			source := base
			source.Kind = core.BackupSourceVolume
			source.TargetID = "vol_01ARZ3NDEKTSV4RRFFQ69G5FAV"
			return source
		}(),
		"config": func() testbackuppolicy.BackupSourceRecord {
			source := base
			source.Kind = core.BackupSourceConfig
			source.TargetID = source.EnvironmentID
			return source
		}(),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if err := testbackuppolicy.ValidateBackupSourceRecord(source); err != nil {
				t.Fatalf("validateBackupSourceRecord() error = %v", err)
			}
		})
	}
	invalid := base
	invalid.Kind = core.BackupSourceConfig
	invalid.TargetID = "env_01ARZ3NDEKTSV4RRFFQ69G5FAW"
	if err := testbackuppolicy.ValidateBackupSourceRecord(invalid); !errors.Is(
		err,
		errs.New(errs.KindValidationFailed, ""),
	) {
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
	metadata := testbackuppolicy.BackupKeyRecord{
		EnvironmentID: "env_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		Recipient:     identity.Recipient().String(), KeyEra: 1, CreatedAt: at, RotatedAt: at,
	}
	if _, err := testbackuppolicy.EncodeBackupKeyRecord(metadata); err != nil {
		t.Fatalf("encodeBackupKeyRecord() error = %v", err)
	}
	value := testbackuppolicy.BackupKeyEncryptedValue{
		EnvironmentID: metadata.EnvironmentID, KeyEra: metadata.KeyEra,
		Ciphertext: []byte("controller-key-wrapped-identity"),
	}
	encoded, err := testbackuppolicy.EncodeBackupKeyEncryptedValue(value)
	if err != nil {
		t.Fatalf("encodeBackupKeyEncryptedValue() error = %v", err)
	}
	decoded, err := testbackuppolicy.DecodeBackupKeyEncryptedValue(encoded)
	if err != nil {
		t.Fatalf("decodeBackupKeyEncryptedValue() error = %v", err)
	}
	defer clear(decoded.Ciphertext)
	if decoded.EnvironmentID != value.EnvironmentID || decoded.KeyEra != value.KeyEra ||
		!bytes.Equal(decoded.Ciphertext, value.Ciphertext) {
		t.Fatalf("decoded = %#v, want %#v", decoded, value)
	}
}
