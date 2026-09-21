package backupconfig

import (
	"bytes"
	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"unicode/utf8"
)

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
