package backupconfig

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
)

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
