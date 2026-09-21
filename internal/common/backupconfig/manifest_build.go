package backupconfig

import (
	"context"
	"crypto/sha256"
)

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
