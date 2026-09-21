package backupconfig

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/backupformat"
)

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
