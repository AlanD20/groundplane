package backupconfig

import (
	"context"
	"crypto/sha256"
	"io"
)

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
