package backupconfig

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"testing"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// L0 - pure byte-contract tests. See docs/standards.md, section 13.

// Rationale: the empty archive is a byte-complete interoperability vector,
// including exact JCS, USTAR checksum spelling, footer, and source digest.
func TestEmptyArtifactMatchesGoldenVector(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	manifest, authority, metadata, err := BuildManifest(ctx, TransferCapture, nil, nil)
	if err != nil {
		t.Fatalf("BuildManifest(): %v", err)
	}
	if got, want := string(manifest), `{"entries":[],"format":"environment-config-v1"}`; got != want {
		t.Fatalf("manifest = %q, want %q", got, want)
	}
	assertHexDigest(t, authority.ManifestSHA256, "b42cc47ac095a7216a02db99c618694c48e69f0e9c230731c6c8f04943c5c37a")
	if authority.EntryCount != 0 || authority.TotalSelectedValueBytes != 0 ||
		authority.ManifestSizeBytes != 47 || authority.SourceSizeBytes != 2048 {
		t.Fatalf("authority = %#v", authority)
	}
	layout, err := ComputeLayout(ctx, authority, nil)
	if err != nil {
		t.Fatalf("ComputeLayout(): %v", err)
	}
	destination := newMemoryWriter(authority.SourceSizeBytes)
	if err := WriteArtifact(ctx, destination, manifest, layout, metadata, nil); err != nil {
		t.Fatalf("WriteArtifact(): %v", err)
	}
	if got, want := destination.bytes[148:156], []byte{'0', '1', '1', '7', '0', '6', 0, ' '}; !bytes.Equal(got, want) {
		t.Fatalf("manifest checksum = %q, want %q", got, want)
	}
	sourceDigest := sha256.Sum256(destination.bytes)
	assertHexDigest(t, sourceDigest, "0b726da4797d0420a45848a567ed63cb4798a2f8c688a3340e01f9a4fbc6904f")
	validated, err := ValidateArtifact(ctx, bytes.NewReader(destination.bytes), SourceEvidence{
		SizeBytes: uint64(len(destination.bytes)), SHA256: sourceDigest,
	}, t.TempDir(), nil)
	if err != nil {
		t.Fatalf("ValidateArtifact(): %v", err)
	}
	if validated.Layout().Authority != authority || validated.SourceSHA256() != sourceDigest {
		t.Fatal("validated authority differs from the golden archive")
	}
	if err := validated.Close(ctx); err != nil {
		t.Fatalf("Close(): %v", err)
	}
}

// Rationale: a populated vector locks the exact value member offset, mode,
// header checksum, selected bytes, padding, and full-source digest.
func TestOneEntryArtifactMatchesGoldenVectorAndStreamsPassTwo(t *testing.T) {
	t.Parallel()

	value := []byte("x\n")
	entry := literalEnvironmentEntry(value)
	ctx := context.Background()
	manifest, authority, metadata, err := BuildManifest(ctx, TransferCapture, []Entry{entry}, testMetadataEncoder)
	if err != nil {
		t.Fatalf("BuildManifest(): %v", err)
	}
	const wantManifest = `{"entries":[{"exposure":{"kind":"all"},"id":"ev_00000000000000000000000000","metadata":{"key":"A","type":"env"},"secret":false,"source":{"kind":"literal"},"value":{"path":"values/ev_00000000000000000000000000","sha256":"73cb3858a687a8494ca3323053016282f3dad39d42cf62ca4e79dda2aac7d9ac","size_bytes":2}}],"format":"environment-config-v1"}`
	if string(manifest) != wantManifest {
		t.Fatalf("manifest = %s", manifest)
	}
	assertHexDigest(t, authority.ManifestSHA256, "22013c553b00ffc3435b7e4fb5eff89c1e96ba7efb3d2cba2bf7b8d9448e4f66")
	if authority.ManifestSizeBytes != 337 || authority.EntryCount != 1 ||
		authority.TotalSelectedValueBytes != 2 || authority.SourceSizeBytes != 3072 {
		t.Fatalf("authority = %#v", authority)
	}
	layout, err := ComputeLayout(ctx, authority, []Entry{entry})
	if err != nil {
		t.Fatalf("ComputeLayout(): %v", err)
	}
	if layout.ValueHeaderOffsets[0] != 1024 || layout.ValuePayloadOffsets[0] != 1536 || layout.FooterOffset != 2048 {
		t.Fatalf("layout = %#v", layout)
	}
	destination := newMemoryWriter(authority.SourceSizeBytes)
	if err := WriteArtifact(
		ctx,
		destination,
		manifest,
		layout,
		metadata,
		[]io.Reader{bytes.NewReader(value)},
	); err != nil {
		t.Fatalf("WriteArtifact(): %v", err)
	}
	if got, want := destination.bytes[148:156], []byte{'0', '1', '1', '7', '0', '2', 0, ' '}; !bytes.Equal(got, want) {
		t.Fatalf("manifest checksum = %q, want %q", got, want)
	}
	if got, want := destination.bytes[1024+148:1024+156], []byte{
		'0',
		'1',
		'3',
		'5',
		'2',
		'6',
		0,
		' ',
	}; !bytes.Equal(
		got,
		want,
	) {
		t.Fatalf("value checksum = %q, want %q", got, want)
	}
	sourceDigest := sha256.Sum256(destination.bytes)
	assertHexDigest(t, sourceDigest, "b3f6259f7be484008dd128fbee46917b63e3068c694df84adc3a672223c2ba3e")
	validated, err := ValidateArtifact(ctx, bytes.NewReader(destination.bytes), SourceEvidence{
		SizeBytes: uint64(len(destination.bytes)), SHA256: sourceDigest,
	}, t.TempDir(), testMetadataEncoder)
	if err != nil {
		t.Fatalf("ValidateArtifact(): %v", err)
	}
	pass, err := validated.BeginPassTwo(ctx)
	if err != nil {
		t.Fatalf("BeginPassTwo(): %v", err)
	}
	gotEntry, reader, err := pass.OpenValue(ctx, 0)
	if err != nil {
		t.Fatalf("OpenValue(): %v", err)
	}
	if gotEntry.ID != entry.ID {
		t.Fatalf("streamed Entry ID = %q, want %q", gotEntry.ID, entry.ID)
	}
	streamed, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll(): %v", err)
	}
	if err := reader.Close(); err != nil {
		t.Fatalf("Close value reader: %v", err)
	}
	if err := pass.CommitValue(ctx, 0); err != nil {
		t.Fatalf("CommitValue(): %v", err)
	}
	if !bytes.Equal(streamed, value) {
		t.Fatalf("streamed value = %x, want %x", streamed, value)
	}
	if err := validated.Close(ctx); err != nil {
		t.Fatalf("Close(): %v", err)
	}
}

// Rationale: restore authority must reject every noncanonical byte class
// rather than accepting permissive tar variants or trailing source bytes.
func TestValidateArtifactRejectsTamperedCanonicalRegions(t *testing.T) {
	t.Parallel()

	value := []byte("x\n")
	entry := literalEnvironmentEntry(value)
	ctx := context.Background()
	manifest, authority, metadata, err := BuildManifest(ctx, TransferCapture, []Entry{entry}, testMetadataEncoder)
	if err != nil {
		t.Fatalf("BuildManifest(): %v", err)
	}
	layout, err := ComputeLayout(ctx, authority, []Entry{entry})
	if err != nil {
		t.Fatalf("ComputeLayout(): %v", err)
	}
	destination := newMemoryWriter(authority.SourceSizeBytes)
	if err := WriteArtifact(
		ctx,
		destination,
		manifest,
		layout,
		metadata,
		[]io.Reader{bytes.NewReader(value)},
	); err != nil {
		t.Fatalf("WriteArtifact(): %v", err)
	}

	tests := map[string]func([]byte) []byte{
		"alternate regular typeflag": func(source []byte) []byte { source[156] = 0; return source },
		"manifest padding":           func(source []byte) []byte { source[512+len(manifest)] = 1; return source },
		"value padding":              func(source []byte) []byte { source[1536+len(value)] = 1; return source },
		"footer":                     func(source []byte) []byte { source[2048] = 1; return source },
		"trailing byte": func(source []byte) []byte {
			return append(source, 0)
		},
	}
	for name, mutate := range tests {
		name, mutate := name, mutate
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			source := mutate(append([]byte(nil), destination.bytes...))
			digest := sha256.Sum256(source)
			if _, err := ValidateArtifact(ctx, bytes.NewReader(source), SourceEvidence{
				SizeBytes: uint64(len(source)), SHA256: digest,
			}, t.TempDir(), testMetadataEncoder); err == nil {
				t.Fatal("ValidateArtifact() accepted tampered source")
			}
		})
	}
}

// Rationale: selected-byte semantics distinguish arbitrary file fact bytes
// from UTF-8/NUL-constrained environment destinations and literal sources.
func TestSelectedValueSemanticPolicy(t *testing.T) {
	t.Parallel()

	binaryValue := []byte{0xff, 0x00, 0xfe}
	fileEntry := Entry{
		ID: "ev_00000000000000000000000000",
		Metadata: Metadata{Kind: MetadataFile, File: FileMetadata{
			Path: "secrets/blob", Mode: 0600, UID: 1000, GID: 1000,
		}},
		Exposure: Exposure{Kind: ExposureAll},
		Source: Source{Kind: SourceFact, Fact: FactReference{
			AttachID: "att_00000000000000000000000000", Fact: "BLOB",
		}},
		Secret: true,
		Value: ValueEvidence{
			Path:      "values/ev_00000000000000000000000000",
			SizeBytes: uint64(len(binaryValue)),
			SHA256:    sha256.Sum256(binaryValue),
		},
	}
	ctx := context.Background()
	manifest, authority, metadata, err := BuildManifest(ctx, TransferCapture, []Entry{fileEntry}, testMetadataEncoder)
	if err != nil {
		t.Fatalf("BuildManifest(file fact): %v", err)
	}
	layout, err := ComputeLayout(ctx, authority, []Entry{fileEntry})
	if err != nil {
		t.Fatalf("ComputeLayout(file fact): %v", err)
	}
	if err := WriteArtifact(
		ctx,
		newMemoryWriter(authority.SourceSizeBytes),
		manifest,
		layout,
		metadata,
		[]io.Reader{bytes.NewReader(binaryValue)},
	); err != nil {
		t.Fatalf("WriteArtifact(file fact arbitrary bytes): %v", err)
	}

	environmentEntry := fileEntry
	environmentEntry.Metadata = Metadata{Kind: MetadataEnvironment, Environment: EnvironmentMetadata{Key: "BLOB"}}
	manifest, authority, metadata, err = BuildManifest(
		ctx,
		TransferCapture,
		[]Entry{environmentEntry},
		testMetadataEncoder,
	)
	if err != nil {
		t.Fatalf("BuildManifest(env fact): %v", err)
	}
	layout, err = ComputeLayout(ctx, authority, []Entry{environmentEntry})
	if err != nil {
		t.Fatalf("ComputeLayout(env fact): %v", err)
	}
	if err := WriteArtifact(
		ctx,
		newMemoryWriter(authority.SourceSizeBytes),
		manifest,
		layout,
		metadata,
		[]io.Reader{bytes.NewReader(binaryValue)},
	); err == nil {
		t.Fatal("WriteArtifact() accepted non-UTF-8/NUL environment bytes")
	}

	badSecretReference := environmentEntry
	badSecretReference.Secret = false
	badSecretReference.Source = Source{
		Kind:            SourceSecretReference,
		SecretReference: SecretReference{AuthoredKey: "DATABASE_URL"},
	}
	if _, _, _, err := BuildManifest(
		ctx,
		TransferCapture,
		[]Entry{badSecretReference},
		testMetadataEncoder,
	); err == nil {
		t.Fatal("BuildManifest() accepted reusable secret source for a plain Entry")
	}
}

// Rationale: the pinned age encoder equality is arithmetic authority for
// exact preflight reservations, including the mandatory empty payload chunk.
func TestAgeStoredSizeVectors(t *testing.T) {
	t.Parallel()

	tests := map[uint64]uint64{
		0: 200, 1: 201, 65535: 65735, 65536: 65736, 65537: 65753,
		131072: 131288, 131073: 131305,
	}
	for source, want := range tests {
		got, err := AgeStoredSize(source)
		if err != nil || got != want {
			t.Errorf("AgeStoredSize(%d) = %d, %v; want %d", source, got, err, want)
		}
	}
	if got, err := AgeStoredSize(MaxSourceBytes); err != nil || got != MaxStoredAgeBytes {
		t.Fatalf("AgeStoredSize(MaxSourceBytes) = %d, %v; want %d", got, err, MaxStoredAgeBytes)
	}
	for _, source := range []uint64{MaxSourceBytes + 1, ^uint64(0)} {
		if _, err := AgeStoredSize(source); err == nil {
			t.Errorf("AgeStoredSize(%d) accepted out-of-range/overflow input", source)
		}
	}
}

// Rationale: literal describes authored provenance, independently from the
// selected value's secret classification and resulting 0600 member mode.
func TestSecretLiteralIsAcceptedAndUsesSecretMemberMode(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	value := []byte("secret\n")
	entry := literalEnvironmentEntry(value)
	entry.Secret = true
	manifest, authority, metadata, err := BuildManifest(ctx, TransferCapture, []Entry{entry}, testMetadataEncoder)
	if err != nil {
		t.Fatalf("BuildManifest(): %v", err)
	}
	layout, err := ComputeLayout(ctx, authority, []Entry{entry})
	if err != nil {
		t.Fatalf("ComputeLayout(): %v", err)
	}
	destination := newMemoryWriter(authority.SourceSizeBytes)
	if err := WriteArtifact(
		ctx,
		destination,
		manifest,
		layout,
		metadata,
		[]io.Reader{bytes.NewReader(value)},
	); err != nil {
		t.Fatalf("WriteArtifact(): %v", err)
	}
	if got := string(
		destination.bytes[layout.ValueHeaderOffsets[0]+100 : layout.ValueHeaderOffsets[0]+108],
	); got != "0000600\x00" {
		t.Fatalf("secret literal member mode = %q", got)
	}
}

// Rationale: exact source evidence cannot be optional, and an advertised
// prefix must not conceal even one byte beyond exactSize.
func TestValidationRequiresDigestAndProbesHiddenTrailingByte(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	manifest, authority, metadata, err := BuildManifest(ctx, TransferCapture, nil, nil)
	if err != nil {
		t.Fatalf("BuildManifest(): %v", err)
	}
	layout, err := ComputeLayout(ctx, authority, nil)
	if err != nil {
		t.Fatalf("ComputeLayout(): %v", err)
	}
	destination := newMemoryWriter(authority.SourceSizeBytes)
	if err := WriteArtifact(ctx, destination, manifest, layout, metadata, nil); err != nil {
		t.Fatalf("WriteArtifact(): %v", err)
	}
	if _, err := ValidateArtifact(ctx, bytes.NewReader(destination.bytes), SourceEvidence{
		SizeBytes: authority.SourceSizeBytes,
	}, t.TempDir(), nil); err == nil {
		t.Fatal("ValidateArtifact() accepted missing SHA-256 evidence")
	}
	withExtra := append(append([]byte(nil), destination.bytes...), 0)
	digest := sha256.Sum256(destination.bytes)
	if _, err := ValidateArtifact(ctx, bytes.NewReader(withExtra), SourceEvidence{
		SizeBytes: authority.SourceSizeBytes, SHA256: digest,
	}, t.TempDir(), nil); err == nil {
		t.Fatal("ValidateArtifact() accepted hidden trailing byte")
	}
}

// Rationale: pass two uses an owned immutable spool, rehashes all framing, and
// authenticates a complete value before exposing its zeroizable reader.
func TestPassTwoOwnsSourceRehashesAndZeroizesReader(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	value := []byte("owned\n")
	entry := literalEnvironmentEntry(value)
	manifest, authority, metadata, err := BuildManifest(ctx, TransferCapture, []Entry{entry}, testMetadataEncoder)
	if err != nil {
		t.Fatalf("BuildManifest(): %v", err)
	}
	layout, err := ComputeLayout(ctx, authority, []Entry{entry})
	if err != nil {
		t.Fatalf("ComputeLayout(): %v", err)
	}
	destination := newMemoryWriter(authority.SourceSizeBytes)
	if err := WriteArtifact(
		ctx,
		destination,
		manifest,
		layout,
		metadata,
		[]io.Reader{bytes.NewReader(value)},
	); err != nil {
		t.Fatalf("WriteArtifact(): %v", err)
	}
	digest := sha256.Sum256(destination.bytes)
	validated, err := ValidateArtifact(ctx, bytes.NewReader(destination.bytes), SourceEvidence{
		SizeBytes: authority.SourceSizeBytes, SHA256: digest,
	}, t.TempDir(), testMetadataEncoder)
	if err != nil {
		t.Fatalf("ValidateArtifact(): %v", err)
	}
	destination.bytes[layout.ValuePayloadOffsets[0]] ^= 0xff
	pass, err := validated.BeginPassTwo(ctx)
	if err != nil {
		t.Fatalf("BeginPassTwo() after original mutation: %v", err)
	}
	_, opened, err := pass.OpenValue(ctx, 0)
	if err != nil {
		t.Fatalf("OpenValue(): %v", err)
	}
	reader := opened.(*authenticatedValueReader)
	if err := reader.Close(); err != nil {
		t.Fatalf("Close value reader: %v", err)
	}
	if !allZero(reader.value) {
		t.Fatal("authenticated reader retained plaintext after Close")
	}
	if _, err := validated.spool.WriteAt([]byte{1}, int64(layout.FooterOffset)); err != nil {
		t.Fatalf("corrupt owned spool fixture: %v", err)
	}
	if _, err := validated.BeginPassTwo(ctx); err == nil {
		t.Fatal("BeginPassTwo() accepted changed owned framing/source digest")
	}
	if err := validated.Close(ctx); err != nil {
		t.Fatalf("Close artifact: %v", err)
	}
}

// Rationale: metadata sizing and cancellation are fail-closed before archive
// or transcript consumers receive partial authority, and callback diagnostics
// remain private behind the one Groundplane error type.
func TestMetadataCapCancellationAndCallbackSanitization(t *testing.T) {
	t.Parallel()
	entry := literalEnvironmentEntry([]byte("x"))
	oversized := func(context.Context, TransferDirection, uint32, Entry) ([]byte, error) {
		return make([]byte, MaxCanonicalEntryBytes+1), nil
	}
	if _, _, _, err := BuildManifest(context.Background(), TransferCapture, []Entry{entry}, oversized); err == nil {
		t.Fatal("BuildManifest() accepted oversized deterministic metadata")
	}
	privateFailure := errors.New("private encoder diagnostic")
	failing := func(context.Context, TransferDirection, uint32, Entry) ([]byte, error) {
		return nil, privateFailure
	}
	_, _, _, err := BuildManifest(context.Background(), TransferCapture, []Entry{entry}, failing)
	if err == nil || !errors.Is(err, privateFailure) {
		t.Fatalf("BuildManifest() did not preserve wrapped callback cause: %v", err)
	}
	if kind, ok := errs.KindOf(err); !ok || kind != errs.KindInternal {
		t.Fatalf("callback error is not sanitized Groundplane error: %v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, _, err := BuildManifest(canceled, TransferCapture, []Entry{entry}, testMetadataEncoder); err == nil {
		t.Fatal("BuildManifest() ignored cancellation")
	} else if !errors.Is(err, context.Canceled) {
		t.Fatalf("BuildManifest() lost context cancellation: %v", err)
	}
}

// Rationale: pass-two publication consumes values exactly once in stable
// ordinal order, and one authenticated reader must close before the next opens.
func TestPassTwoEnforcesOrderedSingleInFlightValue(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	firstValue := []byte("a")
	secondValue := []byte("b")
	first := literalEnvironmentEntry(firstValue)
	second := literalEnvironmentEntry(secondValue)
	second.ID = "ev_00000000000000000000000001"
	second.Metadata.Environment.Key = "B"
	second.Value.Path = "values/" + second.ID
	second.Value.SHA256 = sha256.Sum256(secondValue)
	manifest, authority, metadata, err := BuildManifest(
		ctx,
		TransferCapture,
		[]Entry{first, second},
		testMetadataEncoder,
	)
	if err != nil {
		t.Fatalf("BuildManifest(): %v", err)
	}
	layout, err := ComputeLayout(ctx, authority, []Entry{first, second})
	if err != nil {
		t.Fatalf("ComputeLayout(): %v", err)
	}
	destination := newMemoryWriter(authority.SourceSizeBytes)
	if err := WriteArtifact(ctx, destination, manifest, layout, metadata, []io.Reader{
		bytes.NewReader(firstValue), bytes.NewReader(secondValue),
	}); err != nil {
		t.Fatalf("WriteArtifact(): %v", err)
	}
	digest := sha256.Sum256(destination.bytes)
	validated, err := ValidateArtifact(ctx, bytes.NewReader(destination.bytes), SourceEvidence{
		SizeBytes: authority.SourceSizeBytes, SHA256: digest,
	}, t.TempDir(), testMetadataEncoder)
	if err != nil {
		t.Fatalf("ValidateArtifact(): %v", err)
	}
	pass, err := validated.BeginPassTwo(ctx)
	if err != nil {
		t.Fatalf("BeginPassTwo(): %v", err)
	}
	if len(pass.MetadataFrames()) != 2 {
		t.Fatal("pass two did not expose revalidated metadata")
	}
	if _, _, err := pass.OpenValue(ctx, 1); err == nil {
		t.Fatal("OpenValue() accepted out-of-order ordinal")
	} else if kind, ok := errs.KindOf(err); !ok || kind != errs.KindStateConflict {
		t.Fatalf("out-of-order error kind = %v", err)
	}
	if err := pass.CommitValue(ctx, 0); err == nil {
		t.Fatal("CommitValue() accepted an early commit")
	} else if kind, ok := errs.KindOf(err); !ok || kind != errs.KindStateConflict {
		t.Fatalf("early commit error kind = %v", err)
	}
	_, firstReader, err := pass.OpenValue(ctx, 0)
	if err != nil {
		t.Fatalf("OpenValue(0): %v", err)
	}
	if _, _, err := pass.OpenValue(ctx, 0); err == nil {
		t.Fatal("OpenValue() accepted concurrent/repeated in-flight ordinal")
	} else if kind, ok := errs.KindOf(err); !ok || kind != errs.KindStateConflict {
		t.Fatalf("in-flight error kind = %v", err)
	}
	if err := firstReader.Close(); err != nil {
		t.Fatalf("Close first reader: %v", err)
	}
	_, retryReader, err := pass.OpenValue(ctx, 0)
	if err != nil {
		t.Fatalf("OpenValue(0) after early close did not permit retry: %v", err)
	}
	if _, err := io.ReadAll(retryReader); err != nil {
		t.Fatalf("consume retried first reader: %v", err)
	}
	if err := retryReader.Close(); err != nil {
		t.Fatalf("Close retried first reader: %v", err)
	}
	if _, _, err := pass.OpenValue(ctx, 1); err == nil {
		t.Fatal("OpenValue() advanced before durable commit")
	} else if kind, ok := errs.KindOf(err); !ok || kind != errs.KindStateConflict {
		t.Fatalf("awaiting-commit open error kind = %v", err)
	}
	if err := pass.CommitValue(ctx, 1); err == nil {
		t.Fatal("CommitValue() accepted the wrong ordinal")
	} else if kind, ok := errs.KindOf(err); !ok || kind != errs.KindStateConflict {
		t.Fatalf("wrong commit error kind = %v", err)
	}
	if err := pass.CommitValue(ctx, 0); err != nil {
		t.Fatalf("CommitValue(0): %v", err)
	}
	if err := pass.CommitValue(ctx, 0); err == nil {
		t.Fatal("CommitValue() accepted duplicate commit")
	} else if kind, ok := errs.KindOf(err); !ok || kind != errs.KindStateConflict {
		t.Fatalf("duplicate commit error kind = %v", err)
	}
	_, secondReader, err := pass.OpenValue(ctx, 1)
	if err != nil {
		t.Fatalf("OpenValue(1): %v", err)
	}
	if _, err := io.ReadAll(secondReader); err != nil {
		t.Fatalf("consume second reader: %v", err)
	}
	if err := secondReader.Close(); err != nil {
		t.Fatalf("Close second reader: %v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := pass.CommitValue(canceled, 1); err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("CommitValue() lost cancellation or changed state: %v", err)
	}
	if err := pass.CommitValue(ctx, 1); err != nil {
		t.Fatalf("CommitValue(1) after canceled attempt: %v", err)
	}
	if err := validated.Close(ctx); err != nil {
		t.Fatalf("Close artifact: %v", err)
	}
}

// Rationale: pass-two revalidation proves the owned file itself ends at S;
// section-reader EOF alone cannot reveal an append after validation.
func TestBeginPassTwoRejectsAppendAfterSourceSize(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	manifest, authority, metadata, err := BuildManifest(ctx, TransferCapture, nil, nil)
	if err != nil {
		t.Fatalf("BuildManifest(): %v", err)
	}
	layout, err := ComputeLayout(ctx, authority, nil)
	if err != nil {
		t.Fatalf("ComputeLayout(): %v", err)
	}
	destination := newMemoryWriter(authority.SourceSizeBytes)
	if err := WriteArtifact(ctx, destination, manifest, layout, metadata, nil); err != nil {
		t.Fatalf("WriteArtifact(): %v", err)
	}
	digest := sha256.Sum256(destination.bytes)
	validated, err := ValidateArtifact(ctx, bytes.NewReader(destination.bytes), SourceEvidence{
		SizeBytes: authority.SourceSizeBytes, SHA256: digest,
	}, t.TempDir(), nil)
	if err != nil {
		t.Fatalf("ValidateArtifact(): %v", err)
	}
	if _, err := validated.spool.WriteAt([]byte{0}, int64(authority.SourceSizeBytes)); err != nil {
		t.Fatalf("append owned spool fixture: %v", err)
	}
	if _, err := validated.BeginPassTwo(ctx); err == nil {
		t.Fatal("BeginPassTwo() accepted an appended owned spool")
	}
	if err := validated.Close(ctx); err != nil {
		t.Fatalf("Close artifact: %v", err)
	}
}

// Rationale: a split UTF-8 rune must not leave its former pending plaintext
// in the validator backing array after the next chunk resolves it.
func TestSelectedValueValidatorZeroizesPendingBeforeReslice(t *testing.T) {
	t.Parallel()
	validator := selectedValueValidator{requireUTF8: true}
	if !validator.consume([]byte{0xe2}) || len(validator.pending) != 1 {
		t.Fatal("validator did not retain incomplete rune")
	}
	formerPending := validator.pending[:cap(validator.pending)]
	if !validator.consume([]byte{0x82, 0xac}) || !validator.finish() {
		t.Fatal("validator rejected completed split rune")
	}
	if !allZero(formerPending) {
		t.Fatal("validator retained former pending UTF-8 bytes")
	}
}

// Rationale: comparison cancellation is a retryable execution cause, not an
// archive mismatch, so the typed wrapper must preserve errors.Is semantics.
func TestSameLayoutContentPropagatesCancellation(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	layout := Layout{Authority: ContentAuthority{}}
	if _, err := sameLayoutContent(ctx, layout, layout); err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("sameLayoutContent() lost cancellation: %v", err)
	}
}

// Rationale: failed validation cleanup must retain its primary typed cause and
// every truncate/close failure rather than silently losing staging failures.
func TestCleanupOwnedSpoolJoinsPrimaryTruncateAndCloseFailures(t *testing.T) {
	t.Parallel()
	primary := errs.New(errs.KindInternal, "primary validation failure")
	truncateFailure := errors.New("truncate failure")
	closeFailure := errors.New("close failure")
	spool := &faultOwnedSpool{truncateErr: truncateFailure, closeErr: closeFailure}
	err := cleanupOwnedSpool(primary, spool)
	if err == nil || !errors.Is(err, primary) || !errors.Is(err, truncateFailure) || !errors.Is(err, closeFailure) {
		t.Fatalf("cleanupOwnedSpool() lost joined causes: %v", err)
	}
	if kind, ok := errs.KindOf(err); !ok || kind != errs.KindInternal {
		t.Fatalf("cleanup error is not typed internal: %v", err)
	}
	if spool.truncateCalls != 1 || spool.syncCalls != 1 || spool.closeCalls != 1 {
		t.Fatalf(
			"cleanup calls = truncate %d, sync %d, close %d",
			spool.truncateCalls,
			spool.syncCalls,
			spool.closeCalls,
		)
	}
}

// Rationale: Close is one terminal attempt even when os.File-style Close
// reports an error; the artifact drops the spool and repeats only the memoized
// result, never exposing or retrying an indeterminate descriptor.
func TestValidatedArtifactCloseRecordsFailureWithoutRetryOrReuse(t *testing.T) {
	t.Parallel()
	truncateFailure := errors.New("truncate failure")
	closeFailure := errors.New("close failure")
	spool := &faultOwnedSpool{truncateErr: truncateFailure, closeErr: closeFailure}
	artifact := &ValidatedArtifact{spool: spool}
	first := artifact.Close(context.Background())
	if first == nil || !errors.Is(first, truncateFailure) || !errors.Is(first, closeFailure) {
		t.Fatalf("Close() lost cleanup causes: %v", first)
	}
	if artifact.spool != nil || !artifact.closed {
		t.Fatal("Close() retained a reachable or reusable spool")
	}
	second := artifact.Close(context.Background())
	if second != first {
		t.Fatalf("second Close() = %v, want memoized first result %v", second, first)
	}
	if spool.truncateCalls != 1 || spool.syncCalls != 1 || spool.closeCalls != 1 {
		t.Fatalf(
			"terminal cleanup retried: truncate %d, sync %d, close %d",
			spool.truncateCalls,
			spool.syncCalls,
			spool.closeCalls,
		)
	}
	if _, err := artifact.BeginPassTwo(context.Background()); err == nil {
		t.Fatal("closed artifact exposed its former spool")
	}
}

func literalEnvironmentEntry(value []byte) Entry {
	return Entry{
		ID:       "ev_00000000000000000000000000",
		Metadata: Metadata{Kind: MetadataEnvironment, Environment: EnvironmentMetadata{Key: "A"}},
		Exposure: Exposure{Kind: ExposureAll},
		Source:   Source{Kind: SourceLiteral},
		Value: ValueEvidence{
			Path:      "values/ev_00000000000000000000000000",
			SizeBytes: uint64(len(value)),
			SHA256:    sha256.Sum256(value),
		},
	}
}

func assertHexDigest(t *testing.T, got [32]byte, want string) {
	t.Helper()
	if hex.EncodeToString(got[:]) != want {
		t.Fatalf("digest = %x, want %s", got, want)
	}
}

type memoryWriter struct {
	bytes []byte
}

type faultOwnedSpool struct {
	truncateErr   error
	syncErr       error
	closeErr      error
	truncateCalls int
	syncCalls     int
	closeCalls    int
}

func (spool *faultOwnedSpool) ReadAt([]byte, int64) (int, error)  { return 0, io.EOF }
func (spool *faultOwnedSpool) Write([]byte) (int, error)          { return 0, io.ErrShortWrite }
func (spool *faultOwnedSpool) WriteAt([]byte, int64) (int, error) { return 0, io.ErrShortWrite }
func (spool *faultOwnedSpool) Stat() (os.FileInfo, error)         { return nil, errors.New("unused Stat") }
func (spool *faultOwnedSpool) Truncate(int64) error {
	spool.truncateCalls++
	return spool.truncateErr
}
func (spool *faultOwnedSpool) Sync() error {
	spool.syncCalls++
	return spool.syncErr
}
func (spool *faultOwnedSpool) Close() error {
	spool.closeCalls++
	return spool.closeErr
}

func newMemoryWriter(size uint64) *memoryWriter {
	return &memoryWriter{bytes: make([]byte, int(size))}
}

func (writer *memoryWriter) WriteAt(value []byte, offset int64) (int, error) {
	if offset < 0 || offset > int64(len(writer.bytes)) {
		return 0, io.ErrShortWrite
	}
	count := copy(writer.bytes[int(offset):], value)
	if count != len(value) {
		return count, io.ErrShortWrite
	}
	return count, nil
}

func testMetadataEncoder(_ context.Context, direction TransferDirection, ordinal uint32, entry Entry) ([]byte, error) {
	return []byte{byte(direction), byte(ordinal), byte(len(entry.ID))}, nil
}
