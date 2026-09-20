package idempotency

import (
	"context"
	"errors"
	"fmt"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"time"

	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	infraetcd "github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const unknownEvidenceReadTimeout = 5 * time.Second

type ResolutionKind uint8

const (
	ResolutionApplied ResolutionKind = iota + 1
	ResolutionReplay
)

type Resolution struct {
	Kind     ResolutionKind
	Response idempotencyrecord.IdempotencyResponse
}

func (resolution Resolution) String() string {
	return fmt.Sprintf("Resolution{kind:%d,response:redacted}", resolution.Kind)
}

func (resolution Resolution) GoString() string { return resolution.String() }

type ProtectedEvidence struct {
	record idempotencyrecord.ProtectedIntentRecord
}

// Destroy zeroizes the transient protected intent once publication or replay
// has been resolved.
func (evidence *ProtectedEvidence) Destroy() {
	if evidence == nil {
		return
	}
	clear(evidence.record.Ciphertext)
	evidence.record.Ciphertext = nil
}

type Coordinator struct {
	protector *secretvalue.Protector
}

// EvidenceRepository is the consumer-owned read seam used to classify a
// durable idempotency marker without depending on a concrete repository.
type EvidenceRepository interface {
	Read(context.Context, idempotencyrecord.IdempotencyLocator) (*idempotencyrecord.IdempotencyEvidence, error)
}

func NewCoordinator(protector *secretvalue.Protector) (*Coordinator, error) {
	if protector == nil {
		return nil, internalError("protector is required")
	}
	return &Coordinator{protector: protector}, nil
}

// ProtectIntent consumes a canonical digest and returns only durable-safe
// ciphertext metadata. The plaintext version and digest never cross into the
// infrastructure package.
func (coordinator *Coordinator) ProtectIntent(
	ctx context.Context,
	version Version,
	digest *Digest,
) (ProtectedEvidence, error) {
	if coordinator == nil || coordinator.protector == nil {
		return ProtectedEvidence{}, internalError("coordinator is not initialized")
	}
	envelope, err := Protect(ctx, coordinator.protector, version, digest)
	if err != nil {
		return ProtectedEvidence{}, err
	}
	metadata := envelope.Metadata()
	ciphertext := envelope.Ciphertext()
	return ProtectedEvidence{record: idempotencyrecord.ProtectedIntentRecord{
		EnvelopeVersion:  uint8(metadata.Version),
		Cipher:           string(metadata.Cipher),
		DigestAlgorithm:  string(metadata.Digest.Algorithm),
		CiphertextDigest: metadata.Digest.Value,
		Ciphertext:       ciphertext,
	}}, nil
}

func (evidence ProtectedEvidence) DurableRecord() (idempotencyrecord.ProtectedIntentRecord, error) {
	if _, err := restoreProtectedIntent(evidence.record); err != nil {
		return idempotencyrecord.ProtectedIntentRecord{}, err
	}
	result := evidence.record
	result.Ciphertext = append([]byte(nil), evidence.record.Ciphertext...)
	return result, nil
}

// MatchesDurable compares a candidate with private staged evidence at the
// encryption boundary. Callers receive only equality and never plaintext.
func (coordinator *Coordinator) MatchesDurable(
	ctx context.Context,
	candidate ProtectedEvidence,
	existing idempotencyrecord.ProtectedIntentRecord,
) (bool, error) {
	if coordinator == nil || coordinator.protector == nil {
		return false, internalError("coordinator is not initialized")
	}
	candidateEnvelope, err := restoreProtectedIntent(candidate.record)
	if err != nil {
		return false, err
	}
	existingEnvelope, err := restoreProtectedIntent(existing)
	if err != nil {
		return false, err
	}
	return CompareProtected(ctx, coordinator.protector, existingEnvelope, candidateEnvelope)
}

// ResolveKnown classifies one same-revision transaction result. It never
// rereads mutable state after a known compare failure.
func (coordinator *Coordinator) ResolveKnown(
	ctx context.Context,
	candidate ProtectedEvidence,
	result infraetcd.IdempotencyTransactionResult,
) (Resolution, error) {
	outcome, marker, conflict, err := result.Classify()
	if err != nil {
		return Resolution{}, err
	}
	switch outcome {
	case infraetcd.IdempotencyKnownApplied:
		return Resolution{Kind: ResolutionApplied}, nil
	case infraetcd.IdempotencyKnownConflict:
		return Resolution{}, conflict
	case infraetcd.IdempotencyKnownExisting:
		defer clearProtectedMarker(marker)
		return coordinator.classifyMarker(ctx, candidate, marker)
	default:
		return Resolution{}, internalError("idempotency result is invalid")
	}
}

// ResolveExisting performs the deliberate pre-mutation marker lookup needed by
// destructive routes. A completed delete must replay after its target record
// is gone, while a first request must still validate the current target before
// claiming a marker and creating its Task.
func (coordinator *Coordinator) ResolveExisting(
	ctx context.Context,
	repository EvidenceRepository,
	locator idempotencyrecord.IdempotencyLocator,
	candidate ProtectedEvidence,
) (Resolution, bool, error) {
	if ctx == nil || repository == nil {
		return Resolution{}, false, internalError("existing-outcome evidence is incomplete")
	}
	evidence, err := repository.Read(ctx, locator)
	if err != nil {
		return Resolution{}, false, err
	}
	if evidence == nil {
		return Resolution{}, false, nil
	}
	marker, err := evidence.Marker()
	if err != nil {
		return Resolution{}, true, err
	}
	defer clearProtectedMarker(marker)
	resolution, err := coordinator.classifyMarker(ctx, candidate, marker)
	return resolution, true, err
}

// ResolveMarker classifies marker evidence already read by a caller at a
// fixed revision. It is deliberately separate from ResolveExisting so durable
// reverse-index replays do not perform an unconstrained second read.
func (coordinator *Coordinator) ResolveMarker(
	ctx context.Context,
	candidate ProtectedEvidence,
	marker idempotencyrecord.IdempotencyMarker,
) (Resolution, error) {
	if ctx == nil {
		return Resolution{}, internalError("marker-outcome evidence is incomplete")
	}
	defer clearProtectedMarker(marker)
	return coordinator.classifyMarker(ctx, candidate, marker)
}

// ResolveOperationRootExisting classifies an immutable operation-root
// response. Unlike ordinary Task idempotency, an equal accepted operation
// replays its stored root response while the current attempt is still active.
func (coordinator *Coordinator) ResolveOperationRootExisting(
	ctx context.Context,
	repository EvidenceRepository,
	locator idempotencyrecord.IdempotencyLocator,
	candidate ProtectedEvidence,
) (Resolution, bool, error) {
	if ctx == nil || repository == nil {
		return Resolution{}, false, internalError("existing-outcome evidence is incomplete")
	}
	evidence, err := repository.Read(ctx, locator)
	if err != nil {
		return Resolution{}, false, err
	}
	if evidence == nil {
		return Resolution{}, false, nil
	}
	marker, err := evidence.Marker()
	if err != nil {
		return Resolution{}, true, err
	}
	defer clearProtectedMarker(marker)
	resolution, err := coordinator.classifyOperationRootMarker(ctx, candidate, marker)
	return resolution, true, err
}

// ResolveUnknown performs the only permitted latest linearizable reread: a
// Store outcome whose commit status was unknown. Missing or unavailable
// evidence preserves the original retryable/cancellation error.
func (coordinator *Coordinator) ResolveUnknown(
	ctx context.Context,
	repository EvidenceRepository,
	locator idempotencyrecord.IdempotencyLocator,
	candidate ProtectedEvidence,
	original error,
) (Resolution, error) {
	if ctx == nil || repository == nil || original == nil {
		return Resolution{}, internalError("unknown-outcome evidence is incomplete")
	}
	if contextErr := ctx.Err(); contextErr != nil {
		return Resolution{}, contextErr
	}
	if !eligibleUnknownOutcome(original) {
		return Resolution{}, internalError("result is not an unknown transaction outcome")
	}
	evidenceContext, cancel := context.WithTimeout(context.WithoutCancel(ctx), unknownEvidenceReadTimeout)
	defer cancel()
	evidence, err := repository.Read(evidenceContext, locator)
	if contextErr := ctx.Err(); contextErr != nil {
		return Resolution{}, contextErr
	}
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) ||
			errors.Is(err, errs.New(errs.KindStorageUnavailable, "")) {
			return Resolution{}, original
		}
		return Resolution{}, err
	}
	if evidence == nil {
		return Resolution{}, original
	}
	marker, err := evidence.Marker()
	if err != nil {
		return Resolution{}, err
	}
	defer clearProtectedMarker(marker)
	return coordinator.classifyMarker(ctx, candidate, marker)
}

func clearProtectedMarker(marker idempotencyrecord.IdempotencyMarker) {
	clear(marker.Intent.Ciphertext)
	clear(marker.Response.Body)
}

func eligibleUnknownOutcome(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	kind, ok := errs.KindOf(err)
	return ok && kind == errs.KindStorageUnavailable
}

func (coordinator *Coordinator) classifyMarker(
	ctx context.Context,
	candidate ProtectedEvidence,
	marker idempotencyrecord.IdempotencyMarker,
) (Resolution, error) {
	if coordinator == nil || coordinator.protector == nil {
		return Resolution{}, internalError("coordinator is not initialized")
	}
	candidateEnvelope, err := restoreProtectedIntent(candidate.record)
	if err != nil {
		return Resolution{}, err
	}
	existingEnvelope, err := restoreProtectedIntent(marker.Intent)
	if err != nil {
		return Resolution{}, err
	}
	matched, err := CompareProtected(ctx, coordinator.protector, existingEnvelope, candidateEnvelope)
	if err != nil {
		return Resolution{}, err
	}
	if !matched {
		return Resolution{}, errs.New(errs.KindIdempotencyMismatch, "idempotency key was used for a different request")
	}
	if marker.Kind == idempotencyrecord.IdempotencyMarkerTask && marker.State == idempotencyrecord.IdempotencyMarkerPending {
		return Resolution{}, errs.New(errs.KindIdempotencyInProgress, "the original task is still active")
	}
	return Resolution{
		Kind: ResolutionReplay,
		Response: idempotencyrecord.IdempotencyResponse{
			Status: marker.Response.Status, ContentKind: marker.Response.ContentKind,
			Body: append([]byte(nil), marker.Response.Body...),
		},
	}, nil
}

func (coordinator *Coordinator) classifyOperationRootMarker(
	ctx context.Context,
	candidate ProtectedEvidence,
	marker idempotencyrecord.IdempotencyMarker,
) (Resolution, error) {
	resolution, err := coordinator.classifyMarker(ctx, candidate, marker)
	if !errors.Is(err, errs.New(errs.KindIdempotencyInProgress, "")) {
		return resolution, err
	}
	return Resolution{
		Kind: ResolutionReplay,
		Response: idempotencyrecord.IdempotencyResponse{
			Status: marker.Response.Status, ContentKind: marker.Response.ContentKind,
			Body: append([]byte(nil), marker.Response.Body...),
		},
	}, nil
}

func restoreProtectedIntent(value idempotencyrecord.ProtectedIntentRecord) (secretvalue.Envelope, error) {
	ciphertext := append([]byte(nil), value.Ciphertext...)
	defer clear(ciphertext)
	if len(ciphertext) > maximumProtectedCiphertextBytes {
		return secretvalue.Envelope{}, internalError("protected intent exceeds ciphertext limit")
	}
	return secretvalue.Restore(secretvalue.Metadata{
		Version: secretvalue.EnvelopeVersion(value.EnvelopeVersion),
		Cipher:  secretvalue.CipherSuite(value.Cipher),
		Digest: secretvalue.Digest{
			Algorithm: secretvalue.DigestAlgorithm(value.DigestAlgorithm),
			Value:     value.CiphertextDigest,
		},
	}, ciphertext)
}
