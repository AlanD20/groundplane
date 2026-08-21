package idempotentintent

import (
	"context"
	"errors"
	"fmt"
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
	Response infraetcd.IdempotencyResponse
}

func (resolution Resolution) String() string {
	return fmt.Sprintf("Resolution{kind:%d,response:redacted}", resolution.Kind)
}

func (resolution Resolution) GoString() string { return resolution.String() }

type ProtectedEvidence struct {
	record infraetcd.ProtectedIntentRecord
}

type Coordinator struct {
	protector *secretvalue.Protector
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
	return ProtectedEvidence{record: infraetcd.ProtectedIntentRecord{
		EnvelopeVersion:  uint8(metadata.Version),
		Cipher:           string(metadata.Cipher),
		DigestAlgorithm:  string(metadata.Digest.Algorithm),
		CiphertextDigest: metadata.Digest.Value,
		Ciphertext:       ciphertext,
	}}, nil
}

func (evidence ProtectedEvidence) DurableRecord() (infraetcd.ProtectedIntentRecord, error) {
	if _, err := restoreProtectedIntent(evidence.record); err != nil {
		return infraetcd.ProtectedIntentRecord{}, err
	}
	result := evidence.record
	result.Ciphertext = append([]byte(nil), evidence.record.Ciphertext...)
	return result, nil
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

// ResolveUnknown performs the only permitted latest linearizable reread: a
// Store outcome whose commit status was unknown. Missing or unavailable
// evidence preserves the original retryable/cancellation error.
func (coordinator *Coordinator) ResolveUnknown(
	ctx context.Context,
	repository *infraetcd.IdempotencyRepository,
	locator infraetcd.IdempotencyLocator,
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

func clearProtectedMarker(marker infraetcd.IdempotencyMarker) {
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
	marker infraetcd.IdempotencyMarker,
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
	if marker.Kind == infraetcd.IdempotencyMarkerTask && marker.State == infraetcd.IdempotencyMarkerPending {
		return Resolution{}, errs.New(errs.KindIdempotencyInProgress, "the original task is still active")
	}
	return Resolution{
		Kind: ResolutionReplay,
		Response: infraetcd.IdempotencyResponse{
			Status: marker.Response.Status, ContentKind: marker.Response.ContentKind,
			Body: append([]byte(nil), marker.Response.Body...),
		},
	}, nil
}

func restoreProtectedIntent(value infraetcd.ProtectedIntentRecord) (secretvalue.Envelope, error) {
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
