package resolverexecution

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"github.com/AlanD20/groundplane/internal/common/dnsproof"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/jcs"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// ValidateServingProof binds a genuine probe result to the selected immutable
// image, runtime and Corefile. Process health alone never substitutes for it.
func ValidateServingProof(configuration Configuration, proof *agentpb.DNSResolverObservationEvidence) error {
	if proof == nil || configuration.Validate(proof.ComponentId) != nil || dnsproof.Verify(proof) != nil ||
		proof.ObservedAt == nil || proof.ObservedAt.CheckValid() != nil {
		return invalid("DNS resolver serving proof is missing or invalid")
	}
	service := configuration.Runtime.Services[0]
	if proof.ServiceId != service.ServiceId || proof.ArtifactId != configuration.Apply.ArtifactId ||
		proof.RenderGeneration != configuration.Generation || !bytes.Equal(proof.ArtifactSha256, configuration.Apply.ArtifactDigest) ||
		proof.ImageReference != service.ImageReference || proof.ImageRepository != service.ImageRepository ||
		!bytes.Equal(proof.ImageIndexDigest, service.ImageIndexDigest) || !bytes.Equal(proof.ImageConfigDigest, service.ImageConfigDigest) ||
		!bytes.Equal(proof.VerifiedImageDigest, service.ImageChildDigest) || proof.ImageOs != service.ImageOs ||
		proof.ImageArchitecture != service.ImageArchitecture || proof.ImageVariant != service.ImageVariant ||
		proof.ListenEndpoint != "127.0.0.1:53" {
		return invalid("DNS resolver serving proof differs from the captured image, container or configuration")
	}
	return nil
}

func ServingObservation(attempt workexecution.Attempt, input Input, proof *agentpb.DNSResolverObservationEvidence) (Observation, error) {
	layout, err := CaptureLayout(attempt, input)
	if err != nil || !input.Enabled || ValidateServingProof(*input.Candidate, proof) != nil ||
		attempt.StartedAt == nil || proof.ObservedAt.AsTime().Before(*attempt.StartedAt) || !proof.ObservedAt.AsTime().Before(attempt.Deadline) {
		return Observation{}, invalid("DNS resolver serving observation has no matching active captured input")
	}
	return Observation{Input: attempt.Input, ComponentID: input.ComponentID, Stage: VerifyDNS, RuntimePresent: true,
		ConfigurationSHA256: layout.ConfigurationSHA256, ServingProofSHA256: hex.EncodeToString(proof.ProofSha256), At: proof.ObservedAt.AsTime().UTC()}, nil
}

// CompletionEvidence joins the final cursor and the actual retained DNS-stage
// observation. Later resource changes or history collection cannot change it.
func CompletionEvidence(attempt workexecution.Attempt, input Input, final Progress,
	verification *Progress, proof *agentpb.DNSResolverObservationEvidence,
) (workexecution.Input, error) {
	layout, err := CaptureLayout(attempt, input)
	if err != nil || final.Validate(attempt, layout) != nil || final.InFlight || final.Stage != Complete {
		return workexecution.Input{}, invalid("DNS resolver completion has no final execution checkpoint")
	}
	if input.Enabled {
		observed, err := ServingObservation(attempt, input, proof)
		if err != nil || verification == nil || verification.Validate(attempt, layout) != nil || verification.InFlight ||
			verification.Last == nil || *verification.Last != observed || verification.Sequence+2 != final.Sequence ||
			verification.UpdatedAt.After(final.UpdatedAt) {
			return workexecution.Input{}, invalid("DNS resolver completion is missing its exact acknowledged serving proof")
		}
	} else if verification != nil || proof != nil {
		return workexecution.Input{}, invalid("disabled DNS resolver cannot report a running serving proof")
	}
	return CompletionDigest(attempt, final, proof)
}

// CompletionDigest hashes already-validated evidence. It grants no execution or
// settlement permission; Controller admission still uses CompletionEvidence.
// This lets the compact Agent journal check a saved receipt without copying
// the captured Corefile and Compose artifacts into every local record.
func CompletionDigest(attempt workexecution.Attempt, final Progress, proof *agentpb.DNSResolverObservationEvidence) (workexecution.Input, error) {
	if attempt.Validate() != nil || final.AttemptID != attempt.ID || final.Stage != Complete || final.InFlight {
		return workexecution.Input{}, invalid("DNS resolver completion digest has no final captured boundary")
	}
	normalized := attempt
	normalized.FinishedAt, normalized.Outcome, normalized.Result = nil, "", nil
	var encodedProof []byte
	var err error
	if proof != nil {
		encodedProof, err = (proto.MarshalOptions{Deterministic: true}).Marshal(proof)
		if err != nil {
			return workexecution.Input{}, err
		}
	}
	_, reference, err := captureEvidence(attempt.Input.ID, struct {
		Attempt workexecution.Attempt `json:"attempt"`
		Final   Progress              `json:"final"`
		Proof   []byte                `json:"proof,omitempty"`
	}{normalized, final, encodedProof})
	return reference, err
}

func captureEvidence(id string, value any) ([]byte, workexecution.Input, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, workexecution.Input{}, err
	}
	canonical, err := jcs.Canonicalize(raw)
	if err != nil || len(canonical) > MaximumReportBytes {
		return nil, workexecution.Input{}, invalid("DNS resolver report exceeds its bounded format")
	}
	digest := sha256.Sum256(canonical)
	reference := workexecution.Input{ID: id, SHA256: hex.EncodeToString(digest[:])}
	if err := reference.Validate(); err != nil {
		return nil, workexecution.Input{}, err
	}
	return canonical, reference, nil
}

func derivedReferenceID(attemptID, purpose string) (string, error) {
	at, err := ids.Timestamp(ids.KindAttempt, attemptID)
	if err != nil {
		return "", err
	}
	return ids.DeriveAt(ids.KindPlan, at, attemptID, "dns-resolver/"+purpose), nil
}
