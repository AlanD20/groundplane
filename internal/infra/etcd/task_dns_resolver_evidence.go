package etcd

import (
	"bytes"
	"encoding/hex"
	resolutionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hostresolution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"net/netip"
	"time"

	"github.com/AlanD20/groundplane/internal/common/dnsproof"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/imageref"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type TaskDNSResolverObservationEvidence struct {
	ComponentID             string    `json:"component_id"`
	ServiceID               string    `json:"service_id"`
	ArtifactID              string    `json:"artifact_id"`
	ArtifactSHA256          string    `json:"artifact_sha256"`
	RenderGeneration        uint64    `json:"render_generation"`
	ImageReference          string    `json:"image_reference"`
	VerifiedImageDigest     string    `json:"verified_image_digest"`
	ImageConfigDigest       string    `json:"image_config_digest"`
	ListenEndpoint          string    `json:"listen_endpoint"`
	ReloadSHA512            string    `json:"reload_sha512"`
	ObservedAt              time.Time `json:"observed_at"`
	StaticQueryPresent      bool      `json:"static_query_present"`
	StaticQueryName         string    `json:"static_query_name,omitempty"`
	StaticQueryIPv4         string    `json:"static_query_ipv4,omitempty"`
	StaticQuerySucceeded    bool      `json:"static_query_succeeded"`
	RecursiveQuerySucceeded bool      `json:"recursive_query_succeeded"`
	ForwarderQueryCount     uint32    `json:"forwarder_query_count"`
	ForwarderSuccessCount   uint32    `json:"forwarder_success_count"`
	ProofSHA256             string    `json:"proof_sha256"`
	CanonicalEvidence       []byte    `json:"canonical_evidence"`
}

func validateTaskDNSResolverObservationEvidence(
	resultKind taskjournal.TaskResultKind,
	candidate *TaskDNSResolverObservationEvidence,
) error {
	if candidate == nil {
		return nil
	}
	evidence := *candidate
	canonical, canonicalErr := dnsproof.Unmarshal(evidence.CanonicalEvidence)
	address, addressErr := netip.ParseAddr(evidence.StaticQueryIPv4)
	staticValid := !evidence.StaticQueryPresent && evidence.StaticQueryName == "" &&
		evidence.StaticQueryIPv4 == "" &&
		!evidence.StaticQuerySucceeded
	if evidence.StaticQueryPresent {
		staticValid = resolutionrecord.ValidPlatformDNSName(evidence.StaticQueryName) && addressErr == nil && address.Is4() &&
			!address.Is4In6() && evidence.StaticQuerySucceeded
	}
	if canonicalErr != nil || hex.EncodeToString(canonical.GetProofSha256()) != evidence.ProofSHA256 ||
		canonical.GetComponentId() != evidence.ComponentID || canonical.GetServiceId() != evidence.ServiceID ||
		canonical.GetArtifactId() != evidence.ArtifactID || resultKind != taskjournal.TaskResultCompose || ids.Validate(ids.KindComponent, evidence.ComponentID) != nil ||
		ids.Validate(
			ids.KindService,
			evidence.ServiceID,
		) != nil || ids.Validate(ids.KindConfig, evidence.ArtifactID) != nil ||
		evidence.RenderGeneration == 0 || !recordcodec.ValidSHA256(evidence.ArtifactSHA256) ||
		!imageref.IsDigestPinned(evidence.ImageReference) || !recordcodec.ValidSHA256(evidence.VerifiedImageDigest) ||
		!recordcodec.ValidNonZeroSHA256(evidence.ImageConfigDigest) ||
		hex.EncodeToString(canonical.GetImageConfigDigest()) != evidence.ImageConfigDigest ||
		evidence.ListenEndpoint != "127.0.0.1:53" || !validSHA512(evidence.ReloadSHA512) ||
		recordcodec.ValidateTimestamp("DNS resolver observation observed_at", evidence.ObservedAt) != nil || !staticValid ||
		!evidence.RecursiveQuerySucceeded || evidence.ForwarderSuccessCount != evidence.ForwarderQueryCount ||
		evidence.ForwarderQueryCount > 8 || !recordcodec.ValidSHA256(evidence.ProofSHA256) {
		return errs.New(errs.KindValidationFailed, "task DNS resolver observation evidence is invalid")
	}
	return nil
}

func taskDNSResolverEvidenceEqual(
	left *TaskDNSResolverObservationEvidence,
	right *TaskDNSResolverObservationEvidence,
) bool {
	if (left == nil) != (right == nil) {
		return false
	}
	if left == nil {
		return true
	}
	return left.ComponentID == right.ComponentID && left.ServiceID == right.ServiceID &&
		left.ArtifactID == right.ArtifactID && left.ArtifactSHA256 == right.ArtifactSHA256 &&
		left.RenderGeneration == right.RenderGeneration && left.ImageReference == right.ImageReference &&
		left.VerifiedImageDigest == right.VerifiedImageDigest && left.ImageConfigDigest == right.ImageConfigDigest &&
		left.ListenEndpoint == right.ListenEndpoint &&
		left.ReloadSHA512 == right.ReloadSHA512 && left.ObservedAt == right.ObservedAt &&
		left.StaticQueryPresent == right.StaticQueryPresent && left.StaticQueryName == right.StaticQueryName &&
		left.StaticQueryIPv4 == right.StaticQueryIPv4 && left.StaticQuerySucceeded == right.StaticQuerySucceeded &&
		left.RecursiveQuerySucceeded == right.RecursiveQuerySucceeded &&
		left.ForwarderQueryCount == right.ForwarderQueryCount &&
		left.ForwarderSuccessCount == right.ForwarderSuccessCount && left.ProofSHA256 == right.ProofSHA256 &&
		(left.CanonicalEvidence == nil) == (right.CanonicalEvidence == nil) &&
		bytes.Equal(left.CanonicalEvidence, right.CanonicalEvidence)
}
