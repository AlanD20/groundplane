package agentchannel

import (
	"bytes"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/dnsproof"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Rationale: the Controller channel may validate and persist Agent evidence,
// but must not derive or synthesize any field of the exact candidate proof.
func TestDurableComposeTaskResultPreservesDNSResolverObservation(t *testing.T) {
	artifact := sha256.Sum256([]byte("candidate Corefile"))
	reload := sha512.Sum512([]byte("candidate Corefile"))
	now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	evidence := &agentpb.DNSResolverObservationEvidence{
		ComponentId: "cmp_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		ServiceId:   "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		ArtifactId:  "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAV", ArtifactSha256: artifact[:],
		RenderGeneration:    12,
		ImageReference:      "coredns/coredns@sha256:" + strings.Repeat("a", 64),
		VerifiedImageDigest: bytes.Repeat([]byte{0xaa}, sha256.Size),
		ListenEndpoint:      "127.0.0.1:53", ReloadSha512: reload[:], ObservedAt: timestamppb.New(now),
		CatchAllQuery: &agentpb.DNSQueryProof{
			Name: ".", Type: agentpb.DNSQueryType_DNS_QUERY_TYPE_NS, RecursionAvailable: true,
			SelectedUpstream: "1.1.1.1:53", Attempts: 1,
			Answers: []*agentpb.DNSAnswerRecord{{
				OwnerName: ".", Type: agentpb.DNSQueryType_DNS_QUERY_TYPE_NS,
				NameServer: "a.root-servers.net.",
			}},
			Counters: []*agentpb.DNSForwardCounter{{
				Upstream: "1.1.1.1:53", Before: 1, After: 2,
			}},
		},
		ForwarderQueries: []*agentpb.DNSQueryProof{
			{
				Name: "one.example.", Type: agentpb.DNSQueryType_DNS_QUERY_TYPE_A, LocalRcode: 3,
				DirectRcode: 3, SelectedUpstream: "1.1.1.1:53", Attempts: 1,
				Counters: []*agentpb.DNSForwardCounter{{
					Upstream: "1.1.1.1:53", Rcode: 3, Before: 2, After: 3,
				}},
			},
			{
				Name: "two.example.", Type: agentpb.DNSQueryType_DNS_QUERY_TYPE_A, LocalRcode: 3,
				DirectRcode: 3, SelectedUpstream: "8.8.8.8:53", Attempts: 1,
				Counters: []*agentpb.DNSForwardCounter{{
					Upstream: "8.8.8.8:53", Rcode: 3, Before: 4, After: 5,
				}},
			},
		},
	}
	if err := dnsproof.Seal(evidence); err != nil {
		t.Fatal(err)
	}
	rollback := proto.Clone(evidence).(*agentpb.DNSResolverObservationEvidence)
	rollbackArtifact := sha256.Sum256([]byte("restored Corefile"))
	rollbackReload := sha512.Sum512([]byte("restored Corefile"))
	rollback.ArtifactSha256 = rollbackArtifact[:]
	rollback.ReloadSha512 = rollbackReload[:]
	if err := dnsproof.Seal(rollback); err != nil {
		t.Fatal(err)
	}
	ack := &agentpb.TaskAck{
		ExecutionEpoch: 1,
		Terminal:       agentpb.TaskTerminal_TASK_TERMINAL_FAILED,
		Result: &agentpb.TaskAck_ComposeResult{ComposeResult: &agentpb.ComposeTaskResult{
			Diagnostic:                      agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_NONE,
			DnsResolverCandidateObservation: evidence,
			DnsResolverRollbackObservation:  rollback,
		}},
	}
	if err := validateComposeTaskResult(ack); err != nil {
		t.Fatalf("validateComposeTaskResult() error = %v", err)
	}
	durable := durableComposeTaskResult(ack)
	if durable.DNSResolverCandidateObservation == nil ||
		durable.DNSResolverCandidateObservation.ComponentID != evidence.GetComponentId() ||
		durable.DNSResolverCandidateObservation.ArtifactSHA256 != hex.EncodeToString(artifact[:]) ||
		durable.DNSResolverCandidateObservation.ReloadSHA512 != hex.EncodeToString(reload[:]) ||
		durable.DNSResolverCandidateObservation.ProofSHA256 != hex.EncodeToString(evidence.GetProofSha256()) ||
		!durable.DNSResolverCandidateObservation.ObservedAt.Equal(now) {
		t.Fatalf("durable DNS resolver evidence = %#v", durable.DNSResolverCandidateObservation)
	}
	if durable.DNSResolverRollbackObservation == nil ||
		durable.DNSResolverRollbackObservation.ArtifactSHA256 != hex.EncodeToString(rollbackArtifact[:]) ||
		durable.DNSResolverRollbackObservation.ProofSHA256 != hex.EncodeToString(rollback.GetProofSha256()) {
		t.Fatalf("durable DNS resolver rollback evidence = %#v", durable.DNSResolverRollbackObservation)
	}
	evidence.CatchAllQuery.Name = "changed.example."
	if err := validateComposeTaskResult(ack); err == nil {
		t.Fatal("validateComposeTaskResult() accepted a proof mutated after sealing")
	}
}
