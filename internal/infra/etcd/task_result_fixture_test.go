package etcd

import (
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/dnsproof"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func testDurableDNSProof(
	componentID, serviceID, artifactID, artifactSHA string,
	generation uint64,
	observedAt time.Time,
	forwarders int,
) *testtaskjournal.TaskDNSResolverObservationEvidence {
	artifact, _ := hex.DecodeString(artifactSHA)
	image, _ := hex.DecodeString(strings.Repeat("2", 64))
	config, _ := hex.DecodeString(strings.Repeat("9", 64))
	reload, _ := hex.DecodeString(strings.Repeat("3", 128))
	evidence := &agentpb.DNSResolverObservationEvidence{
		ComponentId: componentID, ServiceId: serviceID, ArtifactId: artifactID,
		ArtifactSha256: artifact, RenderGeneration: generation,
		ImageReference:      "coredns/coredns@sha256:" + strings.Repeat("2", 64),
		VerifiedImageDigest: image, ListenEndpoint: "127.0.0.1:53", ReloadSha512: reload,
		ImageConfigDigest: config,
		ObservedAt:        timestamppb.New(observedAt),
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
	}
	for index := range forwarders {
		upstream := fmt.Sprintf("192.0.2.%d:53", index+1)
		evidence.ForwarderQueries = append(
			evidence.ForwarderQueries,
			&agentpb.DNSQueryProof{
				Name: fmt.Sprintf("proof-%d.example.", index+1),
				Type: agentpb.DNSQueryType_DNS_QUERY_TYPE_A, LocalRcode: 3, DirectRcode: 3,
				SelectedUpstream: upstream, Attempts: 1,
				Counters: []*agentpb.DNSForwardCounter{{
					Upstream: upstream, Rcode: 3, Before: uint64(index + 1), After: uint64(index + 2),
				}},
			},
		)
	}
	_ = dnsproof.Seal(evidence)
	canonical, _ := dnsproof.Marshal(evidence)
	return &testtaskjournal.TaskDNSResolverObservationEvidence{
		ComponentID:             componentID,
		ServiceID:               serviceID,
		ArtifactID:              artifactID,
		ArtifactSHA256:          artifactSHA,
		RenderGeneration:        generation,
		ImageReference:          evidence.ImageReference,
		VerifiedImageDigest:     strings.Repeat("2", 64),
		ImageConfigDigest:       strings.Repeat("9", 64),
		ListenEndpoint:          "127.0.0.1:53",
		ReloadSHA512:            strings.Repeat("3", 128),
		ObservedAt:              observedAt,
		RecursiveQuerySucceeded: true,
		ForwarderQueryCount:     uint32(forwarders),
		ForwarderSuccessCount:   uint32(forwarders),
		ProofSHA256:             hex.EncodeToString(evidence.ProofSha256),
		CanonicalEvidence:       canonical,
	}
}

func completedComposeTaskResult() testtaskjournal.TaskResultRecord {
	return testtaskjournal.TaskResultRecord{
		Kind:           testtaskjournal.TaskResultCompose,
		Diagnostic:     testtaskjournal.TaskResultDiagnosticNone,
		ExecutionEpoch: 1,
	}
}
