package etcd

import (
	"encoding/hex"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/dnsproof"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestTaskResultRecordRoundTripsAsBoundedSummary(t *testing.T) {
	record := validTaskRecord(taskJournalTime())
	startedAt := record.CreatedAt.Add(1)
	terminalAt := record.CreatedAt.Add(2)
	record.Status = TaskStatusCompleted
	record.StartedAt = &startedAt
	record.FinishedAt = &terminalAt
	record.UpdatedAt = terminalAt
	retainUntil := terminalAt.Add(TaskRetention)
	record.RetainUntil = &retainUntil
	result := completedComposeTaskResult()
	result.Projects = []TaskObservedProjectSummary{{
		ProjectName: "gp-platform", ObservedAt: terminalAt,
		ContainerCount: 3, NetworkCount: 2, VolumeCount: 1,
	}}
	result.DNSResolverCandidateObservation = testDurableDNSProof(
		"cmp_01ARZ3NDEKTSV4RRFFQ69G5FAV", "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		"cfg_01ARZ3NDEKTSV4RRFFQ69G5FAV", strings.Repeat("1", 64), 7, terminalAt, 0,
	)
	record.Result = &result

	encoded, err := encodeTaskRecord(record)
	if err != nil {
		t.Fatalf("encodeTaskRecord() error = %v", err)
	}
	decoded, err := decodeTaskRecord(encoded)
	if err != nil {
		t.Fatalf("decodeTaskRecord() error = %v", err)
	}
	if !reflect.DeepEqual(decoded.Result, record.Result) {
		t.Fatalf("decoded result = %#v, want %#v", decoded.Result, record.Result)
	}
	cloned := cloneTaskResult(record.Result)
	cloned.DNSResolverCandidateObservation.ProofSHA256 = strings.Repeat("5", 64)
	if record.Result.DNSResolverCandidateObservation.ProofSHA256 == cloned.DNSResolverCandidateObservation.ProofSHA256 {
		t.Fatal("cloneTaskResult() aliased immutable DNS resolver evidence")
	}
}

func testDurableDNSProof(
	componentID, serviceID, artifactID, artifactSHA string,
	generation uint64,
	observedAt time.Time,
	forwarders int,
) *TaskDNSResolverObservationEvidence {
	artifact, _ := hex.DecodeString(artifactSHA)
	image, _ := hex.DecodeString(strings.Repeat("2", 64))
	reload, _ := hex.DecodeString(strings.Repeat("3", 128))
	evidence := &agentpb.DNSResolverObservationEvidence{
		ComponentId: componentID, ServiceId: serviceID, ArtifactId: artifactID,
		ArtifactSha256: artifact, RenderGeneration: generation,
		ImageReference:      "coredns/coredns@sha256:" + strings.Repeat("2", 64),
		VerifiedImageDigest: image, ListenEndpoint: "127.0.0.1:53", ReloadSha512: reload,
		ObservedAt: timestamppb.New(observedAt),
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
	return &TaskDNSResolverObservationEvidence{
		ComponentID:             componentID,
		ServiceID:               serviceID,
		ArtifactID:              artifactID,
		ArtifactSHA256:          artifactSHA,
		RenderGeneration:        generation,
		ImageReference:          evidence.ImageReference,
		VerifiedImageDigest:     strings.Repeat("2", 64),
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

func completedComposeTaskResult() TaskResultRecord {
	return TaskResultRecord{
		Kind:       TaskResultCompose,
		Diagnostic: TaskResultDiagnosticNone,
	}
}
