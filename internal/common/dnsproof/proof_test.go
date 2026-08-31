package dnsproof

import (
	"sync"
	"testing"

	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func TestCanonicalProofRejectsMutationAndVerifiesConcurrently(t *testing.T) {
	evidence := validEvidence()
	if err := Seal(evidence); err != nil {
		t.Fatal(err)
	}
	var wait sync.WaitGroup
	for range 32 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			for range 100 {
				if err := Verify(evidence); err != nil {
					t.Errorf("Verify() error = %v", err)
					return
				}
			}
		}()
	}
	wait.Wait()
	tampered := proto.Clone(evidence).(*agentpb.DNSResolverObservationEvidence)
	tampered.CatchAllQuery.Name = "changed.example."
	if err := Verify(tampered); err == nil {
		t.Fatal("Verify() accepted a proof mutated after sealing")
	}
}

func TestCanonicalProofRejectsSelfHashedInvalidSemantics(t *testing.T) {
	tests := map[string]func(*agentpb.DNSResolverObservationEvidence){
		"empty catch-all": func(evidence *agentpb.DNSResolverObservationEvidence) {
			evidence.CatchAllQuery = &agentpb.DNSQueryProof{}
		},
		"missing root answer": func(evidence *agentpb.DNSResolverObservationEvidence) {
			evidence.CatchAllQuery.Answers = nil
		},
		"direct mismatch": func(evidence *agentpb.DNSResolverObservationEvidence) {
			evidence.CatchAllQuery.DirectRcode = 2
		},
		"counter without selection": func(evidence *agentpb.DNSResolverObservationEvidence) {
			evidence.CatchAllQuery.Counters[0].After = evidence.CatchAllQuery.Counters[0].Before
		},
		"unbounded attempts": func(evidence *agentpb.DNSResolverObservationEvidence) {
			evidence.CatchAllQuery.Attempts = 4
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			evidence := validEvidence()
			mutate(evidence)
			digest, err := Digest(evidence)
			if err != nil {
				t.Fatal(err)
			}
			evidence.ProofSha256 = digest[:]
			if err := Verify(evidence); err == nil {
				t.Fatal("Verify() accepted self-hashed invalid DNS semantics")
			}
		})
	}
}

func validEvidence() *agentpb.DNSResolverObservationEvidence {
	return &agentpb.DNSResolverObservationEvidence{
		ComponentId: "cmp_exact",
		CatchAllQuery: &agentpb.DNSQueryProof{
			Name: ".", Type: agentpb.DNSQueryType_DNS_QUERY_TYPE_NS, LocalRcode: 0,
			RecursionAvailable: true, SelectedUpstream: "1.1.1.1:53", DirectRcode: 0, Attempts: 1,
			Answers: []*agentpb.DNSAnswerRecord{{
				OwnerName: ".", Type: agentpb.DNSQueryType_DNS_QUERY_TYPE_NS,
				NameServer: "a.root-servers.net.",
			}},
			Counters: []*agentpb.DNSForwardCounter{{
				Upstream: "1.1.1.1:53", Rcode: 0, Before: 4, After: 5,
			}},
		},
	}
}
