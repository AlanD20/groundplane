package dnsresolverobserver

import (
	"context"
	"testing"

	"github.com/AlanD20/groundplane/proto/agentpb"
	"golang.org/x/net/dns/dnsmessage"
)

// Rationale: a truncated UDP response is not serving evidence until the same
// exact query succeeds over TCP with a bounded complete response.
func TestNetworkDNSProberFallsBackFromUDPTruncationToTCP(t *testing.T) {
	transports := []bool{}
	prober := networkDNSProber{exchange: func(_ context.Context, _ string, encoded []byte, tcp bool) ([]byte, error) {
		transports = append(transports, tcp)
		query := dnsmessage.Message{}
		if err := query.Unpack(encoded); err != nil {
			return nil, err
		}
		response := dnsmessage.Message{
			Header:    dnsmessage.Header{ID: query.Header.ID, Response: true, Truncated: !tcp, RecursionAvailable: tcp},
			Questions: query.Questions,
		}
		if tcp {
			root, _ := dnsmessage.NewName(".")
			server, _ := dnsmessage.NewName("a.root-servers.net.")
			response.Answers = []dnsmessage.Resource{{
				Header: dnsmessage.ResourceHeader{Name: root, Type: dnsmessage.TypeNS, Class: dnsmessage.ClassINET},
				Body:   &dnsmessage.NSResource{NS: server},
			}}
		}
		return response.Pack()
	}}
	proof, err := prober.Query(context.Background(), "127.0.0.1:53", ".", agentpb.DNSQueryType_DNS_QUERY_TYPE_NS)
	if err != nil || !proof.GetLocalUsedTcp() || len(proof.GetAnswers()) != 1 ||
		len(transports) != 2 || transports[0] || !transports[1] {
		t.Fatalf("Query() = %#v, %v, transports = %v", proof, err, transports)
	}
}

// Rationale: an unrelated, query-shaped, or structurally unbounded datagram
// must never be encoded as evidence for the request that the observer sent.
func TestNetworkDNSProberRejectsInvalidResponses(t *testing.T) {
	root, _ := dnsmessage.NewName(".")
	other, _ := dnsmessage.NewName("other.example.")
	server, _ := dnsmessage.NewName("a.root-servers.net.")
	resource := dnsmessage.Resource{
		Header: dnsmessage.ResourceHeader{Name: root, Type: dnsmessage.TypeNS, Class: dnsmessage.ClassINET},
		Body:   &dnsmessage.NSResource{NS: server},
	}
	tests := map[string]func(dnsmessage.Message) ([]byte, error){
		"malformed": func(dnsmessage.Message) ([]byte, error) { return []byte{0, 1, 2}, nil },
		"non-response": func(response dnsmessage.Message) ([]byte, error) {
			response.Header.Response = false
			return response.Pack()
		},
		"mismatched transaction": func(response dnsmessage.Message) ([]byte, error) {
			response.Header.ID++
			return response.Pack()
		},
		"missing question": func(response dnsmessage.Message) ([]byte, error) {
			response.Questions = nil
			return response.Pack()
		},
		"mismatched question name": func(response dnsmessage.Message) ([]byte, error) {
			response.Questions[0].Name = other
			return response.Pack()
		},
		"mismatched question type": func(response dnsmessage.Message) ([]byte, error) {
			response.Questions[0].Type = dnsmessage.TypeA
			return response.Pack()
		},
		"mismatched question class": func(response dnsmessage.Message) ([]byte, error) {
			response.Questions[0].Class = dnsmessage.ClassCHAOS
			return response.Pack()
		},
		"extra question": func(response dnsmessage.Message) ([]byte, error) {
			response.Questions = append(response.Questions, response.Questions[0])
			return response.Pack()
		},
		"too many answers": func(response dnsmessage.Message) ([]byte, error) {
			response.Answers = make([]dnsmessage.Resource, 65)
			for index := range response.Answers {
				response.Answers[index] = resource
			}
			return response.Pack()
		},
		"too many authorities": func(response dnsmessage.Message) ([]byte, error) {
			response.Authorities = make([]dnsmessage.Resource, 65)
			for index := range response.Authorities {
				response.Authorities[index] = resource
			}
			return response.Pack()
		},
		"too many additionals": func(response dnsmessage.Message) ([]byte, error) {
			response.Additionals = make([]dnsmessage.Resource, 65)
			for index := range response.Additionals {
				response.Additionals[index] = resource
			}
			return response.Pack()
		},
	}
	for name, build := range tests {
		t.Run(name, func(t *testing.T) {
			prober := networkDNSProber{exchange: func(
				_ context.Context,
				_ string,
				encoded []byte,
				_ bool,
			) ([]byte, error) {
				query := dnsmessage.Message{}
				if err := query.Unpack(encoded); err != nil {
					return nil, err
				}
				return build(dnsmessage.Message{
					Header:    dnsmessage.Header{ID: query.Header.ID, Response: true, RecursionAvailable: true},
					Questions: query.Questions,
				})
			}}
			if _, err := prober.Query(
				context.Background(), "127.0.0.1:53", ".", agentpb.DNSQueryType_DNS_QUERY_TYPE_NS,
			); err == nil {
				t.Fatal("Query() accepted an invalid DNS response")
			}
		})
	}
}

// Rationale: a bounded response to the exact root NS request is the minimum
// parser evidence that the recursive serving proof may evaluate.
func TestNetworkDNSProberAcceptsExactRootNSResponse(t *testing.T) {
	prober := networkDNSProber{exchange: func(_ context.Context, _ string, encoded []byte, _ bool) ([]byte, error) {
		query := dnsmessage.Message{}
		if err := query.Unpack(encoded); err != nil {
			return nil, err
		}
		root, _ := dnsmessage.NewName(".")
		server, _ := dnsmessage.NewName("a.root-servers.net.")
		response := dnsmessage.Message{
			Header:    dnsmessage.Header{ID: query.Header.ID, Response: true, RecursionAvailable: true},
			Questions: query.Questions,
			Answers: []dnsmessage.Resource{{
				Header: dnsmessage.ResourceHeader{Name: root, Type: dnsmessage.TypeNS, Class: dnsmessage.ClassINET},
				Body:   &dnsmessage.NSResource{NS: server},
			}},
		}
		return response.Pack()
	}}
	proof, err := prober.Query(context.Background(), "127.0.0.1:53", ".", agentpb.DNSQueryType_DNS_QUERY_TYPE_NS)
	if err != nil || proof.GetLocalRcode() != 0 || len(proof.GetAnswers()) != 1 ||
		proof.GetAnswers()[0].GetOwnerName() != "." || proof.GetAnswers()[0].GetNameServer() == "" {
		t.Fatalf("Query() = %#v, %v", proof, err)
	}
}
