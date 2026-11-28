package dnsresolverobserver

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane-component-sdk/dnsresolver"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"golang.org/x/net/dns/dnsmessage"
)

// DNS-02/04: prove the exact serving generation with mounted bytes and a real
// DNS wire exchange even when Docker logs are unavailable. A stale or
// forwarded answer cannot attest the mounted candidate.
func TestMarkedConfigurationRequiresExactLocalTXT(t *testing.T) {
	base := []byte(". {\n    bind 127.0.0.1\n    forward . 1.1.1.1\n    reload\n" +
		"    prometheus 127.0.0.1:9153\n    log\n    errors\n}\n")
	marked, err := dnsresolver.AddCorefileServingProof(base)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(base)
	wantTXT := hex.EncodeToString(digest[:])
	image := sha256.Sum256([]byte("running CoreDNS image"))
	imageID := "sha256:" + hex.EncodeToString(image[:])
	stale := sha512.Sum512(base)
	loopback := "127.0.0.1:53"

	for _, test := range []struct {
		name        string
		edit        func(*dnsmessage.Message)
		forwards    uint64
		wantReady   bool
		wantTimeout bool
	}{
		{name: "exact authoritative answer with unreadable logs and stale metric", wantReady: true},
		{name: "stale running digest", edit: func(msg *dnsmessage.Message) {
			msg.Answers[0].Body = &dnsmessage.TXTResource{TXT: []string{strings.Repeat("0", 64)}}
		}},
		{name: "absent record", wantTimeout: true, edit: func(msg *dnsmessage.Message) {
			msg.Answers, msg.RCode = nil, dnsmessage.RCodeNameError
		}},
		{name: "recursion offered", edit: func(msg *dnsmessage.Message) { msg.RecursionAvailable = true }},
		{name: "not authoritative", edit: func(msg *dnsmessage.Message) { msg.Authoritative = false }},
		{name: "cacheable answer", edit: func(msg *dnsmessage.Message) { msg.Answers[0].Header.TTL = 60 }},
		{name: "different owner", edit: func(msg *dnsmessage.Message) {
			msg.Answers[0].Header.Name = dnsmessage.MustNewName("other.invalid.")
		}},
		{name: "different class", edit: func(msg *dnsmessage.Message) {
			msg.Answers[0].Header.Class = dnsmessage.ClassCHAOS
		}},
		{name: "multiple answers", edit: func(msg *dnsmessage.Message) {
			msg.Answers = append(msg.Answers, msg.Answers[0])
		}},
		{name: "multiple TXT strings", edit: func(msg *dnsmessage.Message) {
			msg.Answers[0].Body = &dnsmessage.TXTResource{TXT: []string{wantTXT[:32], wantTXT[32:]}}
		}},
		{name: "unexpected authority", edit: func(msg *dnsmessage.Message) {
			msg.Authorities = []dnsmessage.Resource{msg.Answers[0]}
		}},
		{name: "unexpected forwarding", forwards: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := Request{
				ProjectName: "groundplane-infra", ServiceName: "coredns", ArtifactTarget: "/etc/coredns/Corefile",
				ImageReference: "example/resolver@" + imageID, ImageConfigDigest: image,
				ExpectedLabels: map[string]string{"owned": "true"}, ImageOS: "linux", ImageArchitecture: "amd64",
				ArtifactSHA256: sha256.Sum256(marked), ListenEndpoint: loopback,
			}
			engine := imageIdentityEngine{
				containerImageID: imageID, inspectedImageID: imageID, imageReference: request.ImageReference,
				artifact: marked, platform: ocispec.Platform{OS: "linux", Architecture: "amd64"},
				logError: errors.New("docker logs cannot decode the existing stream"),
			}
			calls := 0
			executor := &Executor{
				runtime: &dockerRuntimeInspector{engine: engine},
				metrics: &observerMetricsStub{values: [][]byte{
					forwardMetrics(&stale, 0, 0), forwardMetrics(&stale, 0, test.forwards),
				}},
				dns: networkDNSProber{exchange: func(
					ctx context.Context, endpoint string, encoded []byte, tcp bool,
				) ([]byte, error) {
					calls++
					var req dnsmessage.Message
					if err := req.Unpack(encoded); err != nil {
						return nil, err
					}
					if !tcp || endpoint != loopback || req.RecursionDesired || len(req.Questions) != 1 {
						t.Fatalf("unexpected proof transport or query: %s, TCP=%t, %+v", endpoint, tcp, req)
					}
					question := req.Questions[0]
					host, suffix, _ := strings.Cut(question.Name.String(), ".")
					random, err := hex.DecodeString(host)
					if len(random) != 16 || err != nil || suffix != dnsresolver.ServingProofZone ||
						question.Type != dnsmessage.TypeTXT || question.Class != dnsmessage.ClassINET {
						t.Fatalf("unexpected proof question: %+v", question)
					}
					if deadline, ok := ctx.Deadline(); !ok || time.Until(deadline) > 3*time.Second {
						t.Fatalf("activation proof lacks a bounded deadline: %v", deadline)
					}
					reply := dnsmessage.Message{
						Header:    dnsmessage.Header{ID: req.ID, Response: true, Authoritative: true},
						Questions: req.Questions,
						Answers: []dnsmessage.Resource{{
							Header: dnsmessage.ResourceHeader{
								Name:  question.Name,
								Type:  question.Type,
								Class: question.Class,
							},
							Body: &dnsmessage.TXTResource{TXT: []string{wantTXT}},
						}},
					}
					if test.edit != nil {
						test.edit(&reply)
					}
					return reply.Pack()
				}},
			}
			actual, ready, err := executor.inspectConfiguration(context.Background(), request)
			if err != nil || ready != test.wantReady || calls != 1 {
				t.Fatalf("marked activation: ready=%t, error=%v, DNS calls=%d", ready, err, calls)
			}
			if ready && actual.effectiveDigest != testEffectiveConfigSHA512(t, request.ArtifactTarget, marked) {
				t.Fatal("accepted wire evidence lost the effective Corefile digest")
			}
			if test.wantTimeout {
				ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
				defer cancel()
				if _, err := executor.awaitConfiguration(ctx, request); !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("absent serving proof must wait within the proof deadline: %v", err)
				}
			}
			if test.wantReady {
				request.ArtifactSHA256 = sha256.Sum256(base)
				if _, ready, err := executor.inspectConfiguration(context.Background(), request); ready || err == nil ||
					calls != 1 {
					t.Fatalf("mounted digest mismatch: ready=%t, error=%v, DNS calls=%d", ready, err, calls)
				}
			}
		})
	}

	// The mounted SHA can match a captured artifact whose marker is malformed.
	// The invalid marker must fail before logs, metrics or DNS are consulted.
	invalid := bytes.Replace(marked, []byte("1.1.1.1"), []byte("8.8.8.8"), 1)
	executor := &Executor{runtime: observerRuntimeStub{evidence: runtimeEvidence{artifact: invalid}}}
	request := Request{ArtifactTarget: "/etc/coredns/Corefile", ArtifactSHA256: sha256.Sum256(invalid)}
	if _, ready, err := executor.inspectConfiguration(context.Background(), request); ready || err == nil {
		t.Fatalf("invalid marker: ready=%t, error=%v", ready, err)
	}
}
