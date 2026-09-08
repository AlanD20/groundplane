package dnsresolverobserver

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
	"encoding/json"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/dnsproof"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"github.com/coredns/caddy/caddyfile"
)

type observerRuntimeStub struct{ evidence runtimeEvidence }

func (runtime observerRuntimeStub) Inspect(context.Context, Request) (runtimeEvidence, error) {
	return runtime.evidence, nil
}
func (observerRuntimeStub) Close() error { return nil }

type observerMetricsStub struct {
	mu     sync.Mutex
	values [][]byte
}

func (metrics *observerMetricsStub) Read(context.Context, string) ([]byte, error) {
	metrics.mu.Lock()
	defer metrics.mu.Unlock()
	value := metrics.values[0]
	if len(metrics.values) > 1 {
		metrics.values = metrics.values[1:]
	}
	return append([]byte(nil), value...), nil
}

type observerDNSStub struct {
	mu    sync.Mutex
	calls []string
}

type observerDNSFunc func(
	context.Context,
	string,
	string,
	agentpb.DNSQueryType,
) (*agentpb.DNSQueryProof, error)

func (query observerDNSFunc) Query(
	ctx context.Context,
	endpoint string,
	name string,
	queryType agentpb.DNSQueryType,
) (*agentpb.DNSQueryProof, error) {
	return query(ctx, endpoint, name, queryType)
}

func (dns *observerDNSStub) Query(
	_ context.Context,
	endpoint, name string,
	queryType agentpb.DNSQueryType,
) (*agentpb.DNSQueryProof, error) {
	dns.mu.Lock()
	dns.calls = append(dns.calls, endpoint+" "+name)
	dns.mu.Unlock()
	proof := &agentpb.DNSQueryProof{Name: name, Type: queryType, LocalRcode: 0, RecursionAvailable: true}
	if name == "app.internal." {
		proof.Answers = []*agentpb.DNSAnswerRecord{{OwnerName: name, Type: queryType, Ipv4: []byte{10, 200, 0, 2}}}
	}
	if name == "." {
		proof.Answers = []*agentpb.DNSAnswerRecord{{OwnerName: ".", Type: queryType, NameServer: "a.root-servers.net."}}
		proof.LocalUsedTcp = endpoint == "127.0.0.1:53"
	}
	return proof, nil
}

// Rationale: observation must bind the mounted Agent-local candidate to the
// CoreDNS effective parsed-configuration SHA-512 reported at startup and
// bounded static/recursive/forward serving, even before the reload metric exists.
func TestObserveReturnsTypedExactCandidateEvidence(t *testing.T) {
	artifact := []byte(". {\n  reload\n  prometheus 127.0.0.1:9153\n  hosts {\n" +
		"    10.200.0.2 app.internal\n  }\n  forward corp.internal 10.30.0.1\n  forward . 1.1.1.1\n}\n")
	artifactSHA256 := sha256.Sum256(artifact)
	reloadSHA512 := testEffectiveConfigSHA512(t, "/etc/coredns/Corefile", artifact)
	imageDigest := sha256.Sum256([]byte("verified child image"))
	imageConfigDigest := sha256.Sum256([]byte("verified image config"))
	dns := &observerDNSStub{}
	baseCounters := forwardMetrics(nil, 4, 9)
	catchCounters := forwardMetrics(nil, 4, 10)
	forwardCounters := forwardMetrics(nil, 5, 10)
	executor := &Executor{
		runtime: observerRuntimeStub{evidence: runtimeEvidence{
			artifact: append([]byte(nil), artifact...), logs: reloadLog(reloadSHA512),
			verifiedImageDigest: imageDigest, imageConfigAuthority: imageConfigDigest,
		}},
		metrics: &observerMetricsStub{values: [][]byte{
			baseCounters, baseCounters, baseCounters,
			baseCounters, catchCounters,
			catchCounters, forwardCounters,
		}},
		dns: dns, now: func() time.Time { return time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC) },
	}
	evidence, err := executor.Observe(context.Background(), Request{
		ComponentID:       "cmp_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		ServiceID:         "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		ArtifactID:        "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		ArtifactSHA256:    artifactSHA256,
		RenderGeneration:  9,
		ProjectName:       "gp-platform",
		ServiceName:       "coredns",
		ArtifactTarget:    "/etc/coredns/Corefile",
		ImageReference:    "coredns/coredns@sha256:" + strings.Repeat("a", 64),
		ImageRepository:   "coredns/coredns",
		ImageIndexDigest:  artifactSHA256,
		ImageConfigDigest: imageConfigDigest,
		ImageOS:           "linux",
		ImageArchitecture: "amd64",
		ListenEndpoint:    "127.0.0.1:53",
		MetricsURL:        "http://127.0.0.1:9153/metrics",
		ReloadMetric:      "coredns_reload_version_info",
		ExpectedLabels:    map[string]string{"com.groundplane.managed": "true"},
	})
	if err != nil {
		t.Fatalf("Observe() error = %v", err)
	}
	if hex.EncodeToString(evidence.GetArtifactSha256()) != hex.EncodeToString(artifactSHA256[:]) ||
		hex.EncodeToString(evidence.GetReloadSha512()) != hex.EncodeToString(reloadSHA512[:]) ||
		hex.EncodeToString(evidence.GetVerifiedImageDigest()) != hex.EncodeToString(imageDigest[:]) ||
		hex.EncodeToString(evidence.GetImageConfigDigest()) != hex.EncodeToString(imageConfigDigest[:]) ||
		evidence.GetRenderGeneration() != 9 || evidence.GetStaticQuery().GetName() != "app.internal." ||
		len(evidence.GetStaticQuery().GetAnswers()) != 1 || evidence.GetCatchAllQuery() == nil ||
		len(evidence.GetForwarderQueries()) != 1 || evidence.GetCatchAllQuery().GetSelectedUpstream() != "1.1.1.1:53" ||
		!evidence.GetCatchAllQuery().GetLocalUsedTcp() || dnsproof.Verify(evidence) != nil || len(dns.calls) != 5 {
		t.Fatalf("observation evidence = %#v, probes = %#v", evidence, dns)
	}
}

// Rationale: raw-byte hashes, stale latest logs, malformed or ambiguous log
// evidence, and a present mismatched reload metric must never prove a generation.
func TestObserveRejectsInvalidEffectiveConfigurationEvidence(t *testing.T) {
	artifact := []byte(". {\n  reload\n  prometheus 127.0.0.1:9153\n  forward . 1.1.1.1\n}\n")
	digest := sha256.Sum256(artifact)
	effective := testEffectiveConfigSHA512(t, "/etc/coredns/Corefile", artifact)
	raw := sha512.Sum512(artifact)
	wrong := sha512.Sum512([]byte("another parsed configuration"))
	request := Request{
		ComponentID: "cmp_01ARZ3NDEKTSV4RRFFQ69G5FAV", ServiceID: "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		ArtifactID: "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAV", ArtifactSHA256: digest, RenderGeneration: 1,
		ProjectName: "gp-platform", ServiceName: "coredns", ArtifactTarget: "/etc/coredns/Corefile",
		ImageReference:  "coredns/coredns@sha256:" + strings.Repeat("a", 64),
		ImageRepository: "coredns/coredns", ImageIndexDigest: digest,
		ImageConfigDigest: digest,
		ImageOS:           "linux", ImageArchitecture: "amd64",
		ListenEndpoint: "127.0.0.1:53", MetricsURL: "http://127.0.0.1:9153/metrics",
		ReloadMetric: "coredns_reload_version_info", ExpectedLabels: map[string]string{"owned": "true"},
	}
	tests := map[string]struct {
		logs    []byte
		metrics []byte
	}{
		"raw artifact hash": {logs: reloadLog(raw), metrics: forwardMetrics(nil, 0, 0)},
		"stale matching log": {
			logs: append(reloadLog(effective), reloadLog(wrong)...), metrics: forwardMetrics(nil, 0, 0),
		},
		"malformed log": {
			logs:    []byte("[INFO] plugin/reload: Running configuration SHA512 = malformed\n"),
			metrics: forwardMetrics(nil, 0, 0),
		},
		"oversized logs": {
			logs: bytes.Repeat([]byte("x"), maximumLogBytes+1), metrics: forwardMetrics(nil, 0, 0),
		},
		"ambiguous log line": {
			logs:    append(bytes.TrimSuffix(reloadLog(effective), []byte("\n")), reloadLog(wrong)...),
			metrics: forwardMetrics(nil, 0, 0),
		},
		"present mismatched metric": {
			logs: reloadLog(effective), metrics: forwardMetrics(&wrong, 0, 0),
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			executor := &Executor{
				runtime: observerRuntimeStub{evidence: runtimeEvidence{artifact: artifact, logs: test.logs}},
				metrics: &observerMetricsStub{values: [][]byte{test.metrics}}, dns: &observerDNSStub{}, now: time.Now,
			}
			if _, err := executor.Observe(context.Background(), request); err == nil {
				t.Fatal("Observe() accepted invalid effective configuration evidence")
			}
		})
	}
}

// Rationale: catch-all forwarding promotes a resolver generation only after
// both the local resolver and selected upstream successfully return a root NS;
// matching failure rcodes or answerless success do not prove recursion.
func TestObserveForwardRequiresSuccessfulRootNSResolution(t *testing.T) {
	tests := map[string]struct {
		rcode       uint32
		includeRoot bool
		wantError   bool
	}{
		"matching SERVFAIL": {rcode: 2, wantError: true},
		"empty root answer": {rcode: 0, wantError: true},
		"valid root NS":     {rcode: 0, includeRoot: true},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			before := forwardMetrics(nil, 0, 9)
			after := forwardMetrics(nil, 0, 10)
			executor := &Executor{
				metrics: &observerMetricsStub{values: [][]byte{before, after}},
				dns: observerDNSFunc(func(
					_ context.Context,
					_ string,
					queryName string,
					queryType agentpb.DNSQueryType,
				) (*agentpb.DNSQueryProof, error) {
					proof := &agentpb.DNSQueryProof{
						Name: queryName, Type: queryType, LocalRcode: test.rcode, RecursionAvailable: true,
					}
					if test.includeRoot {
						proof.Answers = []*agentpb.DNSAnswerRecord{{
							OwnerName: ".", Type: agentpb.DNSQueryType_DNS_QUERY_TYPE_NS,
							NameServer: "a.root-servers.net.",
						}}
					}
					return proof, nil
				}),
			}
			proof, err := executor.observeForward(
				context.Background(),
				Request{ListenEndpoint: "127.0.0.1:53", MetricsURL: "http://127.0.0.1:9153/metrics"},
				forwardGroup{domain: ".", endpoints: []string{"1.1.1.1:53"}},
				".",
				agentpb.DNSQueryType_DNS_QUERY_TYPE_NS,
			)
			if (err != nil) != test.wantError {
				t.Fatalf("observeForward() = %#v, %v, want error = %t", proof, err, test.wantError)
			}
		})
	}
}

func testEffectiveConfigSHA512(t *testing.T, path string, artifact []byte) [sha512.Size]byte {
	t.Helper()
	blocks, err := caddyfile.Parse(path, bytes.NewReader(artifact), nil)
	if err != nil {
		t.Fatalf("parse Corefile fixture: %v", err)
	}
	parsed, err := json.Marshal(blocks)
	if err != nil {
		t.Fatalf("marshal Corefile fixture: %v", err)
	}
	return sha512.Sum512(parsed)
}

func reloadLog(digest [sha512.Size]byte) []byte {
	return []byte("[INFO] plugin/reload: Running configuration SHA512 = " + hex.EncodeToString(digest[:]) + "\n")
}

func forwardMetrics(reload *[sha512.Size]byte, corp, catch uint64) []byte {
	version := ""
	if reload != nil {
		version = `coredns_reload_version_info{hash="sha512",value="` + hex.EncodeToString(reload[:]) + `"} 1` + "\n"
	}
	return []byte(
		version +
			`coredns_proxy_request_duration_seconds_count{proxy_name="forward",to="10.30.0.1:53",rcode="NOERROR"} ` + strconv.FormatUint(corp, 10) + "\n" +
			`coredns_proxy_request_duration_seconds_count{proxy_name="forward",to="1.1.1.1:53",rcode="NOERROR"} ` + strconv.FormatUint(catch, 10) + "\n",
	)
}
