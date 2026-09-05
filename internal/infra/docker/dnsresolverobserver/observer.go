// Package dnsresolverobserver proves one catalog-selected DNS resolver
// generation from Agent-local runtime state and bounded serving probes.
package dnsresolverobserver

import (
	"context"
	"crypto/sha256"
	"net/http"
	"strings"
	"time"

	"github.com/moby/moby/client"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/AlanD20/groundplane/internal/common/dnsproof"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

const (
	maximumArtifactBytes = 96 * 1024
	maximumMetricsBytes  = 256 * 1024
	maximumLogBytes      = 256 * 1024
	maximumLogLines      = 256
	probeDeadline        = 60 * time.Second
	maximumProofAttempts = 3
	dockerSocketPath     = "/var/run/docker.sock"
)

type Request struct {
	ComponentID       string
	ServiceID         string
	ArtifactID        string
	ArtifactSHA256    [sha256.Size]byte
	RenderGeneration  uint64
	ProjectName       string
	ServiceName       string
	ArtifactTarget    string
	ImageReference    string
	ImageRepository   string
	ImageIndexDigest  [sha256.Size]byte
	ImageConfigDigest [sha256.Size]byte
	ImageOS           string
	ImageArchitecture string
	ImageVariant      string
	ListenEndpoint    string
	MetricsURL        string
	ReloadMetric      string
	ExpectedLabels    map[string]string
}

type runtimeEvidence struct {
	artifact                  []byte
	logs                      []byte
	verifiedImageDigest       [sha256.Size]byte
	verifiedImageConfigDigest [sha256.Size]byte
}

type runtimeInspector interface {
	Inspect(context.Context, Request) (runtimeEvidence, error)
	Close() error
}

type metricsReader interface {
	Read(context.Context, string) ([]byte, error)
}

type dnsProber interface {
	Query(context.Context, string, string, agentpb.DNSQueryType) (*agentpb.DNSQueryProof, error)
}

type Executor struct {
	runtime runtimeInspector
	metrics metricsReader
	dns     dnsProber
	now     func() time.Time
}

func New() (*Executor, error) {
	engine, err := client.New(client.WithHost("unix://" + dockerSocketPath))
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	return &Executor{
		runtime: &dockerRuntimeInspector{engine: engine}, metrics: httpMetricsReader{client: &http.Client{}},
		dns: networkDNSProber{}, now: func() time.Time { return time.Now().UTC() },
	}, nil
}

func (executor *Executor) Close() error {
	if executor == nil || executor.runtime == nil {
		return nil
	}
	return executor.runtime.Close()
}

func (executor *Executor) Observe(
	ctx context.Context,
	request Request,
) (*agentpb.DNSResolverObservationEvidence, error) {
	if executor == nil || executor.runtime == nil || executor.metrics == nil || executor.dns == nil ||
		executor.now == nil || ctx == nil || validateRequest(request) != nil {
		return nil, errs.New(errs.KindValidationFailed, "DNS resolver observation request is invalid")
	}
	proofCtx, cancel := context.WithTimeout(ctx, probeDeadline)
	defer cancel()
	runtime, err := executor.runtime.Inspect(proofCtx, request)
	if err != nil {
		return nil, err
	}
	defer clear(runtime.artifact)
	defer clear(runtime.logs)
	artifactDigest := sha256.Sum256(runtime.artifact)
	if artifactDigest != request.ArtifactSHA256 {
		return nil, errs.New(errs.KindStateConflict, "DNS resolver mounted artifact digest changed")
	}
	parsed, err := parseArtifact(runtime.artifact)
	if err != nil {
		return nil, err
	}
	effectiveDigest, err := effectiveConfigSHA512(request.ArtifactTarget, runtime.artifact)
	if err != nil {
		return nil, err
	}
	reportedDigest, err := latestReportedConfigSHA512(runtime.logs)
	if err != nil || reportedDigest != effectiveDigest {
		return nil, errs.New(errs.KindStateConflict, "DNS resolver reported configuration does not match the mounted artifact")
	}
	metrics, err := executor.metrics.Read(proofCtx, request.MetricsURL)
	if err != nil {
		return nil, err
	}
	defer clear(metrics)
	metricDigest, metricPresent, err := reloadMetricSHA512(metrics, request.ReloadMetric)
	if err != nil {
		return nil, err
	}
	if metricPresent && metricDigest != effectiveDigest {
		return nil, errs.New(errs.KindStateConflict, "DNS resolver reload metric does not match the mounted artifact")
	}
	evidence := &agentpb.DNSResolverObservationEvidence{
		ComponentId: request.ComponentID, ServiceId: request.ServiceID, ArtifactId: request.ArtifactID,
		ArtifactSha256: append([]byte(nil), artifactDigest[:]...), RenderGeneration: request.RenderGeneration,
		ImageReference:      request.ImageReference,
		VerifiedImageDigest: append([]byte(nil), runtime.verifiedImageDigest[:]...),
		ListenEndpoint:      request.ListenEndpoint, ReloadSha512: append([]byte(nil), effectiveDigest[:]...),
		ObservedAt: timestamppb.New(executor.now().UTC()), ImageRepository: request.ImageRepository,
		ImageIndexDigest: append([]byte(nil), request.ImageIndexDigest[:]...), ImageOs: request.ImageOS,
		ImageArchitecture: request.ImageArchitecture, ImageVariant: request.ImageVariant,
		ImageConfigDigest: append([]byte(nil), runtime.verifiedImageConfigDigest[:]...),
	}
	if parsed.staticName != "" {
		evidence.StaticQuery, err = executor.observeStatic(proofCtx, request, parsed)
		if err != nil {
			return nil, err
		}
	}
	catchAll, found := parsed.forwardGroup(".")
	if !found {
		return nil, errs.New(errs.KindStateConflict, "DNS resolver catch-all forwarder is missing")
	}
	evidence.CatchAllQuery, err = executor.observeForward(
		proofCtx, request, catchAll, ".", agentpb.DNSQueryType_DNS_QUERY_TYPE_NS,
	)
	if err != nil {
		return nil, err
	}
	for index, group := range parsed.forwardGroups {
		if group.domain == "." {
			continue
		}
		name := forwardProofName(
			request.ComponentID, request.RenderGeneration, index, strings.TrimSuffix(group.domain, "."),
		)
		proof, proofErr := executor.observeForward(
			proofCtx, request, group, name, agentpb.DNSQueryType_DNS_QUERY_TYPE_A,
		)
		if proofErr != nil {
			return nil, proofErr
		}
		evidence.ForwarderQueries = append(evidence.ForwarderQueries, proof)
	}
	if err := dnsproof.Seal(evidence); err != nil {
		return nil, err
	}
	return evidence, nil
}
