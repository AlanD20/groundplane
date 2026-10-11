package dnsresolverobserver

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/dnsname"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// ConnectionQuery selects a name inside a configured forwarding domain. Empty
// selection uses root NS for catch-all and an A lookup of each forwarder domain.
// Endpoints always come from the verified serving artifact, never this input.
type ConnectionQuery struct {
	Domain string
	Name   string
	Type   agentpb.DNSQueryType
}

type ConnectionOutcome string

const (
	ConnectionResolved    ConnectionOutcome = "resolved"
	ConnectionNameMissing ConnectionOutcome = "name_not_found"
	ConnectionNoAnswer    ConnectionOutcome = "no_answer"
	ConnectionRefused     ConnectionOutcome = "refused"
	ConnectionServerError ConnectionOutcome = "server_error"
	ConnectionTimeout     ConnectionOutcome = "timeout"
	ConnectionQueryFailed ConnectionOutcome = "query_failed"
	ConnectionCanceled    ConnectionOutcome = "canceled"
)

// ConnectionAnswer is a bounded DNS response, not a container/socket health
// claim. NXDOMAIN is distinct from successful resolution and transport failure.
type ConnectionAnswer struct {
	Domain, Name, Server string
	Type                 agentpb.DNSQueryType
	ThroughResolver      bool
	Outcome              ConnectionOutcome
	Rcode                *uint32
	UsedTCP              bool
	Answers              []*agentpb.DNSAnswerRecord
	ElapsedMilliseconds  int64
}

type ConnectionCheckResult struct {
	ComponentID, ArtifactID string
	RenderGeneration        uint64
	ObservedAt              time.Time
	Answers                 []ConnectionAnswer
}

// CheckConnections verifies the exact running CoreDNS configuration first.
// Both Agent and CoreDNS use host networking, so these queries originate in
// the resolver's network context. Each configured endpoint is queried once;
// UDP truncation may require the existing client's bounded TCP continuation.
// No reload, cache flush, configuration write or external mutation is performed.
func (executor *Executor) CheckConnections(ctx context.Context, request Request,
	queries []ConnectionQuery,
) (ConnectionCheckResult, error) {
	if executor == nil || executor.runtime == nil || executor.metrics == nil || executor.dns == nil ||
		executor.now == nil || ctx == nil || validateRequest(request) != nil {
		return ConnectionCheckResult{}, errs.New(errs.KindValidationFailed, "DNS connection check target is invalid")
	}
	checkCtx, cancel := context.WithTimeout(ctx, probeDeadline)
	defer cancel()
	configuration, ready, err := executor.inspectConfiguration(checkCtx, request)
	if err != nil {
		return ConnectionCheckResult{}, err
	}
	if !ready {
		return ConnectionCheckResult{}, errs.New(
			errs.KindStateConflict,
			"CoreDNS has not loaded the selected configuration; wait for its update before checking DNS",
		)
	}
	selected, err := selectConnectionQueries(configuration.parsed, queries)
	if err != nil {
		return ConnectionCheckResult{}, err
	}
	result := ConnectionCheckResult{ComponentID: request.ComponentID, ArtifactID: request.ArtifactID,
		RenderGeneration: request.RenderGeneration}
	for index, group := range configuration.parsed.forwardGroups {
		query := selected[index]
		result.Answers = append(result.Answers, executor.checkConnection(checkCtx, query, request.ListenEndpoint, true))
		for _, endpoint := range group.endpoints {
			result.Answers = append(result.Answers, executor.checkConnection(checkCtx, query, endpoint, false))
		}
	}
	// Queries can overlap an operator's configuration update. Verify the same
	// serving artifact again, rather than attributing mixed results to old files.
	settlement, finish := context.WithTimeout(ctx, 5*time.Second)
	defer finish()
	_, ready, err = executor.inspectConfiguration(settlement, request)
	if err != nil {
		return ConnectionCheckResult{}, err
	}
	if !ready {
		return ConnectionCheckResult{}, errs.New(
			errs.KindStateConflict,
			"CoreDNS configuration changed while DNS was being checked; submit a new check after its update",
		)
	}
	result.ObservedAt = executor.now().UTC()
	return result, nil
}

func selectConnectionQueries(artifact parsedArtifact, supplied []ConnectionQuery) ([]ConnectionQuery, error) {
	// Eight authored domains plus optional Tailnet delegation and catch-all.
	if len(artifact.forwardGroups) == 0 || len(artifact.forwardGroups) > 10 ||
		len(supplied) > len(artifact.forwardGroups) {
		return nil, errs.New(errs.KindValidationFailed, "DNS connection check requires bounded configured forwarders")
	}
	selected := make([]ConnectionQuery, len(artifact.forwardGroups))
	for index, group := range artifact.forwardGroups {
		if len(group.endpoints) == 0 || len(group.endpoints) > 15 {
			return nil, errs.New(
				errs.KindValidationFailed,
				"DNS connection check requires one to fifteen configured endpoints per forwarder",
			)
		}
		query := ConnectionQuery{Domain: group.domain, Name: group.domain, Type: agentpb.DNSQueryType_DNS_QUERY_TYPE_A}
		if group.domain == "." {
			query.Type = agentpb.DNSQueryType_DNS_QUERY_TYPE_NS
		}
		selected[index] = query
	}
	seen := make(map[string]struct{}, len(supplied))
	for _, query := range supplied {
		if query.Domain == "" || query.Name == "" {
			return nil, errs.New(errs.KindValidationFailed, "DNS check domain and query name are required")
		}
		query.Domain = absoluteName(query.Domain)
		query.Name = absoluteName(query.Name)
		if _, exists := seen[query.Domain]; exists {
			return nil, errs.New(errs.KindValidationFailed, "DNS check repeats a forwarding domain")
		}
		seen[query.Domain] = struct{}{}
		index := -1
		for candidate, group := range artifact.forwardGroups {
			if group.domain == query.Domain {
				index = candidate
			}
		}
		if index < 0 || !validConnectionQuery(artifact, query) {
			return nil, errs.New(
				errs.KindValidationFailed,
				"DNS check name must use its configured forwarder and A or NS query type",
			)
		}
		selected[index] = query
	}
	return selected, nil
}

func validConnectionQuery(artifact parsedArtifact, query ConnectionQuery) bool {
	if query.Name != "." && !dnsname.Valid(strings.TrimSuffix(query.Name, ".")) ||
		query.Type != agentpb.DNSQueryType_DNS_QUERY_TYPE_A && query.Type != agentpb.DNSQueryType_DNS_QUERY_TYPE_NS ||
		artifact.staticName != "" && query.Name == absoluteName(artifact.staticName) {
		return false
	}
	// Choose the same longest domain suffix as CoreDNS. A request for the
	// catch-all cannot accidentally test another configured forwarder.
	matched := "."
	for _, group := range artifact.forwardGroups {
		if group.domain != "." && (query.Name == group.domain || strings.HasSuffix(query.Name, "."+group.domain)) &&
			len(group.domain) > len(matched) {
			matched = group.domain
		}
	}
	return matched == query.Domain
}

func (executor *Executor) checkConnection(ctx context.Context, query ConnectionQuery,
	endpoint string, throughResolver bool,
) ConnectionAnswer {
	started := time.Now()
	result := ConnectionAnswer{Domain: query.Domain, Name: query.Name, Server: endpoint,
		Type: query.Type, ThroughResolver: throughResolver}
	queryCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	proof, err := executor.dns.Query(queryCtx, endpoint, query.Name, query.Type)
	if err != nil {
		switch {
		case errors.Is(ctx.Err(), context.Canceled):
			result.Outcome = ConnectionCanceled
		case errors.Is(queryCtx.Err(), context.DeadlineExceeded):
			result.Outcome = ConnectionTimeout
		default:
			result.Outcome = ConnectionQueryFailed
		}
	} else {
		rcode := proof.GetLocalRcode()
		result.Rcode = &rcode
		result.UsedTCP, result.Answers = proof.GetLocalUsedTcp(), proof.GetAnswers()
		switch rcode {
		case 0:
			result.Outcome = ConnectionNoAnswer
			for _, answer := range result.Answers {
				if answer.GetType() == query.Type {
					result.Outcome = ConnectionResolved
				}
			}
		case 3:
			result.Outcome = ConnectionNameMissing
		case 5:
			result.Outcome = ConnectionRefused
		default:
			result.Outcome = ConnectionServerError
		}
	}
	cancel()
	result.ElapsedMilliseconds = time.Since(started).Milliseconds()
	return result
}
