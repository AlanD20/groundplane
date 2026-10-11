package componentaction

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/dnsconnectioncheck"
	"github.com/AlanD20/groundplane/internal/infra/docker/dnsresolverobserver"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// CheckDNSConnections uses the same compiled catalog recipe as activation's
// observation step. No action is executed and no configuration is published.
func (runtime *Runtime) CheckDNSConnections(
	ctx context.Context,
	input *agentpb.CheckDNSConnections,
) (*agentpb.DNSConnectionResult, error) {
	if runtime == nil || runtime.observer == nil || ctx == nil {
		return nil, errs.New(errs.KindInternal, "Agent DNS check runtime is unavailable")
	}
	if err := dnsconnectioncheck.ValidateRequest(input); err != nil {
		return nil, err
	}
	envelope, err := DecodeComponentAction(input.Action)
	if err != nil {
		return nil, err
	}
	_, _, recipe, err := runtime.catalog.ResolveDNSResolverObservationActionEnvelope(envelope)
	if err != nil {
		return nil, errs.New(errs.KindValidationFailed, "DNS check action is not part of the compiled resolver catalog")
	}
	target, err := dnsResolverRuntimeRequest(input.Runtime, input.Action, recipe)
	if err != nil {
		return nil, err
	}
	queries := make([]dnsresolverobserver.ConnectionQuery, len(input.Queries))
	for index, query := range input.Queries {
		queries[index] = dnsresolverobserver.ConnectionQuery{Domain: query.Domain, Name: query.Name, Type: query.Type}
	}
	checked, err := runtime.observer.CheckConnections(ctx, target, queries)
	result := &agentpb.DNSConnectionResult{RequestId: input.RequestId, ComponentId: input.Action.ComponentId,
		ArtifactId: input.Action.ArtifactId, RenderGeneration: input.Action.Generation,
		ObservedAt: timestamppb.New(time.Now().UTC())}
	if err != nil {
		kind, _ := errs.KindOf(err)
		switch kind {
		case errs.KindStateConflict:
			result.Failure = "configuration_changed"
		case errs.KindValidationFailed:
			result.Failure = "query_selection_invalid"
		default:
			result.Failure = "execution_failed"
		}
		return result, nil
	}
	result.ObservedAt = timestamppb.New(checked.ObservedAt)
	for _, row := range checked.Answers {
		answer := &agentpb.DNSConnectionAnswer{Domain: row.Domain, Name: row.Name, Server: row.Server,
			Type: row.Type, ThroughResolver: row.ThroughResolver, Outcome: string(row.Outcome), Rcode: row.Rcode,
			UsedTcp: row.UsedTCP, TotalAnswers: uint32(len(row.Answers)), ElapsedMilliseconds: uint32(row.ElapsedMilliseconds)}
		for _, record := range row.Answers[:min(len(row.Answers), dnsconnectioncheck.MaximumAnswerSample)] {
			answer.Answers = append(answer.Answers, proto.CloneOf(record))
		}
		result.Answers = append(result.Answers, answer)
	}
	if err := dnsconnectioncheck.ValidateResult(input, result); err != nil {
		return nil, err
	}
	return result, nil
}
