package dnsresolver

import (
	"context"
	"net/netip"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd/dnschecks"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (service *ConnectionCheckService) GetConnectionCheck(
	ctx context.Context,
	componentID, taskID string,
) (apiTypes.DNSCheckResult, error) {
	var record dnschecks.Record
	if taskID == "" {
		loaded, err := service.records.Latest(ctx, componentID)
		if err != nil {
			return apiTypes.DNSCheckResult{}, err
		}
		record = loaded.Record
	} else {
		loaded, err := service.records.Load(ctx, taskID)
		if err != nil {
			return apiTypes.DNSCheckResult{}, err
		}
		record = loaded.Record
	}
	if record.Source.ComponentID != componentID {
		return apiTypes.DNSCheckResult{}, errs.New(
			errs.KindRequestNotFound,
			"DNS check was not found for this Component",
		)
	}
	stale, err := service.planner.components.DNSCheckSourcesChanged(ctx, record.Source)
	if err != nil {
		return apiTypes.DNSCheckResult{}, err
	}
	return projectDNSCheck(record, stale)
}

func projectDNSCheck(record dnschecks.Record, stale bool) (apiTypes.DNSCheckResult, error) {
	request, err := record.Input()
	if err != nil {
		return apiTypes.DNSCheckResult{}, err
	}
	source := record.Source
	configuration := apiTypes.DNSCheckConfiguration{
		ComponentID:         source.ComponentID,
		ComponentRevision:   source.ComponentRevision,
		ObservationRevision: source.ObservationRevision,
		AgentID:             source.AgentID,
		AgentGeneration:     source.AgentGeneration,
		AppliedTaskID:       source.AppliedTaskID,
		ServiceID:           source.ServiceID,
		ArtifactID:          source.ArtifactID,
		ArtifactSHA256:      source.ArtifactSHA256,
		ImageReference:      source.ImageReference,
		RenderGeneration:    source.RenderGeneration,
		Queries:             []apiTypes.DNSCheckQuery{},
	}
	for _, query := range request.Queries {
		configuration.Queries = append(
			configuration.Queries,
			apiTypes.DNSCheckQuery{Domain: query.Domain, Name: query.Name, Type: dnsCheckType(query.Type)},
		)
	}
	result := apiTypes.DNSCheckResult{TaskID: record.Owner.TaskID, ExecutionLocation: "CoreDNS host network",
		Configuration: configuration, Stale: stale, Phase: string(record.Phase), Outcome: record.Outcome, Failure: record.Failure,
		Answers: []apiTypes.DNSCheckAnswer{}, CreatedAt: record.CreatedAt.Format(time.RFC3339)}
	if record.StartedAt != nil {
		result.StartedAt = record.StartedAt.Format(time.RFC3339)
	}
	if record.FinishedAt != nil {
		result.FinishedAt = record.FinishedAt.Format(time.RFC3339)
	}
	observation, err := record.Observation()
	if err != nil {
		return apiTypes.DNSCheckResult{}, err
	}
	if observation == nil {
		return result, nil
	}
	result.ObservedAt = observation.ObservedAt.AsTime().Format(time.RFC3339)
	for _, answer := range observation.Answers {
		row := apiTypes.DNSCheckAnswer{
			Domain:              answer.Domain,
			Name:                answer.Name,
			Type:                dnsCheckType(answer.Type),
			Server:              answer.Server,
			ThroughResolver:     answer.ThroughResolver,
			Outcome:             answer.Outcome,
			Rcode:               answer.Rcode,
			UsedTCP:             answer.UsedTcp,
			TotalAnswers:        answer.TotalAnswers,
			ElapsedMilliseconds: answer.ElapsedMilliseconds,
			Answers:             []apiTypes.DNSCheckAnswerSample{},
		}
		for _, sample := range answer.Answers {
			value := sample.NameServer
			if sample.Type == agentpb.DNSQueryType_DNS_QUERY_TYPE_A {
				address, valid := netip.AddrFromSlice(sample.Ipv4)
				if !valid {
					return apiTypes.DNSCheckResult{}, errs.New(
						errs.KindInternal,
						"DNS check contains an invalid IPv4 answer",
					)
				}
				value = address.String()
			}
			row.Answers = append(
				row.Answers,
				apiTypes.DNSCheckAnswerSample{Name: sample.OwnerName, Type: dnsCheckType(sample.Type), Value: value},
			)
		}
		result.Answers = append(result.Answers, row)
	}
	return result, nil
}

func dnsCheckType(value agentpb.DNSQueryType) string {
	if value == agentpb.DNSQueryType_DNS_QUERY_TYPE_NS {
		return "NS"
	}
	return "A"
}
