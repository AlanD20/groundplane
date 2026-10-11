// Package dnsconnectioncheck owns the closed read-only resolver-check exchange.
// It carries captured catalog/runtime identity, never commands or credentials.
package dnsconnectioncheck

import (
	"bytes"
	"net"
	"net/netip"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/dnsname"
	"github.com/AlanD20/groundplane/internal/common/dnsproof"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

const (
	ExchangeTimeout     = 75 * time.Second
	MaximumRequestBytes = 256 << 10
	MaximumResultBytes  = 448 << 10
	MaximumAnswerSample = 4
)

func ValidateRequest(request *agentpb.CheckDNSConnections) error {
	if request == nil || executionplan.RejectUnknown(request) != nil || !validRequestID(request.RequestId) ||
		proto.Size(request) > MaximumRequestBytes || request.Action == nil || request.Runtime == nil ||
		dnsproof.Verify(request.PriorObservation) != nil || len(request.Queries) > 10 {
		return invalid()
	}
	action, runtime, proof := request.Action, request.Runtime, request.PriorObservation
	if ids.Validate(ids.KindComponent, proof.ComponentId) != nil ||
		ids.Validate(ids.KindService, proof.ServiceId) != nil ||
		ids.Validate(ids.KindConfig, proof.ArtifactId) != nil ||
		proof.RenderGeneration == 0 ||
		action.ComponentId != proof.ComponentId ||
		action.ArtifactId != proof.ArtifactId ||
		action.Generation != proof.RenderGeneration ||
		!bytes.Equal(action.ArtifactDigest, proof.ArtifactSha256) ||
		action.ManagedConfigContent ||
		action.ActionId == "" ||
		len(action.DefinitionDigest) != 32 ||
		len(action.CatalogDigest) != 32 ||
		runtime.OwnerKind != agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_PLATFORM ||
		runtime.OwnerId != "" ||
		ids.Validate(ids.KindConfig, runtime.ArtifactId) != nil ||
		runtime.ProjectName == "" ||
		len(runtime.ProjectName) > 128 ||
		len(runtime.Services) != 1 ||
		runtime.Services[0] == nil {
		return invalid()
	}
	service := runtime.Services[0]
	if service.ServiceId != proof.ServiceId || service.OwnerComponentId != proof.ComponentId ||
		service.ExpectedReplicas != 1 || service.ImageReference != proof.ImageReference ||
		!bytes.Equal(service.ImageConfigDigest, proof.ImageConfigDigest) ||
		service.ComposeName == "" || len(service.ExpectedLabels) == 0 || len(service.ExpectedLabels) > 32 {
		return invalid()
	}
	labels := make(map[string]struct{}, len(service.ExpectedLabels))
	for _, label := range service.ExpectedLabels {
		if label == nil || label.Key == "" || len(label.Key) > 128 || len(label.Value) > 256 {
			return invalid()
		}
		if _, duplicate := labels[label.Key]; duplicate {
			return invalid()
		}
		labels[label.Key] = struct{}{}
	}
	seen := make(map[string]struct{}, len(request.Queries))
	for _, query := range request.Queries {
		if query == nil || !ValidName(query.Domain) || !ValidName(query.Name) || !validType(query.Type) {
			return invalid()
		}
		if _, duplicate := seen[query.Domain]; duplicate {
			return invalid()
		}
		seen[query.Domain] = struct{}{}
	}
	return nil
}

func ValidateCancel(request *agentpb.CancelDNSConnections) error {
	if request == nil || executionplan.RejectUnknown(request) != nil || !validRequestID(request.RequestId) {
		return invalid()
	}
	return nil
}

func ValidateResult(request *agentpb.CheckDNSConnections, result *agentpb.DNSConnectionResult) error {
	if ValidateRequest(request) != nil || result == nil || executionplan.RejectUnknown(result) != nil ||
		proto.Size(result) > MaximumResultBytes || result.RequestId != request.RequestId ||
		result.ComponentId != request.Action.ComponentId || result.ArtifactId != request.Action.ArtifactId ||
		result.RenderGeneration != request.Action.Generation || result.ObservedAt == nil ||
		result.ObservedAt.CheckValid() != nil || result.ObservedAt.AsTime().IsZero() || len(result.Answers) > 160 {
		return invalid()
	}
	if result.Failure != "" {
		if len(result.Answers) != 0 {
			return invalid()
		}
		switch result.Failure {
		case "runtime_unavailable", "configuration_changed", "query_selection_invalid", "execution_failed":
			return nil
		default:
			return invalid()
		}
	}
	if len(result.Answers) == 0 {
		return invalid()
	}
	seen := make(map[string]struct{}, len(result.Answers))
	for _, answer := range result.Answers {
		if !validAnswer(answer, request.PriorObservation.ListenEndpoint) {
			return invalid()
		}
		key := answer.Domain + "\x00" + answer.Name + "\x00" + answer.Server
		if answer.ThroughResolver {
			key += "\x00resolver"
		}
		if _, duplicate := seen[key]; duplicate {
			return invalid()
		}
		seen[key] = struct{}{}
	}
	return nil
}

func validAnswer(answer *agentpb.DNSConnectionAnswer, listener string) bool {
	if answer == nil || !ValidName(answer.Domain) || !ValidName(answer.Name) || !validType(answer.Type) ||
		len(answer.Answers) > MaximumAnswerSample || answer.TotalAnswers > 64 ||
		answer.TotalAnswers < uint32(len(answer.Answers)) || answer.ElapsedMilliseconds > 75000 {
		return false
	}
	host, port, err := net.SplitHostPort(answer.Server)
	address, parseErr := netip.ParseAddr(host)
	if err != nil || parseErr != nil || port != "53" || address.Zone() != "" || address.IsUnspecified() ||
		address.IsMulticast() ||
		answer.ThroughResolver && answer.Server != listener {
		return false
	}
	if answer.Rcode == nil {
		if len(answer.Answers) != 0 || answer.TotalAnswers != 0 || answer.UsedTcp {
			return false
		}
		switch answer.Outcome {
		case "timeout", "canceled", "query_failed":
			return true
		default:
			return false
		}
	}
	if *answer.Rcode > 15 {
		return false
	}
	switch answer.Outcome {
	case "resolved", "no_answer":
		if *answer.Rcode != 0 || answer.Outcome == "resolved" && answer.TotalAnswers == 0 {
			return false
		}
	case "name_not_found":
		if *answer.Rcode != 3 {
			return false
		}
	case "refused":
		if *answer.Rcode != 5 {
			return false
		}
	case "server_error":
		if *answer.Rcode == 0 || *answer.Rcode == 3 || *answer.Rcode == 5 {
			return false
		}
	default:
		return false
	}
	for _, record := range answer.Answers {
		if record == nil || !ValidName(strings.ToLower(record.OwnerName)) || !validType(record.Type) ||
			record.Type == agentpb.DNSQueryType_DNS_QUERY_TYPE_A &&
				(len(record.Ipv4) != 4 || record.NameServer != "") ||
			record.Type == agentpb.DNSQueryType_DNS_QUERY_TYPE_NS &&
				(len(record.Ipv4) != 0 || !ValidName(strings.ToLower(record.NameServer))) {
			return false
		}
	}
	return true
}

func ValidName(value string) bool {
	return value == "." || strings.HasSuffix(value, ".") && dnsname.Valid(strings.TrimSuffix(value, "."))
}

func validType(value agentpb.DNSQueryType) bool {
	return value == agentpb.DNSQueryType_DNS_QUERY_TYPE_A || value == agentpb.DNSQueryType_DNS_QUERY_TYPE_NS
}
func validRequestID(value string) bool { return ids.Validate(ids.KindOperation, "op_"+value) == nil }
func invalid() error {
	return errs.New(errs.KindValidationFailed, "DNS connection check exchange is invalid")
}
