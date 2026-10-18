package etcd

import (
	"bytes"
	"context"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/dnsconnectioncheck"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/components"
	"github.com/AlanD20/groundplane/internal/infra/etcd/dnschecks"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hostbindings"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/localagents"
	"github.com/AlanD20/groundplane/internal/infra/etcd/resolverwork"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// DNSCheckSource can only be captured by the repository. Its private comparisons
// bind publication to one MVCC view; public freshness metadata is not authority.
type DNSCheckSource struct {
	configuration dnschecks.Source
	conditions    []keyvalue.Condition
	runtime       *agentpb.ComposeArtifact
	proof         *agentpb.DNSResolverObservationEvidence
	readRevision  int64
	requestID     string
}

func (source DNSCheckSource) Configuration() dnschecks.Source { return source.configuration }

// Request supplies only the compiled observation action and operator query names.
// Runtime identity, artifact digest and generation come from the captured proof.
func (source DNSCheckSource) Request(action *agentpb.ComponentApply,
	queries []*agentpb.DNSConnectionQuery,
) (*agentpb.CheckDNSConnections, error) {
	if source.configuration.Validate() != nil || source.readRevision <= 0 ||
		source.runtime == nil || source.proof == nil || action == nil {
		return nil, errs.New(errs.KindInternal, "DNS check source has not been captured")
	}
	request := &agentpb.CheckDNSConnections{RequestId: source.requestID, Action: proto.CloneOf(action),
		Runtime: proto.CloneOf(source.runtime), PriorObservation: proto.CloneOf(source.proof)}
	request.Action.ComponentId = source.proof.ComponentId
	request.Action.ArtifactId = source.proof.ArtifactId
	request.Action.ArtifactDigest = bytes.Clone(source.proof.ArtifactSha256)
	request.Action.Generation = source.proof.RenderGeneration
	for _, query := range queries {
		request.Queries = append(request.Queries, proto.CloneOf(query))
	}
	if err := dnsconnectioncheck.ValidateRequest(request); err != nil {
		return nil, err
	}
	return request, nil
}

func (repository *ComponentRepository) LoadDNSCheckSource(ctx context.Context, componentID string, revision int64) (DNSCheckSource, error) {
	if ids.Validate(ids.KindComponent, componentID) != nil || revision <= 0 {
		return DNSCheckSource{}, errs.New(errs.KindValidationFailed, "DNS check requires a Component and its current revision")
	}
	host, err := hostbindings.ReadLocal(ctx, repository.store, 0)
	if err != nil {
		return DNSCheckSource{}, err
	}
	state, err := resolverwork.ReadState(ctx, repository.store, host.Binding.HostID, componentID, host.ReadRevision)
	if err != nil {
		return DNSCheckSource{}, err
	}
	keys := []string{components.RecordKey(componentID), localagents.LocalAgentSingletonKey, dnschecks.LatestKey(componentID), dnschecks.ActiveKey(componentID)}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: state.ReadRevision})
	if err != nil {
		return DNSCheckSource{}, err
	}
	if read == nil || read.ReadRevision != state.ReadRevision || len(read.Values) != len(keys) {
		return DNSCheckSource{}, errs.New(errs.KindInternal, "DNS check configuration capture is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil {
		return DNSCheckSource{}, errs.New(errs.KindRequestNotFound, "CoreDNS Component was not found")
	}
	if read.Values[0].ModRevision != revision {
		return DNSCheckSource{}, errs.New(errs.KindStateConflict, "CoreDNS changed; refresh its settings before testing DNS")
	}
	component, err := components.DecodeRecord(read.Values[0].Value)
	if err != nil {
		return DNSCheckSource{}, err
	}
	if component.Desired.ID != componentID || component.Desired.Kind != core.ComponentKindCoreDNS || component.Desired.Owner != core.ComponentOwnerPlatform {
		return DNSCheckSource{}, errs.New(errs.KindValidationFailed, "DNS checks require the platform CoreDNS Component")
	}
	if !component.Desired.Enabled || state.Applied == nil || !state.Applied.Desired.Enabled || read.Values[1] == nil {
		return DNSCheckSource{}, errs.New(errs.KindStateConflict, "enable CoreDNS and wait for its configuration to be applied before testing DNS")
	}
	if state.Pending != nil || state.ActiveItem != nil || read.Values[3] != nil {
		return DNSCheckSource{}, errs.New(errs.KindResourceInUse, "CoreDNS has unfinished configuration or check work; finish it before testing DNS")
	}
	applied := state.Applied
	if len(component.Runtime.GeneratedServices) != 1 || component.Runtime.GeneratedServices[0] != applied.Configuration.Runtime.Services[0].ServiceId {
		return DNSCheckSource{}, errs.New(errs.KindStateConflict, "CoreDNS runtime differs from its applied configuration")
	}
	agentID, err := localagents.DecodeLocalAgentReference(read.Values[1].Value)
	if err != nil {
		return DNSCheckSource{}, err
	}
	if agentID != host.Binding.AgentID {
		return DNSCheckSource{}, errs.New(errs.KindStateConflict, "CoreDNS host has a different enrolled Agent")
	}
	agentKey := localagents.LocalAgentPrimaryKey(agentID)
	agentRead, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{agentKey}, Revision: state.ReadRevision})
	if err != nil {
		return DNSCheckSource{}, err
	}
	if agentRead == nil || agentRead.ReadRevision != state.ReadRevision || len(agentRead.Values) != 1 || agentRead.Values[0] == nil {
		return DNSCheckSource{}, errs.New(errs.KindStateConflict, "installed Agent is unavailable; wait for it before testing DNS")
	}
	defer keyvalue.ClearValues(agentRead.Values)
	agent, err := localagents.DecodeLocalAgentPrimary(agentRead.Values[0].Value)
	if err != nil {
		return DNSCheckSource{}, err
	}
	if agent.ID != agentID || agent.Phase != localagents.LocalAgentPhaseReady {
		return DNSCheckSource{}, errs.New(errs.KindStateConflict, "wait for Agent installation or update to finish before testing DNS")
	}
	proof := applied.ServingProof
	configuration := dnschecks.Source{ComponentID: componentID, ComponentRevision: revision, HostID: state.HostID,
		AppliedRevision: state.AppliedRevision, IntentRevision: state.IntentRevision, SingletonRevision: read.Values[1].ModRevision,
		AgentID: agentID, AgentRevision: agentRead.Values[0].ModRevision, AgentGeneration: agent.Generation,
		ServiceID: proof.ServiceId, ArtifactID: proof.ArtifactId, ArtifactSHA256: hex.EncodeToString(proof.ArtifactSha256),
		ImageReference: proof.ImageReference, ImageConfigDigest: hex.EncodeToString(proof.ImageConfigDigest), RenderGeneration: proof.RenderGeneration}
	if err := configuration.Validate(); err != nil {
		return DNSCheckSource{}, err
	}
	source := DNSCheckSource{configuration: configuration, runtime: proto.CloneOf(applied.Configuration.Runtime),
		proof: proto.CloneOf(proof), readRevision: state.ReadRevision, requestID: ids.NewULID(),
		conditions: append(state.Conditions, host.Conditions...)}
	source.conditions = append(source.conditions, keyvalue.Condition{Key: agentKey, ModRevision: configuration.AgentRevision})
	for i, key := range keys {
		source.conditions = append(source.conditions, keyvalue.Condition{Key: key, ModRevision: keyvalue.RevisionOf(read.Values[i])})
	}
	return source, nil
}

// Retained results compare resource revisions only. Display never reruns a DNS
// query, resolves a newer image or depends on the applying Task's retention.
func (repository *ComponentRepository) DNSCheckSourcesChanged(ctx context.Context, source dnschecks.Source) (bool, error) {
	if err := source.Validate(); err != nil {
		return false, err
	}
	keys := []string{components.RecordKey(source.ComponentID), resolverwork.AppliedKey(source.HostID, source.ComponentID),
		localagents.LocalAgentSingletonKey, localagents.LocalAgentPrimaryKey(source.AgentID), resolverwork.IntentKey(source.HostID, source.ComponentID)}
	revisions := []int64{source.ComponentRevision, source.AppliedRevision, source.SingletonRevision, source.AgentRevision, source.IntentRevision}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys})
	if err != nil {
		return false, err
	}
	if read == nil || read.ReadRevision <= 0 || len(read.Values) != len(keys) {
		return false, errs.New(errs.KindInternal, "DNS check freshness snapshot is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	for i, revision := range revisions {
		if keyvalue.RevisionOf(read.Values[i]) != revision {
			return true, nil
		}
	}
	return false, nil
}
