package etcd

import (
	"bytes"
	"context"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/dnsconnectioncheck"
	"github.com/AlanD20/groundplane/internal/common/dnsproof"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/components"
	"github.com/AlanD20/groundplane/internal/infra/etcd/dnschecks"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/localagents"
	"github.com/AlanD20/groundplane/internal/infra/etcd/platformcomponents"
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

func (repository *ComponentRepository) LoadDNSCheckSource(ctx context.Context,
	componentID string, revision int64,
) (DNSCheckSource, error) {
	if ids.Validate(ids.KindComponent, componentID) != nil || revision <= 0 {
		return DNSCheckSource{}, errs.New(
			errs.KindValidationFailed,
			"DNS check requires a Component and its current revision",
		)
	}
	keys := []string{components.RecordKey(componentID), platformcomponents.ComponentObservationKey(componentID),
		platformcomponents.ActiveTaskKey(
			componentID,
		), localagents.LocalAgentSingletonKey, dnschecks.LatestKey(componentID),
		dnschecks.ActiveKey(componentID)}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys})
	if err != nil {
		return DNSCheckSource{}, err
	}
	if read == nil || read.ReadRevision <= 0 || len(read.Values) != len(keys) {
		if read != nil {
			keyvalue.ClearValues(read.Values)
		}
		return DNSCheckSource{}, errs.New(errs.KindInternal, "DNS check configuration capture is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil {
		return DNSCheckSource{}, errs.New(errs.KindRequestNotFound, "CoreDNS Component was not found")
	}
	if read.Values[0].ModRevision != revision {
		return DNSCheckSource{}, errs.New(
			errs.KindStateConflict,
			"CoreDNS changed; refresh its settings before testing DNS",
		)
	}
	component, err := components.DecodeRecord(read.Values[0].Value)
	if err != nil {
		return DNSCheckSource{}, err
	}
	if platformcomponents.ValidatePlatformComponentRecord(component) != nil ||
		component.Desired.ID != componentID || component.Desired.Kind != core.ComponentKindCoreDNS {
		return DNSCheckSource{}, errs.New(
			errs.KindValidationFailed,
			"DNS checks require the platform CoreDNS Component",
		)
	}
	if !component.Desired.Enabled || read.Values[1] == nil || read.Values[3] == nil {
		return DNSCheckSource{}, errs.New(
			errs.KindStateConflict,
			"enable CoreDNS and wait for its configuration to be applied before testing DNS",
		)
	}
	if read.Values[2] != nil || read.Values[5] != nil {
		return DNSCheckSource{}, errs.New(
			errs.KindResourceInUse,
			"CoreDNS configuration is changing; wait for its Task before testing DNS",
		)
	}
	observation, err := platformcomponents.DecodeComponentObservation(read.Values[1].Value)
	if err != nil {
		return DNSCheckSource{}, err
	}
	if !observation.Enabled || observation.ComponentID != componentID || observation.DNSResolverProof == nil ||
		len(
			component.Runtime.GeneratedServices,
		) != 1 || component.Runtime.GeneratedServices[0] != observation.ServiceID {
		return DNSCheckSource{}, errs.New(
			errs.KindStateConflict,
			"CoreDNS has no acknowledged serving configuration; apply its settings before testing DNS",
		)
	}
	proof, err := dnsproof.Unmarshal(observation.DNSResolverProof.CanonicalEvidence)
	if err != nil {
		return DNSCheckSource{}, errs.New(errs.KindInternal, "CoreDNS acknowledged query evidence is corrupt")
	}
	if proof.ComponentId != componentID || proof.ServiceId != observation.ServiceID ||
		proof.ArtifactId != observation.DNSResolverProof.ArtifactID ||
		proof.RenderGeneration != observation.RenderGeneration ||
		hex.EncodeToString(proof.ArtifactSha256) != observation.CorefileSHA256 ||
		hex.EncodeToString(proof.ImageConfigDigest) != observation.DNSResolverProof.ImageConfigDigest {
		return DNSCheckSource{}, errs.New(
			errs.KindInternal,
			"CoreDNS acknowledged query evidence does not match its runtime",
		)
	}
	agentID, err := localagents.DecodeLocalAgentReference(read.Values[3].Value)
	if err != nil {
		return DNSCheckSource{}, err
	}
	if agentID != observation.AgentID {
		return DNSCheckSource{}, errs.New(
			errs.KindStateConflict,
			"CoreDNS runtime belongs to another Agent; apply its settings on the installed Agent",
		)
	}
	agentKey := localagents.LocalAgentPrimaryKey(agentID)
	agentRead, err := repository.store.GetMany(
		ctx,
		keyvalue.GetManyRequest{Keys: []string{agentKey}, Revision: read.ReadRevision},
	)
	if err != nil {
		return DNSCheckSource{}, err
	}
	if agentRead == nil || agentRead.ReadRevision != read.ReadRevision || len(agentRead.Values) != 1 ||
		agentRead.Values[0] == nil {
		if agentRead != nil {
			keyvalue.ClearValues(agentRead.Values)
		}
		return DNSCheckSource{}, errs.New(
			errs.KindStateConflict,
			"installed Agent is unavailable; wait for it before testing DNS",
		)
	}
	defer keyvalue.ClearValues(agentRead.Values)
	agent, err := localagents.DecodeLocalAgentPrimary(agentRead.Values[0].Value)
	if err != nil {
		return DNSCheckSource{}, err
	}
	if agent.ID != agentID || agent.Phase != localagents.LocalAgentPhaseReady {
		return DNSCheckSource{}, errs.New(
			errs.KindStateConflict,
			"wait for Agent installation or update to finish before testing DNS",
		)
	}
	configuration := dnschecks.Source{ComponentID: componentID, ComponentRevision: revision,
		ObservationRevision: read.Values[1].ModRevision, SingletonRevision: read.Values[3].ModRevision,
		AgentID: agentID, AgentRevision: agentRead.Values[0].ModRevision, AgentGeneration: agent.Generation,
		AppliedTaskID: observation.TaskID, ServiceID: observation.ServiceID, ArtifactID: proof.ArtifactId,
		ArtifactSHA256: observation.CorefileSHA256, ImageReference: proof.ImageReference,
		ImageConfigDigest: observation.DNSResolverProof.ImageConfigDigest, RenderGeneration: observation.RenderGeneration}
	if err := configuration.Validate(); err != nil {
		return DNSCheckSource{}, err
	}
	source := DNSCheckSource{configuration: configuration, runtime: proto.CloneOf(observation.ComposeArtifact),
		proof: proof, readRevision: read.ReadRevision, requestID: ids.NewULID(),
		conditions: []keyvalue.Condition{{Key: agentKey, ModRevision: configuration.AgentRevision}}}
	for index, key := range keys {
		source.conditions = append(
			source.conditions,
			keyvalue.Condition{Key: key, ModRevision: keyvalue.RevisionOf(read.Values[index])},
		)
	}
	return source, nil
}

// DNSCheckSourcesChanged reads revisions only. Displaying a retained result does
// not rerun a query or require the old reporting Task to remain in the journal.
func (repository *ComponentRepository) DNSCheckSourcesChanged(
	ctx context.Context,
	source dnschecks.Source,
) (bool, error) {
	if err := source.Validate(); err != nil {
		return false, err
	}
	keys := []string{
		components.RecordKey(source.ComponentID),
		platformcomponents.ComponentObservationKey(source.ComponentID),
		localagents.LocalAgentSingletonKey,
		localagents.LocalAgentPrimaryKey(source.AgentID),
		platformcomponents.ActiveTaskKey(source.ComponentID),
	}
	revisions := []int64{
		source.ComponentRevision,
		source.ObservationRevision,
		source.SingletonRevision,
		source.AgentRevision,
		0,
	}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys})
	if err != nil {
		return false, err
	}
	if read == nil || read.ReadRevision <= 0 || len(read.Values) != len(keys) {
		if read != nil {
			keyvalue.ClearValues(read.Values)
		}
		return false, errs.New(errs.KindInternal, "DNS check freshness evidence is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	for index, revision := range revisions {
		if keyvalue.RevisionOf(read.Values[index]) != revision {
			return true, nil
		}
	}
	return false, nil
}
