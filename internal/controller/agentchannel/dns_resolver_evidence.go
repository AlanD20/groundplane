package agentchannel

import (
	"encoding/hex"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"net/netip"

	"github.com/AlanD20/groundplane/internal/common/dnsproof"

	"github.com/AlanD20/groundplane/proto/agentpb"
)

func durableDNSResolverObservation(
	evidence *agentpb.DNSResolverObservationEvidence,
) *taskjournal.TaskDNSResolverObservationEvidence {
	canonicalEvidence, _ := dnsproof.Marshal(evidence)
	static := evidence.GetStaticQuery()
	staticIPv4 := ""
	for _, answer := range static.GetAnswers() {
		if len(answer.GetIpv4()) == 4 {
			staticIPv4 = netip.AddrFrom4([4]byte(answer.GetIpv4())).String()
			break
		}
	}
	return &taskjournal.TaskDNSResolverObservationEvidence{
		ComponentID: evidence.GetComponentId(), ServiceID: evidence.GetServiceId(),
		ArtifactID: evidence.GetArtifactId(), ArtifactSHA256: hex.EncodeToString(evidence.GetArtifactSha256()),
		RenderGeneration: evidence.GetRenderGeneration(), ImageReference: evidence.GetImageReference(),
		VerifiedImageDigest: hex.EncodeToString(evidence.GetVerifiedImageDigest()),
		ImageConfigDigest:   hex.EncodeToString(evidence.GetImageConfigDigest()),
		ListenEndpoint:      evidence.GetListenEndpoint(), ReloadSHA512: hex.EncodeToString(evidence.GetReloadSha512()),
		ObservedAt: evidence.GetObservedAt().AsTime().UTC(), StaticQueryPresent: static != nil,
		StaticQueryName: static.GetName(), StaticQueryIPv4: staticIPv4, StaticQuerySucceeded: static != nil,
		RecursiveQuerySucceeded: evidence.GetCatchAllQuery() != nil,
		ForwarderQueryCount:     uint32(len(evidence.GetForwarderQueries())),
		ForwarderSuccessCount:   uint32(len(evidence.GetForwarderQueries())),
		ProofSHA256:             hex.EncodeToString(evidence.GetProofSha256()), CanonicalEvidence: canonicalEvidence,
	}
}
