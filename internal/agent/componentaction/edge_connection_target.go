package componentaction

import (
	"bytes"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/edgeconnectioncheck"
	edgecheck "github.com/AlanD20/groundplane/internal/infra/docker/edgeconnectioncheck"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// PrepareEdgeConnectionTarget authenticates the selected readiness endpoint
// through the same compiled catalog as ordinary Component planning. A caller
// cannot supply a command, a different image or a remote readiness endpoint.
func (runtime *Runtime) PrepareEdgeConnectionTarget(
	action *agentpb.ComponentApply,
	artifact *agentpb.ComposeArtifact,
	request edgeconnectioncheck.Request,
) (edgecheck.Target, error) {
	if runtime == nil || runtime.catalog == nil || artifact == nil || action.GetComponentId() != request.ComponentID ||
		action.GetArtifactId() != artifact.ArtifactId || !bytes.Equal(action.GetArtifactDigest(), artifact.YamlSha256) ||
		hex.EncodeToString(action.GetArtifactDigest()) != request.RuntimeSHA256 || action.GetManagedConfigContent() ||
		len(
			action.GetExpectedPreviousArtifactDigest(),
		) != 0 || action.GetExpectedPreviousArtifactId() != "" || action.GetExpectedPreviousGeneration() != 0 {
		return edgecheck.Target{}, errs.New(
			errs.KindValidationFailed,
			"Tunnel check action differs from its acknowledged runtime",
		)
	}
	envelope, err := DecodeComponentAction(action)
	if err != nil {
		return edgecheck.Target{}, err
	}
	_, _, recipe, err := runtime.catalog.ResolveEdgeTunnelObservationActionEnvelope(envelope)
	if err != nil {
		return edgecheck.Target{}, errs.New(
			errs.KindValidationFailed,
			"Tunnel check action is not part of the compiled edge catalog",
		)
	}
	return edgecheck.NewTarget(request, artifact, recipe, action.Generation)
}
