package releaseoperation

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	testreleases "github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// Rationale: the actual publisher must retain post-activation inputs separately
// from its prepared Compose artifact without promoting them before execution.
func provePreparedReleaseRuntime(
	t *testing.T,
	store *directPublicationStore,
	marker testreleases.ReleasePublicationMarker,
	plan *agentpb.ExecutionPlan,
	serviceID string,
	detachedNetwork bool,
) {
	t.Helper()
	if len(marker.PreparedRuntimes) != 1 || marker.PreparedRuntimes[0].ServiceID != serviceID {
		t.Fatal("publication omitted prepared member runtime")
	}
	runtime := marker.PreparedRuntimes[0]
	artifact := &agentpb.ComposeArtifact{}
	if err := proto.Unmarshal(runtime.CurrentArtifact, artifact); err != nil {
		t.Fatal(err)
	}
	if err := executionplan.ValidateNativePredecessorWitness(artifact.GetOwnerId(), serviceID,
		runtime.CurrentArtifact, runtime.RetainedPriorArtifact); err != nil {
		t.Fatal(err)
	}
	if len(artifact.GetServices()) != 2 || runtime.RetainedPriorArtifact != nil {
		t.Fatal("first Release runtime includes an unselected or fabricated predecessor workload")
	}
	var switched *agentpb.ServiceProxySwitch
	for _, step := range plan.GetSteps() {
		if step.GetServiceProxySwitch().GetServiceId() == serviceID {
			switched = step.GetServiceProxySwitch()
		}
	}
	if switched == nil || runtime.Target != switched.GetToTarget() || runtime.ReleaseID != switched.GetReleaseId() ||
		runtime.ProxyGeneration != switched.GetProxyGeneration() || !bytes.Equal(runtime.ProxyConfigSHA256, switched.GetConfigSha256()) {
		t.Fatal("prepared runtime does not retain exact sealed activation identity")
	}
	for _, service := range artifact.GetServices() {
		if service.GetRole() == agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY &&
			(!bytes.Equal(service.GetProxyConfigJson(), switched.GetConfigJson()) ||
				!bytes.Equal(service.GetProxyConfigSha256(), switched.GetConfigSha256())) {
			t.Fatal("prepared runtime retained pre-switch proxy configuration")
		}
	}
	if detachedNetwork &&
		(strings.Contains(string(artifact.GetCanonicalYaml()), "gp_attach_net_01arz3ndektsv4rrffq69g5fav") ||
			!strings.Contains(string(artifact.GetCanonicalYaml()), publicationCurrentAttachNetwork)) {
		t.Fatal("prepared runtime failed to preserve current Attach memberships")
	}
	current, err := store.Get(context.Background(), serviceruntimerecord.Key(serviceID))
	if err != nil || current.Entry != nil {
		t.Fatal("publication promoted prepared runtime before successful execution")
	}
}
