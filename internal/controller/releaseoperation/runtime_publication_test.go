package releaseoperation

import (
	"context"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// TASK-100: native admission retains exact candidate bytes separately from
// applied runtime. Capturing a first blue-green candidate must not acknowledge
// a serving Release before any host execution or traffic switch has occurred.
func provePreparedReleaseRuntime(
	t *testing.T,
	store *directPublicationStore,
	capture releaserender.ServiceRolloutCapture,
	serviceID string,
	detachedNetwork bool,
) {
	t.Helper()
	artifact, err := releaserender.OpenReleaseCandidateArtifact(capture)
	if err != nil {
		t.Fatal(err)
	}
	if len(artifact.GetServices()) != 3 || capture.Render.PriorRuntime != nil {
		t.Fatal("first Release runtime includes an unselected or fabricated predecessor workload")
	}
	workloads, proxies := 0, 0
	for _, service := range artifact.GetServices() {
		if service.GetServiceId() != serviceID {
			t.Fatal("explicit first Deploy captured a different Service")
		}
		if service.GetRole() == agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY {
			proxies++
		} else if service.GetRole() == agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT &&
			service.GetExpectedReplicas() == 1 && service.GetImageReference() == capture.Render.CandidateWorkload.LocalImageID &&
			service.GetSlot() == string(capture.Render.CandidateTarget) {
			workloads++
		}
	}
	if workloads != 1 || proxies != 1 {
		t.Fatal("first Deploy did not select one workload and its stable proxy")
	}
	// Permitting an unused slot must not permit a second Release. Recapture
	// changed metadata so a stale byte hash cannot be the rejection reason.
	for _, releaseID := range []string{capture.Render.ReleaseID, ids.New(ids.KindDeployment)} {
		changed := proto.CloneOf(artifact)
		for _, service := range changed.Services {
			if service.Role == agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT &&
				service.Slot != string(capture.Render.CandidateTarget) {
				service.ExpectedLabels = append(service.ExpectedLabels, &agentpb.LabelPair{
					Key: "com.groundplane.release-id", Value: releaseID})
			}
		}
		altered := capture
		altered.CandidateArtifact, err = proto.MarshalOptions{Deterministic: true}.Marshal(changed)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := releaserender.OpenReleaseCandidateArtifact(altered); err == nil {
			t.Fatal("a slot claiming Release ownership was accepted as an unused template")
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
