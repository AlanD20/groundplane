package app

import (
	"encoding/hex"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testreleaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	testreleases "github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// SeedEntryAcknowledgedRuntime models the receipt written by the successful
// native operation represented by the fixture's already-sealed artifacts.
func (fixture *ExecutedArtifactFixture) SeedEntryAcknowledgedRuntime(
	t *testing.T, render testreleaserender.ReleaseRenderInput, artifacts []*agentpb.ComposeArtifact,
) testkeyvalue.Versioned[serviceruntimerecord.Record] {
	t.Helper()
	if len(artifacts) < 1 || len(artifacts) > 2 {
		t.Fatal("Entry receipt fixture requires one current and at most one retained artifact")
	}
	current, err := (proto.MarshalOptions{Deterministic: true}).Marshal(artifacts[0])
	if err != nil {
		t.Fatal(err)
	}
	runtime := executionplan.CandidateRuntime{ServiceID: render.ServiceID, ReleaseID: render.ReleaseID,
		Target: string(render.CandidateTarget), CurrentArtifact: current}
	if len(artifacts) == 2 {
		runtime.RetainedPriorArtifact, err = (proto.MarshalOptions{Deterministic: true}).Marshal(artifacts[1])
		if err != nil {
			t.Fatal(err)
		}
	}
	if len(render.ProxyPorts) != 0 {
		runtime.ProxyGeneration = render.ProxyGeneration
		runtime.ProxyConfigSHA256, err = hex.DecodeString(render.ProxyConfigDigest)
		if err != nil {
			t.Fatal(err)
		}
	}
	record := serviceruntimerecord.Record{EnvironmentID: render.EnvironmentID, Runtime: runtime,
		Source: serviceruntimerecord.Acknowledgement{
			TaskID: ids.New(ids.KindTask), PlanID: render.PlanID, PlanHash: strings.Repeat("a", 64),
			StepID: ids.New(ids.KindStep), AgentID: ids.New(ids.KindAgent), AssignmentID: ids.New(ids.KindAssignment),
			ExecutionEpoch: 1, RenderGeneration: render.Projection.RenderGeneration,
			EffectDigest: strings.Repeat("b", 64), AcknowledgedAt: time.Unix(1_700_000_000, 0).UTC(),
		}}
	if err := serviceruntimerecord.Validate(record); err != nil {
		t.Fatal(err)
	}
	value, err := testreleases.EncodeReleaseRecord("service-acknowledged-runtime", record)
	if err != nil {
		t.Fatal(err)
	}
	revision, err := fixture.store.Put(t.Context(), serviceruntimerecord.Key(render.ServiceID), value)
	if err != nil {
		t.Fatal(err)
	}
	return testkeyvalue.Versioned[serviceruntimerecord.Record]{
		Record:       record,
		Revision:     revision,
		ReadRevision: revision,
	}
}

// AdvanceAcknowledgedRuntime rewrites byte-identical authority at a new
// revision so publication, claim, and terminal fences can be exercised.
func (fixture *ExecutedArtifactFixture) AdvanceAcknowledgedRuntime(t *testing.T, serviceID string) {
	t.Helper()
	value, err := fixture.store.Get(t.Context(), serviceruntimerecord.Key(serviceID))
	if err != nil || value.Entry == nil {
		t.Fatal("advance acknowledged runtime", err)
	}
	if _, err := fixture.store.Put(t.Context(), value.Entry.Key, value.Entry.Value); err != nil {
		t.Fatal(err)
	}
}
