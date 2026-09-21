package servicelifecycle

import (
	"bytes"
	"context"
	"crypto/sha256"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	testenvironmentprojection "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testreleasequeries "github.com/AlanD20/groundplane/internal/infra/etcd/releasequeries"
	testreleaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type acknowledgedRuntimeReader struct {
	*authorityReader
	runtimes []testkeyvalue.Versioned[serviceruntimerecord.Record]
}

func (reader *acknowledgedRuntimeReader) LoadAcknowledgedServiceRuntimesAtRevision(
	_ context.Context,
	_ string,
	_ []string,
	revision int64,
) ([]testkeyvalue.Versioned[serviceruntimerecord.Record], error) {
	reader.revisions = append(reader.revisions, revision)
	return reader.runtimes, nil
}

// Rationale: SVC-15/JOURNEY-02 recovery must select the acknowledged runtime
// changed by Attach/Entry, not the older immutable Release rendering.
func TestCaptureAcknowledgedRuntimePreservesReceiptBytesAndRetainedProxy(t *testing.T) {
	const revision = int64(91)
	environmentID, serviceID := ids.New(ids.KindEnvironment), ids.New(ids.KindService)
	releaseID, retainedReleaseID := ids.New(ids.KindDeployment), ids.New(ids.KindDeployment)
	record := acknowledgedRuntimeFixture(t, environmentID, serviceID, releaseID, retainedReleaseID)
	reader := acknowledgedRuntimeFixtureReader(record, revision)
	reader.serving.Intent.PriorServingReleaseID = retainedReleaseID
	reader.renders[releaseID] = testkeyvalue.Versioned[testreleaserender.ReleaseRenderInput]{
		Record: testreleaserender.ReleaseRenderInput{
			ReleaseID: releaseID, ServiceID: serviceID, EnvironmentID: environmentID,
			Strategy: domain.StrategyBlueGreen, PriorStrategy: domain.StrategyBlueGreen,
			CandidateTarget: domain.WorkloadBlue, PriorTarget: domain.WorkloadGreen,
			PriorArtifactID: ids.New(ids.KindConfig),
		},
		Revision: 83, ReadRevision: revision,
	}
	newArtifactID := ids.New(ids.KindConfig)
	captured, err := CaptureAcknowledgedRuntime(
		context.Background(),
		reader,
		testkeyvalue.Versioned[testenvironmentprojection.EnvironmentComposeProjection]{ReadRevision: revision},
		environmentID,
		serviceID,
		newArtifactID,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer captured.Clear()
	if captured.RuntimeRevision != 89 || captured.RetainedPriorReleaseID != retainedReleaseID {
		t.Fatalf("capture identity = %#v", captured)
	}
	assertReidentifiedRuntimeArtifact(t, record.Runtime.CurrentArtifact, captured.CurrentArtifact, newArtifactID)
	retained := new(agentpb.ComposeArtifact)
	if proto.Unmarshal(captured.RetainedPriorArtifact, retained) != nil {
		t.Fatal("open retained acknowledged runtime")
	}
	assertReidentifiedRuntimeArtifact(t, record.Runtime.RetainedPriorArtifact,
		captured.RetainedPriorArtifact, retained.GetArtifactId())
	current := new(agentpb.ComposeArtifact)
	if proto.Unmarshal(captured.CurrentArtifact, current) != nil || len(current.GetServices()) != 2 ||
		!bytes.Contains(current.GetCanonicalYaml(), []byte("attached-after-release")) ||
		!bytes.Equal(current.GetServices()[1].GetProxyConfigJson(),
			mustOpenRuntimeArtifact(t, record.Runtime.CurrentArtifact).GetServices()[1].GetProxyConfigJson()) {
		t.Fatal("capture did not preserve acknowledged network, file, or proxy bytes")
	}
	for _, used := range reader.revisions {
		if used != revision {
			t.Fatalf("runtime source read at revision %d, want %d", used, revision)
		}
	}
}

func TestCaptureAcknowledgedRuntimeRejectsMissingForeignAndMismatchedReceipts(t *testing.T) {
	const revision = int64(91)
	environmentID, serviceID := ids.New(ids.KindEnvironment), ids.New(ids.KindService)
	releaseID := ids.New(ids.KindDeployment)
	record := acknowledgedRuntimeFixture(t, environmentID, serviceID, releaseID, "")
	for _, test := range []struct {
		name   string
		mutate func(*acknowledgedRuntimeReader)
	}{
		{name: "missing", mutate: func(reader *acknowledgedRuntimeReader) { reader.runtimes = nil }},
		{name: "foreign", mutate: func(reader *acknowledgedRuntimeReader) {
			reader.runtimes[0].Record.EnvironmentID = ids.New(ids.KindEnvironment)
		}},
		{name: "target mismatch", mutate: func(reader *acknowledgedRuntimeReader) {
			reader.renders[releaseID] = testkeyvalue.Versioned[testreleaserender.ReleaseRenderInput]{
				Record: testreleaserender.ReleaseRenderInput{ReleaseID: releaseID, ServiceID: serviceID,
					EnvironmentID: environmentID, CandidateTarget: domain.WorkloadGreen},
				Revision: 83, ReadRevision: revision,
			}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			reader := acknowledgedRuntimeFixtureReader(record, revision)
			test.mutate(reader)
			if _, err := CaptureAcknowledgedRuntime(context.Background(), reader, testkeyvalue.Versioned[testenvironmentprojection.EnvironmentComposeProjection]{ReadRevision: revision}, environmentID, serviceID, ids.New(ids.KindConfig)); err == nil {
				t.Fatal("invalid acknowledged runtime was accepted")
			}
		})
	}
}

func acknowledgedRuntimeFixtureReader(
	record serviceruntimerecord.Record,
	revision int64,
) *acknowledgedRuntimeReader {
	releaseID := record.Runtime.ReleaseID
	return &acknowledgedRuntimeReader{
		authorityReader: &authorityReader{
			serving: testreleasequeries.ServingRelease{Intent: domain.Intent{ID: releaseID},
				ProjectionRevision: 81, IntentRevision: 82, Revision: revision},
			renders: map[string]testkeyvalue.Versioned[testreleaserender.ReleaseRenderInput]{releaseID: {
				Record: testreleaserender.ReleaseRenderInput{ReleaseID: releaseID, ServiceID: record.Runtime.ServiceID,
					EnvironmentID: record.EnvironmentID, CandidateTarget: domain.WorkloadBlue},
				Revision: 83, ReadRevision: revision,
			}},
		},
		runtimes: []testkeyvalue.Versioned[serviceruntimerecord.Record]{{
			Record: record, Revision: 89, ReadRevision: revision,
		}},
	}
}

func acknowledgedRuntimeFixture(
	t *testing.T,
	environmentID string,
	serviceID string,
	releaseID string,
	retainedReleaseID string,
) serviceruntimerecord.Record {
	t.Helper()
	current, proxyConfig := acknowledgedRuntimeArtifact(t, environmentID, serviceID, releaseID, "blue", true)
	runtime := executionplan.CandidateRuntime{
		ServiceID: serviceID, ReleaseID: releaseID, Target: "blue", CurrentArtifact: current,
	}
	if retainedReleaseID != "" {
		runtime.RetainedPriorArtifact, _ = acknowledgedRuntimeArtifact(
			t, environmentID, serviceID, retainedReleaseID, "green", false,
		)
	}
	runtime.ProxyGeneration = 7
	digest := sha256.Sum256(proxyConfig)
	runtime.ProxyConfigSHA256 = slices.Clone(digest[:])
	record := serviceruntimerecord.Record{
		EnvironmentID: environmentID,
		Runtime:       runtime,
		Source: serviceruntimerecord.Acknowledgement{
			TaskID: ids.New(ids.KindTask), PlanID: ids.New(ids.KindPlan), PlanHash: strings.Repeat("a", 64),
			StepID: ids.New(ids.KindStep), AgentID: ids.New(ids.KindAgent),
			AssignmentID: ids.New(ids.KindAssignment), ExecutionEpoch: 1, RenderGeneration: 1,
			EffectDigest: strings.Repeat("b", 64), AcknowledgedAt: time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC),
		},
	}
	if err := serviceruntimerecord.Validate(record); err != nil {
		t.Fatal(err)
	}
	return record
}

func acknowledgedRuntimeArtifact(
	t *testing.T,
	environmentID string,
	serviceID string,
	releaseID string,
	target string,
	proxy bool,
) ([]byte, []byte) {
	t.Helper()
	image := "sha256:" + strings.Repeat(map[string]string{"blue": "a", "green": "b"}[target], 64)
	labels := []*agentpb.LabelPair{
		{Key: "com.groundplane.release-id", Value: releaseID},
		{Key: "com.groundplane.runtime-role", Value: "slot"},
		{Key: "com.groundplane.slot", Value: target},
	}
	workload := &agentpb.ComposeService{
		ServiceId: serviceID, ComposeName: "api--" + target,
		Role: agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT, Slot: target,
		ExpectedReplicas: 1, HasHealthcheck: true, ImageReference: image, ExpectedLabels: labels,
	}
	yaml := []byte("services:\n  api--" + target + ":\n    image: " + image +
		"\n    networks: [attached-after-release]\n    configs: [entry-file]\n")
	artifact := &agentpb.ComposeArtifact{
		ArtifactId: ids.New(ids.KindConfig), OwnerKind: agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT,
		OwnerId: environmentID, ProjectName: "gp-" + strings.ToLower(environmentID),
		AuthorizedVolumeDir: "/var/lib/groundplane/vol/receipt", CanonicalYaml: yaml,
		Services: []*agentpb.ComposeService{workload},
	}
	var proxyConfig []byte
	if proxy {
		proxyConfig = []byte(`{"apps":{"http":{"servers":{"gp_g7_` + strings.ToLower(releaseID) + `_p":{}}}}}`)
		digest := sha256.Sum256(proxyConfig)
		artifact.Services = append(artifact.Services, &agentpb.ComposeService{
			ServiceId: serviceID, ComposeName: "api--proxy",
			Role:             agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY,
			ExpectedReplicas: 1, ImageReference: "sha256:" + strings.Repeat("c", 64),
			ExpectedLabels:  []*agentpb.LabelPair{{Key: "com.groundplane.runtime-role", Value: "proxy"}},
			ProxyConfigJson: proxyConfig, ProxyConfigSha256: digest[:],
		})
	}
	yamlDigest := sha256.Sum256(yaml)
	artifact.YamlSha256 = yamlDigest[:]
	value, err := (proto.MarshalOptions{Deterministic: true}).Marshal(artifact)
	if err != nil {
		t.Fatal(err)
	}
	return value, proxyConfig
}

func assertReidentifiedRuntimeArtifact(t *testing.T, before, after []byte, artifactID string) {
	t.Helper()
	want, got := mustOpenRuntimeArtifact(t, before), mustOpenRuntimeArtifact(t, after)
	want.ArtifactId = artifactID
	if !proto.Equal(want, got) {
		t.Fatal("acknowledged runtime changed beyond artifact identity")
	}
}

func mustOpenRuntimeArtifact(t *testing.T, value []byte) *agentpb.ComposeArtifact {
	t.Helper()
	artifact := new(agentpb.ComposeArtifact)
	if proto.Unmarshal(value, artifact) != nil {
		t.Fatal("open acknowledged runtime artifact")
	}
	return artifact
}
