package controller

import (
	"context"
	"crypto/sha256"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type managedTeardownPlanReader struct {
	*blueprintPlanReader
	source etcd.EnvironmentComposeProjection
}

func (reader *managedTeardownPlanReader) GetEnvironmentComposeProjectionRevision(
	context.Context,
	string,
	string,
) (etcd.Versioned[etcd.EnvironmentComposeProjection], bool, error) {
	return etcd.Versioned[etcd.EnvironmentComposeProjection]{Record: reader.source}, true, nil
}

// Rationale: disabling a previously attempted managed Component must remove
// only its exact historical generated Service, even though the new candidate
// artifact no longer contains that Service.
func TestBlueprintManagedComponentTeardownBuildsTargetedHistoricalRemoval(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 9, 6, 8, 40, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, now, 1)
	revisionID := ids.NewAt(ids.KindTask, now, 2)
	sourceRevisionID := ids.NewAt(ids.KindTask, now, 3)
	componentID := ids.NewAt(ids.KindComponent, now, 4)
	serviceID := ids.NewAt(ids.KindService, now, 5)
	futureServiceID := ids.NewAt(ids.KindService, now, 9)
	sourceArtifactID := ids.NewAt(ids.KindConfig, now, 6)
	candidateArtifactID := ids.NewAt(ids.KindConfig, now, 7)
	sourceArtifact := &agentpb.ComposeArtifact{
		ArtifactId: sourceArtifactID, OwnerId: environmentID,
		Services: []*agentpb.ComposeService{{
			ServiceId: serviceID, ComposeName: "cloudflare-tunnel", OwnerComponentId: componentID,
		}},
	}
	sourceBytes, err := (proto.MarshalOptions{Deterministic: true}).Marshal(sourceArtifact)
	if err != nil {
		t.Fatal(err)
	}
	sourceDigest := sha256.Sum256(sourceBytes)
	sourceProjection := etcd.EnvironmentComposeProjection{
		EnvironmentID: environmentID, RevisionID: sourceRevisionID, ComposeArtifact: sourceBytes,
	}
	projection := etcd.EnvironmentComposeProjection{
		EnvironmentID: environmentID, RevisionID: revisionID,
		ManagedComponentRuntimeSources: []etcd.ManagedComponentRuntimeSource{{
			ComponentKind: "cloudflare-tunnel", ComponentID: componentID, ServiceID: futureServiceID,
			ComposeName: "cloudflare-tunnel-new", RevisionID: revisionID, ArtifactID: candidateArtifactID,
			ArtifactSHA256: stringHex(make([]byte, sha256.Size)),
		}},
	}
	priorSource := etcd.ManagedComponentRuntimeSource{
		ComponentKind: "cloudflare-tunnel", ComponentID: componentID,
		ServiceID: serviceID, ComposeName: "cloudflare-tunnel",
		RevisionID: sourceRevisionID, ArtifactID: sourceArtifactID,
		ArtifactSHA256: stringHex(sourceDigest[:]),
	}
	reader := &managedTeardownPlanReader{
		blueprintPlanReader: &blueprintPlanReader{}, source: sourceProjection,
	}
	resolver, err := NewTaskPlanResolverWithBlueprints("/var/lib/groundplane/vol", reader, nil)
	if err != nil {
		t.Fatal(err)
	}
	task := etcd.TaskRecord{
		ID: revisionID, Target: environmentID, PlanID: ids.NewAt(ids.KindPlan, now, 8), TimeoutSeconds: 120,
		Params:                          map[string]string{etcd.EnvironmentDesiredRevisionParam: revisionID},
		ManagedComponentTeardownSources: []etcd.ManagedComponentRuntimeSource{priorSource},
	}
	candidate := &agentpb.ComposeArtifact{
		ArtifactId: candidateArtifactID,
		OwnerId:    environmentID,
		Services: []*agentpb.ComposeService{{
			ServiceId: futureServiceID, ComposeName: "cloudflare-tunnel-new", OwnerComponentId: componentID,
		}},
	}

	result, err := resolver.BlueprintManagedComponentTeardown(
		context.Background(), task, projection, candidate, "", false,
	)
	if err != nil {
		t.Fatalf("BlueprintManagedComponentTeardown() error = %v", err)
	}
	if len(result.Artifacts) != 2 || len(result.Steps) != 1 || result.Procedure == nil ||
		len(result.Procedure.Services) != 1 {
		t.Fatalf("teardown = %#v", result)
	}
	remove := result.Steps[0].GetComposeRemove()
	if remove == nil || remove.GetArtifactId() != sourceArtifactID || remove.GetWholeProject() ||
		len(remove.GetServiceIds()) != 1 || remove.GetServiceIds()[0] != serviceID {
		t.Fatalf("targeted removal = %#v", remove)
	}
}

func stringHex(value []byte) string {
	const digits = "0123456789abcdef"
	result := make([]byte, len(value)*2)
	for index, item := range value {
		result[index*2] = digits[item>>4]
		result[index*2+1] = digits[item&0x0f]
	}
	return string(result)
}
