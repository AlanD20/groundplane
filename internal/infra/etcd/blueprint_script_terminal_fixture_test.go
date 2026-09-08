package etcd

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	domain "github.com/AlanD20/groundplane/internal/core/release"
)

func seedBlueprintTerminalCandidateLedger(t *testing.T, published environmentBlueprintAtomicPublication) {
	t.Helper()
	ctx := context.Background()
	task := published.task
	publicationID := published.releasePublicationID
	read, err := published.store.GetMany(ctx, GetManyRequest{Keys: []string{
		releasePublicationKey(publicationID), releaseManifestStagingKey(publicationID),
		environmentMutationEpochKey(published.environmentID),
	}})
	if err != nil || read.Values[0] == nil || read.Values[1] == nil || read.Values[2] == nil {
		t.Fatalf("read Blueprint Release authority = %#v, %v", read, err)
	}
	marker, err := decodeReleaseRecord[ReleasePublicationMarker](read.Values[0].Value, "release-publication")
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := decodeReleaseRecord[ReleaseStagedManifest](read.Values[1].Value, "release-staged-manifest")
	if err != nil || len(manifest.Members) != 1 {
		t.Fatalf("decode staged manifest = %#v, %v", manifest, err)
	}
	member := manifest.Members[0]
	projection := withTestEnvironmentComposeArtifact(EnvironmentComposeProjection{
		EnvironmentID: published.environmentID, RevisionID: task.ID, RenderGeneration: uint64(task.RenderGeneration),
		DesiredServices: []EnvironmentServiceProjection{{
			EnvironmentID: published.environmentID,
			Desired: core.Service{
				ID:       member.ServiceID,
				Name:     "api",
				Image:    "example/api:1",
				Strategy: core.StrategyRecreate,
			},
		}},
		ServiceDependencyPlans: core.ServiceDependencyPlans{},
	})
	render := ReleaseRenderInput{
		ReleaseID: member.ReleaseID, PlanID: task.PlanID, ArtifactID: task.Params[TaskComposeArtifactParam],
		ServiceID: member.ServiceID, ServiceName: "api", CandidateWorkload: releaseTestWorkloadSeal("example/api:1"),
		Strategy: domain.StrategyRecreate, CandidateTarget: domain.WorkloadSingleton,
		PriorArtifactID: ids.NewAt(
			ids.KindConfig,
			task.CreatedAt,
			19991,
		), PriorWorkload: releaseTestPriorWorkload("example/api:previous"),
		PriorStrategy: domain.StrategyRecreate, PriorTarget: domain.WorkloadSingleton,
		TenantID: task.Owner.TenantID, TenantSlug: "tenant", ProjectID: task.Owner.ProjectID, ProjectSlug: "project",
		EnvironmentID: published.environmentID, EnvironmentName: "production",
		AuthorizedVolumeDir: "/var/lib/groundplane/volumes", Projection: projection,
		ServiceDependencyPlans: core.ServiceDependencyPlans{},
	}
	rawRender, err := EncodeReleaseRenderInput(render)
	if err != nil {
		t.Fatal(err)
	}
	renderDigest, err := domain.Digest(rawRender)
	if err != nil {
		t.Fatal(err)
	}
	intent := domain.Intent{
		ID: member.ReleaseID, EnvironmentID: published.environmentID, ServiceID: member.ServiceID,
		OperationID: task.OperationID, OperationKind: domain.OperationBlueprintApply,
		GroupOperationID: task.OperationID, GroupMemberOrdinal: 1,
		CandidateWorkload: render.CandidateWorkload, Tag: "stable", Strategy: domain.StrategyRecreate,
		OnFailure: domain.OnFailureSwitchBack, RenderInputID: render.ArtifactID, RenderInputDigest: renderDigest,
		CreatedAt: task.CreatedAt, Actor: "operator", OriginatingTaskID: task.ID,
		Workspace: domain.Workspace{
			Kind: domain.WorkspaceTenant, TenantID: task.Owner.TenantID,
			ProjectID: task.Owner.ProjectID, EnvironmentID: published.environmentID,
		},
	}
	if err := domain.ValidateIntent(intent); err != nil {
		t.Fatal(err)
	}
	checkpoint := domain.Checkpoint{ReleaseID: member.ReleaseID, State: domain.StatePending, UpdatedAt: task.CreatedAt}
	manifest.Members[0].IntentDigest, err = domain.Digest(intent)
	if err != nil {
		t.Fatal(err)
	}
	manifest.Members[0].RenderDigest = renderDigest
	manifest.Members[0].CheckpointDigest, err = domain.Digest(checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	manifest.Digest, err = blueprintCandidateManifestDigest(manifest)
	if err != nil {
		t.Fatal(err)
	}
	marker.ManifestDigest = manifest.Digest
	values := make(map[string][]byte)
	for key, record := range map[string]struct {
		typeName string
		value    any
	}{
		releasePublicationKey(publicationID):                          {"release-publication", marker},
		releaseManifestStagingKey(publicationID):                      {"release-staged-manifest", manifest},
		releaseIntentStagingKey(publicationID, member.ReleaseID):      {"release-intent", intent},
		releaseRenderInputStagingKey(publicationID, member.ReleaseID): {"release-render-input", json.RawMessage(rawRender)},
		releaseCheckpointStagingKey(publicationID, member.ReleaseID):  {"release-checkpoint", checkpoint},
	} {
		values[key], err = encodeReleaseRecord(record.typeName, record.value)
		if err != nil {
			t.Fatal(err)
		}
	}
	mutations := make([]Mutation, 0, len(values))
	for key, value := range values {
		mutations = append(mutations, Mutation{Type: MutationPut, Key: key, Value: value})
	}
	mutations = append(mutations, Mutation{
		Type: MutationPut, Key: environmentMutationEpochKey(published.environmentID), Value: read.Values[2].Value,
	})
	updated, err := published.store.Transact(ctx, nil, mutations)
	if err != nil || !updated.Succeeded {
		t.Fatalf("install exact candidate ledger = %#v, %v", updated, err)
	}

}
