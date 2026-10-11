package taskplanning

import (
	strings "strings"
	testing "testing"
	time "time"

	ids "github.com/AlanD20/groundplane/internal/common/ids"
	serviceproxy "github.com/AlanD20/groundplane/internal/common/serviceproxy"
	composeidentity "github.com/AlanD20/groundplane/internal/controller/composeidentity"
	fixtureowner "github.com/AlanD20/groundplane/internal/controller/composerender"
	core "github.com/AlanD20/groundplane/internal/core"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	testcomponents "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	testenvironmentprojection "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	composetypes "github.com/compose-spec/compose-go/v2/types"
)

func releaseTestWorkload(reference string) domain.WorkloadSeal {
	return domain.WorkloadSeal{
		RequestedReference: reference,
		LocalImageID:       "sha256:" + strings.Repeat("a", 64),
		ReplicaCount:       1,
	}
}

func mustComposeIdentitySnapshotFromProjection(
	t *testing.T,
	projection testenvironmentprojection.EnvironmentComposeProjection,
) composeidentity.Snapshot {
	t.Helper()
	snapshot, err := fixtureowner.ComposeIdentitySnapshotFromProjection(projection)
	if err != nil {
		t.Fatalf("ComposeIdentitySnapshotFromProjection() error = %v", err)
	}
	return snapshot
}

func composeRenderTestInput(project *composetypes.Project) fixtureowner.ComposeRenderInput {
	return fixtureowner.ComposeRenderInput{
		Project:          project,
		ArtifactID:       composeRenderTestArtifactID,
		ProjectOwnerKind: fixtureowner.ComposeProjectOwnerTenant,
		TenantID:         "tnt_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		ProjectID:        "prj_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		EnvironmentID:    composeRenderTestEnvironmentID,
		PlanID:           "plan_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		RenderGeneration: 7,
		AuthorizedVolumeDir: "/var/lib/groundplane/vol/tnt_01ARZ3NDEKTSV4RRFFQ69G5FAV/" +
			"prj_01ARZ3NDEKTSV4RRFFQ69G5FAV/" + composeRenderTestEnvironmentID,
	}
}

func captureTestServiceProxySelection(
	t *testing.T,
	projection *testenvironmentprojection.EnvironmentComposeProjection,
	implementation serviceproxy.Implementation,
) {
	t.Helper()
	if projection == nil {
		t.Fatal("capture Service proxy selection into nil projection")
	}
	routerKind := core.ComponentKindIngressCaddy
	if implementation == serviceproxy.Envoy {
		routerKind = core.ComponentKindIngressEnvoy
	} else if implementation != serviceproxy.Caddy {
		t.Fatalf("unsupported test Service proxy implementation %q", implementation)
	}
	router, err := testcomponents.NewRecord(core.Component{
		ID: "cmp_01ARZ3NDEKTSV4RRFFQ69G5FAV", Owner: core.ComponentOwnerEnvironment,
		OwnerID: projection.EnvironmentID, Kind: routerKind,
	})
	if err != nil {
		t.Fatal(err)
	}
	tunnel, err := testcomponents.NewRecord(core.Component{
		ID: "cmp_01ARZ3NDEKTSV4RRFFQ69G5FAW", Owner: core.ComponentOwnerEnvironment,
		OwnerID: projection.EnvironmentID, Kind: core.ComponentKindEdgeCloudflare,
	})
	if err != nil {
		t.Fatal(err)
	}
	projection.Components = []testcomponents.Record{router, tunnel}
	if routerKind == core.ComponentKindIngressEnvoy {
		projection.Components[0], projection.Components[1] = projection.Components[1], projection.Components[0]
	}
}

const (
	composeRenderTestEnvironmentID      = "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	composeRenderTestEnvironmentIDLower = "env_01arz3ndektsv4rrffq69g5fav"
	composeRenderTestArtifactID         = "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAV"
)

func composeIdentityTestID(kind ids.Kind, seed int64) string {
	return ids.NewAt(kind, time.Date(2026, 8, 22, 12, 0, 0, 0, time.UTC), seed)
}
