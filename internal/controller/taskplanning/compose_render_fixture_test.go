package taskplanning

import (
	strings "strings"
	testing "testing"
	time "time"

	ids "github.com/AlanD20/groundplane/internal/common/ids"
	composeidentity "github.com/AlanD20/groundplane/internal/controller/composeidentity"
	fixtureowner "github.com/AlanD20/groundplane/internal/controller/composerender"
	domain "github.com/AlanD20/groundplane/internal/core/release"
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

const (
	composeRenderTestEnvironmentID      = "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	composeRenderTestEnvironmentIDLower = "env_01arz3ndektsv4rrffq69g5fav"
	composeRenderTestArtifactID         = "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAV"
)

func composeIdentityTestID(kind ids.Kind, seed int64) string {
	return ids.NewAt(kind, time.Date(2026, 8, 22, 12, 0, 0, 0, time.UTC), seed)
}
