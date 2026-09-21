package composerender

import (
	"encoding/hex"
	"testing"

	composeidentity "github.com/AlanD20/groundplane/internal/controller/composeidentity"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/proto/agentpb"
	composetypes "github.com/compose-spec/compose-go/v2/types"
	"google.golang.org/protobuf/proto"

	testcomponents "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	testenvironmentprojection "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	testservices "github.com/AlanD20/groundplane/internal/infra/etcd/services"
)

func testSelectedComponentImage(repository string) composeidentity.ComponentImage {
	image, platform, reference := controllerTestOCIPlatform(repository)
	return composeidentity.ComponentImage{
		Repository:  repository,
		IndexDigest: image.IndexDigest,
		Reference:   reference,
		Platform:    platform,
	}
}

// Rationale: pinned rerenders preserve their selected platform and config,
// including a non-current platform, and never reconstruct authority from today's catalog.
func TestComposeIdentitySnapshotPreservesSelectedComponentImage(t *testing.T) {
	const serviceID = "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	const componentID = "cmp_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	image := controllerTestOCIImage("example/router")
	platform, reference, _ := image.Select("linux", "arm64")
	selected := composeidentity.ComponentImage{
		Repository:  image.Repository,
		IndexDigest: image.IndexDigest,
		Reference:   reference,
		Platform:    platform,
	}
	for _, mutation := range []string{"none", "missing config", "mismatched reference", "invalid platform"} {
		t.Run(mutation, func(t *testing.T) {
			service := &agentpb.ComposeService{
				ServiceId:        serviceID,
				ComposeName:      "router",
				OwnerComponentId: componentID,
				ImageRepository:  image.Repository,
				ImageReference:   reference,
				ImageIndexDigest: mustDecodePlatformDigest(
					image.IndexDigest,
				),
				ImageChildDigest: mustDecodePlatformDigest(platform.ChildDigest),
				ImageConfigDigest: mustDecodePlatformDigest(
					platform.ConfigDigest,
				),
				ImageOs:           platform.OS,
				ImageArchitecture: platform.Architecture,
				ImageVariant:      platform.Variant,
			}
			switch mutation {
			case "missing config":
				service.ImageConfigDigest = nil
			case "mismatched reference":
				service.ImageReference = "example/router@sha256:" + platform.ConfigDigest
			case "invalid platform":
				service.ImageArchitecture = "riscv64"
			}
			encoded, err := proto.Marshal(&agentpb.ComposeArtifact{Services: []*agentpb.ComposeService{service}})
			if err != nil {
				t.Fatal(err)
			}
			projection := testenvironmentprojection.EnvironmentComposeProjection{ComposeArtifact: encoded,
				DesiredServices: []testservices.EnvironmentServiceProjection{
					{Desired: core.Service{ID: serviceID, Name: "router"}},
				},
				Components: []testcomponents.Record{
					{
						Desired: testcomponents.DesiredRecord{ID: componentID},
						Runtime: testcomponents.RuntimeRecord{GeneratedServices: []string{serviceID}},
					},
				}}
			identities, err := ComposeIdentitySnapshotFromProjection(projection)
			if mutation != "none" {
				if err == nil {
					t.Fatal("accepted invalid pinned image authority")
				}
				return
			}
			if err != nil || len(identities.Services) != 1 || identities.Services[0].ComponentImage == nil ||
				*identities.Services[0].ComponentImage != selected {
				t.Fatalf("pinned selected authority lost: %v, %v", identities, err)
			}
			if hex.EncodeToString(
				service.ImageConfigDigest,
			) != identities.Services[0].ComponentImage.Platform.ConfigDigest {
				t.Fatal("pinned config identity changed")
			}
			input := composeRenderTestInput(&composetypes.Project{Services: composetypes.Services{
				"router": {Name: "router", Image: reference},
			}})
			input.Identities = identities
			rerendered, err := RenderCompose(input)
			if err != nil {
				t.Fatal(err)
			}
			recovered, err := PinnedComponentServiceIdentity(rerendered.Services[0], componentID)
			if err != nil || recovered.ComponentImage == nil || *recovered.ComponentImage != selected {
				t.Fatalf("rerender changed pinned platform or image authority: %v, %v", recovered, err)
			}
		})
	}
}

func mustComposeIdentitySnapshotFromProjection(
	t *testing.T,
	projection testenvironmentprojection.EnvironmentComposeProjection,
) composeidentity.Snapshot {
	t.Helper()
	snapshot, err := ComposeIdentitySnapshotFromProjection(projection)
	if err != nil {
		t.Fatalf("ComposeIdentitySnapshotFromProjection() error = %v", err)
	}
	return snapshot
}

func TestComposeIdentitySnapshotRejectsDuplicateComponentOwnership(t *testing.T) {
	serviceID := "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	projection := testenvironmentprojection.EnvironmentComposeProjection{Components: []testcomponents.Record{
		{
			Desired: testcomponents.DesiredRecord{ID: "cmp_01ARZ3NDEKTSV4RRFFQ69G5FAV"},
			Runtime: testcomponents.RuntimeRecord{GeneratedServices: []string{serviceID}},
		},
		{
			Desired: testcomponents.DesiredRecord{ID: "cmp_01ARZ3NDEKTSV4RRFFQ69G5FAW"},
			Runtime: testcomponents.RuntimeRecord{GeneratedServices: []string{serviceID}},
		},
	}}
	if _, err := ComposeIdentitySnapshotFromProjection(projection); err == nil {
		t.Fatal("ComposeIdentitySnapshotFromProjection(duplicate owner) error = nil")
	}
}
