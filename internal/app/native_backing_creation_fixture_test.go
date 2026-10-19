package app

import (
	"net/netip"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/app/adaptercompiler"
	"github.com/AlanD20/groundplane/internal/common/imagefetch"
	"github.com/AlanD20/groundplane/internal/controller/backingservices"
	"github.com/AlanD20/groundplane/internal/controller/desiredrevision"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	ageinfra "github.com/AlanD20/groundplane/internal/infra/age"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backingprovisionwork"
	desiredstore "github.com/AlanD20/groundplane/internal/infra/etcd/desiredrevision"
)

// Reuses actual creation, encryption and publication owners. Only registry
// lookup and Agent-local image observations are supplied by the fixture.
func newNativeBackingCreationService(
	t *testing.T,
	f *nativeVolumeFixture,
) (*backingservices.CreationService, *nativeBackingCreationImages, *backingprovisionwork.Repository) {
	t.Helper()
	adaptercompiler.Register()
	desired, err := desiredstore.NewRepository(f.store)
	if err != nil {
		t.Fatal(err)
	}
	inputs, err := backingprovisionwork.NewRepository(f.store)
	if err != nil {
		t.Fatal(err)
	}
	key, err := ageinfra.GenerateKeypair()
	if err != nil {
		t.Fatal(err)
	}
	cipher := nativeVolumeCipher{key: key}
	protector, err := secretvalue.NewProtector(cipher, cipher)
	if err != nil {
		t.Fatal(err)
	}
	coordinator, err := requestidempotency.NewCoordinator(protector)
	if err != nil {
		t.Fatal(err)
	}
	replay, err := etcd.NewIdempotencyRepository(f.store)
	if err != nil {
		t.Fatal(err)
	}
	idempotency, err := desiredrevision.NewIdempotency(coordinator, replay)
	if err != nil {
		t.Fatal(err)
	}
	agent, err := f.agents.GetSingleton(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	images := &nativeBackingCreationImages{t: t, agentID: agent.Record.ID, plan: imagefetch.Plan{
		Requested: "registry.example/internal/search:1.4", Repository: "registry.example/internal/search",
		ManifestDigest: "sha256:" + strings.Repeat("a", 64), ConfigDigest: "sha256:" + strings.Repeat("b", 64),
		Architecture: "amd64"}}
	service, err := backingservices.NewCreationService(
		"/var/lib/groundplane/vol",
		netip.MustParsePrefix("10.0.0.0/8"),
		nativeDesiredRepository{
			f.blueprint,
			desired,
		},
		idempotency,
		protector,
		inputs,
		nil,
		nil,
		f.agents,
		images,
		images,
	)
	if err != nil {
		t.Fatal(err)
	}
	return service, images, inputs
}
