package composehelpercontainer

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"testing"

	containerderrdefs "github.com/containerd/errdefs"
	"github.com/moby/moby/api/types/image"
	"github.com/moby/moby/client"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// TASK-31: a cached child reference is not proof of its executable config.
func TestManagedImageRejectsCachedIdentityMismatch(t *testing.T) {
	service := managedService()
	engine := &imageEngine{
		fakeEngine: newFakeEngine(nil),
		observed: client.ImageInspectResult{
			InspectResponse: image.InspectResponse{ID: "sha256:wrong", Os: "linux", Architecture: "amd64"},
		},
	}
	executor, err := NewWithEngine(engine, helperImage)
	if err != nil {
		t.Fatal(err)
	}
	err = executor.prepareServiceImage(context.Background(), service)
	if !errors.Is(err, errs.New(errs.KindStateConflict, "")) || len(engine.pulled) != 0 {
		t.Fatalf("error=%v pulls=%v", err, engine.pulled)
	}
}

type imageEngine struct {
	*fakeEngine
	observed          client.ImageInspectResult
	inspected, pulled []string
	inspectErrors     []error
	pullErr           error
	pullResult        *imagePull
}

func (engine *imageEngine) ImageInspect(
	_ context.Context,
	ref string,
	_ ...client.ImageInspectOption,
) (client.ImageInspectResult, error) {
	engine.inspected = append(engine.inspected, ref)
	if len(engine.inspectErrors) != 0 {
		err := engine.inspectErrors[0]
		engine.inspectErrors = engine.inspectErrors[1:]
		if err != nil {
			return client.ImageInspectResult{}, err
		}
	}
	return engine.observed, nil
}

func (engine *imageEngine) ImagePull(
	_ context.Context,
	ref string,
	_ client.ImagePullOptions,
) (client.ImagePullResponse, error) {
	engine.pulled = append(engine.pulled, ref)
	if engine.pullResult != nil {
		return engine.pullResult, engine.pullErr
	}
	return nil, engine.pullErr
}

type imagePull struct {
	client.ImagePullResponse
	waitErr, closeErr error
	waited, closed    bool
}

func (pull *imagePull) Wait(context.Context) error { pull.waited = true; return pull.waitErr }
func (pull *imagePull) Close() error               { pull.closed = true; return pull.closeErr }

// TASK-31: missing managed images are fetched by captured digest, re-inspected
// and accepted only after both the pull stream and identity checks succeed.
func TestManagedImagePreparation(t *testing.T) {
	for _, name := range []string{"cached", "missing", "wrong-platform", "wrong-pulled-config", "daemon-error", "pull-error", "wait-error", "close-error"} {
		t.Run(name, func(t *testing.T) {
			service := managedService()
			engine := &imageEngine{
				fakeEngine: newFakeEngine(nil),
				observed: client.ImageInspectResult{
					InspectResponse: image.InspectResponse{
						ID:           "sha256:" + hex.EncodeToString(service.ImageConfigDigest),
						Os:           "linux",
						Architecture: "amd64",
					},
				},
				pullResult: &imagePull{},
			}
			wantSuccess := name == "cached" || name == "missing"
			wantPull := name != "cached" && name != "wrong-platform" && name != "daemon-error"
			if wantPull {
				engine.inspectErrors = []error{containerderrdefs.ErrNotFound}
			}
			switch name {
			case "wrong-platform":
				engine.observed.Architecture = "arm64"
			case "wrong-pulled-config":
				engine.observed.ID = "sha256:wrong"
			case "daemon-error":
				engine.inspectErrors = []error{errors.New("daemon unavailable")}
			case "pull-error":
				engine.pullErr = errors.New("pull failed")
			case "wait-error":
				engine.pullResult.waitErr = errors.New("stream failed")
			case "close-error":
				engine.pullResult.closeErr = errors.New("close failed")
			}
			executor, err := NewWithEngine(engine, helperImage)
			if err != nil {
				t.Fatal(err)
			}
			err = executor.prepareServiceImage(context.Background(), service)
			if wantSuccess != (err == nil) {
				t.Fatalf("image preparation error=%v", err)
			}
			if wantPull != (len(engine.pulled) == 1) {
				t.Fatalf("pulls=%v", engine.pulled)
			}
			for _, ref := range append(engine.inspected, engine.pulled...) {
				if ref != service.ImageReference {
					t.Fatalf("used unsealed reference %q", ref)
				}
			}
			if wantPull && name != "pull-error" && (!engine.pullResult.waited || !engine.pullResult.closed) {
				t.Fatal("pull stream not waited and closed")
			}
			if name == "missing" && len(engine.inspected) != 2 {
				t.Fatal("pulled image was not reinspected")
			}
		})
	}
}

// TASK-31: workload startup must not silently fetch or resolve a missing image.
func TestWorkloadLocalImageIsNeverPulled(t *testing.T) {
	for _, missing := range []bool{false, true} {
		service := &agentpb.ComposeService{
			Role: agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_RECREATE_SINGLETON,
		}
		// Nonzero canonical local identity is supplied by the prepublished seal.
		service.ImageReference = "sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
		engine := &imageEngine{
			fakeEngine: newFakeEngine(nil),
			observed:   client.ImageInspectResult{InspectResponse: image.InspectResponse{ID: service.ImageReference}},
		}
		if missing {
			engine.inspectErrors = []error{containerderrdefs.ErrNotFound}
		}
		executor, err := NewWithEngine(engine, helperImage)
		if err != nil {
			t.Fatal(err)
		}
		err = executor.prepareServiceImage(context.Background(), service)
		if missing != (err != nil) || len(engine.pulled) != 0 ||
			len(engine.inspected) != 1 ||
			engine.inspected[0] != service.ImageReference {
			t.Fatalf(
				"missing=%v err=%v inspected=%v pulled=%v",
				missing,
				err,
				engine.inspected,
				engine.pulled,
			)
		}
	}
}

// TASK-31: actual executable identity, not a mutable tag or cached reference,
// controls managed image preparation.
func managedService() *agentpb.ComposeService {
	index, child, config := sha256.Sum256(
		[]byte("index"),
	), sha256.Sum256(
		[]byte("child"),
	), sha256.Sum256(
		[]byte("config"),
	)
	return &agentpb.ComposeService{
		OwnerComponentId: "cmp_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		ImageRepository:  "registry.example/managed",
		ImageIndexDigest: index[:], ImageChildDigest: child[:], ImageConfigDigest: config[:],
		ImageReference: "registry.example/managed@sha256:" + hex.EncodeToString(child[:]),
		ImageOs:        "linux", ImageArchitecture: "amd64",
	}
}
