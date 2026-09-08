package composehelpercontainer

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"reflect"
	"sort"
	"testing"

	"google.golang.org/protobuf/proto"

	containerderrdefs "github.com/containerd/errdefs"
	"github.com/moby/moby/api/types/image"
	"github.com/moby/moby/client"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// A cached child reference is not proof of its actual executable config.
func TestManagedImageMismatchPreventsHelperCreation(t *testing.T) {
	request := managedImageRequest(t)
	engine := &imageEngine{
		fakeEngine: newFakeEngine(t, successfulResponse()),
		observed: client.ImageInspectResult{
			InspectResponse: image.InspectResponse{ID: "sha256:wrong", Os: "linux", Architecture: "amd64"},
		},
	}
	executor, err := NewWithEngine(engine, helperImage)
	if err != nil {
		t.Fatal(err)
	}
	response, err := executor.Execute(context.Background(), request)
	if response != nil || !errors.Is(err, errs.New(errs.KindStateConflict, "")) || len(engine.createCalls) != 0 ||
		len(engine.pulled) != 0 {
		t.Fatalf("response=%v error=%v creates=%d pulls=%v", response, err, len(engine.createCalls), engine.pulled)
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

func TestManagedImagePreparation(t *testing.T) {
	for _, name := range []string{"cached", "missing", "wrong-platform", "wrong-pulled-config", "daemon-error", "pull-error", "wait-error", "close-error"} {
		t.Run(name, func(t *testing.T) {
			request := managedImageRequest(t)
			service := request.Plan.Artifacts[0].Services[0]
			engine := &imageEngine{
				fakeEngine: newFakeEngine(t, successfulResponse()),
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
			response, err := executor.Execute(context.Background(), request)
			if wantSuccess != (err == nil && response != nil) || wantSuccess != (len(engine.createCalls) == 1) {
				t.Fatalf("response=%v err=%v creates=%d", response, err, len(engine.createCalls))
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

func TestWorkloadLocalImageIsNeverPulled(t *testing.T) {
	for _, missing := range []bool{false, true} {
		request := helperRequest(t)
		service := request.Plan.Artifacts[0].Services[0]
		// Nonzero canonical local identity is supplied by the prepublished seal.
		service.ImageReference = "sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
		request.Plan.PlanHash = nil
		var err error
		request.Plan, err = executionplan.Seal(request.Plan)
		if err != nil {
			t.Fatal(err)
		}
		engine := &imageEngine{
			fakeEngine: newFakeEngine(t, successfulResponse()),
			observed:   client.ImageInspectResult{InspectResponse: image.InspectResponse{ID: service.ImageReference}},
		}
		if missing {
			engine.inspectErrors = []error{containerderrdefs.ErrNotFound}
		}
		executor, err := NewWithEngine(engine, helperImage)
		if err != nil {
			t.Fatal(err)
		}
		response, err := executor.Execute(context.Background(), request)
		if missing != (err != nil) || (!missing) != (response != nil) || len(engine.pulled) != 0 ||
			len(engine.inspected) != 1 ||
			engine.inspected[0] != service.ImageReference ||
			missing && len(engine.createCalls) != 0 {
			t.Fatalf(
				"missing=%v response=%v err=%v inspected=%v pulled=%v creates=%d",
				missing,
				response,
				err,
				engine.inspected,
				engine.pulled,
				len(engine.createCalls),
			)
		}
	}
}

func TestReadOnlyAndInvalidRequestsDoNotPrepareImages(t *testing.T) {
	for _, name := range []string{"stop", "remove", "invalid"} {
		t.Run(name, func(t *testing.T) {
			request := managedImageRequest(t)
			step := request.Plan.Steps[0]
			artifact := request.Plan.Artifacts[0]
			switch name {
			case "stop":
				step.Payload = &agentpb.ExecutionStep_ComposeStop{
					ComposeStop: &agentpb.ComposeStop{
						ArtifactId:   artifact.ArtifactId,
						ServiceIds:   []string{artifact.Services[0].ServiceId},
						GraceSeconds: 10,
					},
				}
			case "remove":
				step.Payload = &agentpb.ExecutionStep_ComposeRemove{
					ComposeRemove: &agentpb.ComposeRemove{
						ArtifactId: artifact.ArtifactId,
						ServiceIds: []string{artifact.Services[0].ServiceId},
					},
				}
			}
			request.Plan.PlanHash = nil
			var err error
			request.Plan, err = executionplan.Seal(request.Plan)
			if err != nil {
				t.Fatal(err)
			}
			if name == "invalid" {
				request.Schema = 999
			}
			engine := &imageEngine{fakeEngine: newFakeEngine(t, successfulResponse())}
			executor, err := NewWithEngine(engine, helperImage)
			if err != nil {
				t.Fatal(err)
			}
			_, err = executor.Execute(context.Background(), request)
			if (name == "invalid") != (err != nil) || len(engine.inspected) != 0 || len(engine.pulled) != 0 {
				t.Fatalf("err=%v inspected=%v pulled=%v", err, engine.inspected, engine.pulled)
			}
		})
	}
}

func managedImageRequest(t *testing.T) *agentpb.ComposeHelperRequest {
	t.Helper()
	request := helperRequest(t)
	service := request.Plan.Artifacts[0].Services[0]
	service.OwnerComponentId = "cmp_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	service.ImageRepository = "registry.example/managed"
	index, child, config := sha256.Sum256(
		[]byte("index"),
	), sha256.Sum256(
		[]byte("child"),
	), sha256.Sum256(
		[]byte("config"),
	)
	service.ImageIndexDigest, service.ImageChildDigest, service.ImageConfigDigest = index[:], child[:], config[:]
	service.ImageReference = service.ImageRepository + "@sha256:" + hex.EncodeToString(child[:])
	service.ImageOs, service.ImageArchitecture = "linux", "amd64"
	artifact := request.Plan.Artifacts[0]
	artifact.CanonicalYaml = []byte("services:\n  api:\n    image: " + service.ImageReference + "\n")
	yamlHash := sha256.Sum256(artifact.CanonicalYaml)
	artifact.YamlSha256 = yamlHash[:]
	service.ExpectedLabels = append(
		service.ExpectedLabels,
		&agentpb.LabelPair{Key: "com.groundplane.component-id", Value: service.OwnerComponentId},
		&agentpb.LabelPair{Key: "com.groundplane.image-index-digest", Value: "sha256:" + hex.EncodeToString(index[:])},
		&agentpb.LabelPair{Key: "com.groundplane.image-child-digest", Value: "sha256:" + hex.EncodeToString(child[:])},
		&agentpb.LabelPair{
			Key:   "com.groundplane.image-config-digest",
			Value: "sha256:" + hex.EncodeToString(config[:]),
		},
		&agentpb.LabelPair{Key: "com.groundplane.image-platform", Value: "linux/amd64"},
	)
	sort.Slice(
		service.ExpectedLabels,
		func(i, j int) bool { return service.ExpectedLabels[i].Key < service.ExpectedLabels[j].Key },
	)
	var err error
	request.Plan.PlanHash = nil
	request.Plan, err = executionplan.Seal(request.Plan)
	if err != nil {
		t.Fatal(err)
	}
	return request
}

// Only the selected service and its transitive dependency may trigger pulls.
func TestManagedStartupPreparesDependencyButNotUnrelatedService(t *testing.T) {
	request := managedImageRequest(t)
	artifact := request.Plan.Artifacts[0]
	selected := artifact.Services[0]
	dependency, unrelated := proto.CloneOf(selected), proto.CloneOf(selected)
	dependency.ComposeName = "dependency"
	unrelated.ComposeName = "unrelated"
	dependency.ServiceId = "svc_01ARZ3NDEKTSV4RRFFQ69G5FAW"
	unrelated.ServiceId = "svc_01ARZ3NDEKTSV4RRFFQ69G5FAX"
	for _, service := range []*agentpb.ComposeService{dependency, unrelated} {
		for _, label := range service.ExpectedLabels {
			if label.Key == "com.groundplane.service-id" {
				label.Value = service.ServiceId
			}
		}
	}
	dependency.ImageRepository = "registry.example/dependency"
	unrelated.ImageRepository = "registry.example/unrelated"
	dependency.ImageReference = dependency.ImageRepository + "@sha256:" + hex.EncodeToString(
		dependency.ImageChildDigest,
	)
	unrelated.ImageReference = unrelated.ImageRepository + "@sha256:" + hex.EncodeToString(unrelated.ImageChildDigest)
	artifact.Services = append(artifact.Services, dependency, unrelated)
	artifact.CanonicalYaml = []byte(
		"services:\n  api:\n    image: " + selected.ImageReference + "\n    depends_on:\n      dependency:\n        condition: service_started\n  dependency:\n    image: " + dependency.ImageReference + "\n  unrelated:\n    image: " + unrelated.ImageReference + "\n",
	)
	hash := sha256.Sum256(artifact.CanonicalYaml)
	artifact.YamlSha256 = hash[:]
	request.Plan.PlanHash = nil
	var err error
	request.Plan, err = executionplan.Seal(request.Plan)
	if err != nil {
		t.Fatal(err)
	}
	engine := &imageEngine{
		fakeEngine: newFakeEngine(t, successfulResponse()),
		observed: client.ImageInspectResult{
			InspectResponse: image.InspectResponse{
				ID:           "sha256:" + hex.EncodeToString(selected.ImageConfigDigest),
				Os:           "linux",
				Architecture: "amd64",
			},
		},
	}
	executor, err := NewWithEngine(engine, helperImage)
	if err != nil {
		t.Fatal(err)
	}
	_, err = executor.Execute(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(engine.inspected, []string{selected.ImageReference, dependency.ImageReference}) ||
		len(engine.createCalls) != 1 {
		t.Fatalf("inspected=%v creates=%d", engine.inspected, len(engine.createCalls))
	}
}

func TestInvalidPlatformAuthorityPreventsAllImageIO(t *testing.T) {
	request := managedImageRequest(t)
	service := request.Plan.Artifacts[0].Services[0]
	// Platform-owned generated services have no Component management owner.
	service.OwnerComponentId = ""
	for i, label := range service.ExpectedLabels {
		if label.Key == "com.groundplane.component-id" {
			service.ExpectedLabels = append(service.ExpectedLabels[:i], service.ExpectedLabels[i+1:]...)
			break
		}
	}
	service.ImageChildDigest = nil
	request.Plan.PlanHash = nil
	var err error
	request.Plan, err = executionplan.Seal(request.Plan)
	if err != nil {
		t.Fatal(err)
	}
	engine := &imageEngine{fakeEngine: newFakeEngine(t, successfulResponse())}
	executor, err := NewWithEngine(engine, helperImage)
	if err != nil {
		t.Fatal(err)
	}
	response, err := executor.Execute(context.Background(), request)
	if response != nil || !errors.Is(err, errs.New(errs.KindValidationFailed, "")) || len(engine.inspected) != 0 ||
		len(engine.pulled) != 0 ||
		len(engine.createCalls) != 0 {
		t.Fatalf(
			"response=%v err=%v inspected=%v pulls=%v creates=%d",
			response,
			err,
			engine.inspected,
			engine.pulled,
			len(engine.createCalls),
		)
	}
}
