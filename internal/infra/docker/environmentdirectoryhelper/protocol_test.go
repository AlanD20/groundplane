package environmentdirectoryhelper

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

const (
	testTaskID        = "task_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	testOperationID   = "op_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	testPlanID        = "plan_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	testStepID        = "step_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	testEnvironmentID = "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	testArtifactID    = "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	testVolumeID      = "vol_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	testVolumeRoot    = "/srv/groundplane/vol"
	testVolumeDir     = testVolumeRoot + "/tnt_01ARZ3NDEKTSV4RRFFQ69G5FAV/prj_01ARZ3NDEKTSV4RRFFQ69G5FAV/" + testEnvironmentID
)

type fakeCreator struct {
	root      string
	directory string
	volumes   []ManagedVolume
	removed   bool
	err       error
}

func (creator *fakeCreator) EnsureManagedVolumes(
	_ context.Context,
	request ManagedVolumeEnsureRequest,
) error {
	creator.root = request.VolumeRoot
	creator.directory = request.VolumeDir
	creator.volumes = append([]ManagedVolume(nil), request.Volumes...)
	return creator.err
}

func (creator *fakeCreator) Create(_ context.Context, root string, directory string) error {
	creator.root = root
	creator.directory = directory
	return creator.err
}

func (creator *fakeCreator) Remove(_ context.Context, root string, directory string) error {
	creator.root = root
	creator.directory = directory
	creator.removed = true
	return creator.err
}

func TestRequestFrameRoundTripsAndRejectsTrailingBytes(t *testing.T) {
	request := validRequest(t)
	framed, err := MarshalRequest(request)
	if err != nil {
		t.Fatalf("MarshalRequest() error = %v", err)
	}
	decoded, err := ReadRequest(context.Background(), bytes.NewReader(framed))
	if err != nil || decoded.TaskId != request.TaskId || decoded.Plan.GetTargetId() != testEnvironmentID {
		t.Fatalf("ReadRequest() = %#v, %v", decoded, err)
	}
	framed = append(framed, 0)
	if _, err := ReadRequest(
		context.Background(),
		bytes.NewReader(framed),
	); !errors.Is(
		err,
		errs.New(errs.KindValidationFailed, ""),
	) {
		t.Fatalf("ReadRequest(trailing) error = %v, want validation.failed", err)
	}
}

func TestExecuteAuthorizesRootAndReturnsClosedOutcome(t *testing.T) {
	creator := &fakeCreator{}
	response, err := Execute(context.Background(), testVolumeRoot, creator, validRequest(t))
	if err != nil || response.ExitCode != 0 || response.FailedStepId != "" {
		t.Fatalf("Execute(success) = %#v, %v", response, err)
	}
	if creator.root != testVolumeRoot || creator.directory != testVolumeDir {
		t.Fatalf("creator = root %q directory %q", creator.root, creator.directory)
	}

	creator.err = errors.New("mutation failed")
	response, err = Execute(context.Background(), testVolumeRoot, creator, validRequest(t))
	if err != nil || response.ExitCode != 1 || response.FailedStepId != testStepID {
		t.Fatalf("Execute(failure) = %#v, %v", response, err)
	}
}

func TestExecuteRejectsUntrustedRootBeforeMutation(t *testing.T) {
	creator := &fakeCreator{}
	response, err := Execute(context.Background(), "/var/lib/groundplane/vol", creator, validRequest(t))
	if response != nil || !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("Execute(untrusted root) = %#v, %v", response, err)
	}
	if creator.root != "" || creator.directory != "" {
		t.Fatal("creator was called before trusted-root authorization")
	}
}

// Rationale: the helper must derive direct managed-volume leaves only from
// the authenticated artifact table rather than accepting caller paths.
func TestExecuteEnsuresManagedVolumeDirectoriesFromArtifact(t *testing.T) {
	creator := &fakeCreator{}
	response, err := Execute(context.Background(), testVolumeRoot, creator, validManagedVolumeRequest(t))
	if err != nil || response.ExitCode != 0 || response.FailedStepId != "" {
		t.Fatalf("Execute(managed volumes) = %#v, %v", response, err)
	}
	if creator.root != testVolumeRoot || creator.directory != testVolumeDir ||
		len(creator.volumes) != 1 || creator.volumes[0].ID != testVolumeID || creator.volumes[0].Key != "app-data" {
		t.Fatalf("managed volume creator = %#v", creator)
	}
}

// Rationale: the helper must select directory removal only from an authenticated remove plan and preserve the
// Controller-authorized Environment path exactly.
func TestExecuteRemovesEnvironmentDirectoryFromPlan(t *testing.T) {
	creator := &fakeCreator{}
	response, err := Execute(context.Background(), testVolumeRoot, creator, validRemoveRequest(t))
	if err != nil || response.ExitCode != 0 || !creator.removed || creator.directory != testVolumeDir {
		t.Fatalf("Execute(remove) = %#v, creator %#v, %v", response, creator, err)
	}
}

func validRequest(t *testing.T) *agentpb.EnvironmentDirectoryHelperRequest {
	t.Helper()
	plan, err := executionplan.Seal(&agentpb.ExecutionPlan{
		Schema: executionplan.SchemaVersion, PlanId: testPlanID, RenderGeneration: 1,
		Operation: agentpb.PlanOperation_PLAN_OPERATION_ENVIRONMENT_CREATE,
		TargetId:  testEnvironmentID,
		Steps: []*agentpb.ExecutionStep{{
			StepId: testStepID, TimeoutSeconds: 30,
			Payload: &agentpb.ExecutionStep_EnvironmentDirectoryCreate{
				EnvironmentDirectoryCreate: &agentpb.EnvironmentDirectoryCreate{
					EnvironmentId: testEnvironmentID, ExpectedVolumeDir: testVolumeDir,
				},
			},
		}},
	})
	if err != nil {
		t.Fatalf("Seal() error = %v", err)
	}
	return &agentpb.EnvironmentDirectoryHelperRequest{
		Schema: SchemaVersion, AssignmentId: "asgn_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		TaskId: testTaskID, OperationId: testOperationID,
		Plan: plan, StepId: testStepID, TimeoutSeconds: 30,
	}
}

func validManagedVolumeRequest(t *testing.T) *agentpb.EnvironmentDirectoryHelperRequest {
	t.Helper()
	yaml := []byte("volumes:\n  app-data:\n    driver: local\n# " + strings.Repeat("a", 64) + "\n")
	digest := sha256.Sum256(yaml)
	plan, err := executionplan.Seal(&agentpb.ExecutionPlan{
		Schema: executionplan.SchemaVersion, PlanId: testPlanID, RenderGeneration: 1,
		Operation: agentpb.PlanOperation_PLAN_OPERATION_DEPLOY, TargetId: testEnvironmentID,
		Artifacts: []*agentpb.ComposeArtifact{{
			ArtifactId: testArtifactID,
			OwnerKind:  agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT,
			OwnerId:    testEnvironmentID, ProjectName: "gp-" + strings.ToLower(testEnvironmentID),
			CanonicalYaml: yaml, YamlSha256: digest[:], AuthorizedVolumeDir: testVolumeDir,
			Volumes: []*agentpb.ComposeVolume{{
				VolumeId: testVolumeID, ComposeName: "app-data", DockerName: "gp_vol_" + testVolumeID,
				ExpectedLabels: []*agentpb.LabelPair{
					{Key: "com.groundplane.environment-id", Value: testEnvironmentID},
					{Key: "com.groundplane.kind", Value: "volume"},
					{Key: "com.groundplane.managed", Value: "true"},
				},
			}},
		}},
		Steps: []*agentpb.ExecutionStep{{
			StepId: testStepID, TimeoutSeconds: 30,
			Payload: &agentpb.ExecutionStep_ManagedVolumeDirectoriesEnsure{
				ManagedVolumeDirectoriesEnsure: &agentpb.ManagedVolumeDirectoriesEnsure{
					ArtifactId: testArtifactID, VolumeIds: []string{testVolumeID}, IntentSha256: make([]byte, 32),
				},
			},
		}},
	})
	if err != nil {
		t.Fatalf("Seal() error = %v", err)
	}
	return &agentpb.EnvironmentDirectoryHelperRequest{
		Schema: SchemaVersion, AssignmentId: "asgn_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		TaskId: testTaskID, OperationId: testOperationID,
		Plan: plan, StepId: testStepID, TimeoutSeconds: 30,
	}
}

func validRemoveRequest(t *testing.T) *agentpb.EnvironmentDirectoryHelperRequest {
	t.Helper()
	plan, err := executionplan.Seal(&agentpb.ExecutionPlan{
		Schema: executionplan.SchemaVersion, PlanId: testPlanID, RenderGeneration: 1,
		Operation: agentpb.PlanOperation_PLAN_OPERATION_REMOVE, TargetId: testEnvironmentID,
		Steps: []*agentpb.ExecutionStep{{
			StepId: testStepID, TimeoutSeconds: 30,
			Payload: &agentpb.ExecutionStep_EnvironmentDirectoryRemove{
				EnvironmentDirectoryRemove: &agentpb.EnvironmentDirectoryRemove{
					EnvironmentId: testEnvironmentID, ExpectedVolumeDir: testVolumeDir,
				},
			},
		}},
	})
	if err != nil {
		t.Fatalf("Seal() error = %v", err)
	}
	return &agentpb.EnvironmentDirectoryHelperRequest{
		Schema: SchemaVersion, AssignmentId: "asgn_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		TaskId: testTaskID, OperationId: testOperationID,
		Plan: plan, StepId: testStepID, TimeoutSeconds: 30,
	}
}
