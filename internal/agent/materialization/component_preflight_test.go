package materialization

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"testing"

	componentsdk "github.com/AlanD20/groundplane-component-sdk/component"
	testtaskassignment "github.com/AlanD20/groundplane/internal/agent/taskassignment"
	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func TestMaterializationRejectsComponentFileWithoutPreflight(t *testing.T) {
	// Rationale: validating only after Compose starts a Component would replace
	// its last serving file before discovering an invalid native configuration.
	assignment, payload, source := componentPreflightFixture(t)
	helper := &decodingMaterializationHelper{}
	runtime, err := New(helper, nil)
	if err != nil {
		t.Fatal(err)
	}
	err = runtime.ExecuteStep(context.Background(), assignment, assignment.Plan.Steps[0], payload)
	if err == nil || helper.volumeDir != "" || helper.content != nil || !source.closed {
		t.Fatalf("unvalidated Component file reached writer: error=%v, directory=%q", err, helper.volumeDir)
	}
}

func TestMaterializationPreflightPrecedesWriterAndPreservesExactBytes(t *testing.T) {
	// Rationale: verification must use the same immutable bytes later framed to
	// the writer, and a successful callback cannot silently alter those bytes.
	assignment, payload, source := componentPreflightFixture(t)
	helper := &decodingMaterializationHelper{}
	validator := &componentPreflightValidator{check: func(content []byte) {
		if helper.volumeDir != "" || !source.closed || string(content) != "native candidate\n" {
			t.Fatal("validator did not precede the writer with owned complete bytes")
		}
	}}
	runtime, err := New(helper, validator)
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.ExecuteStep(t.Context(), assignment, assignment.Plan.Steps[0], payload); err != nil {
		t.Fatal(err)
	}
	if validator.calls != 1 || validator.destination != payload.Header.Destination() ||
		string(
			helper.content,
		) != "native candidate\n" || validator.envelope.Artifact().ID().String() != materializationID {
		t.Fatal("validated bytes or sealed authority changed before materialization")
	}
}

func TestMaterializationPreflightFailureNeverReachesWriter(t *testing.T) {
	// Rationale: native rejection, changed inputs, cancellation, or malformed
	// sealed correlation must retain the previous file and release private bytes.
	for _, name := range []string{"native rejection", "source digest", "source close", "duplicate action", "wrong prerequisite", "wrong digest", "changed bytes", "cancel"} {
		t.Run(name, func(t *testing.T) {
			assignment, payload, source := componentPreflightFixture(t)
			helper := &decodingMaterializationHelper{}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			validator := &componentPreflightValidator{}
			switch name {
			case "native rejection":
				validator.err = errors.New("invalid native config")
			case "source digest":
				source.Reader = bytes.NewReader([]byte("changed bytes\n"))
			case "source close":
				source.err = errors.New("close failed")
			case "duplicate action":
				assignment.Plan.Steps = append(assignment.Plan.Steps, assignment.Plan.Steps[1])
			case "wrong prerequisite":
				assignment.Plan.Steps[1].PrerequisiteStepId = materializationSecondStepID
			case "wrong digest":
				assignment.Plan.Steps[1].GetComponentApply().ArtifactDigest = make([]byte, sha256.Size)
			case "changed bytes":
				validator.check = func(content []byte) { content[0] ^= 1 }
			case "cancel":
				validator.check = func([]byte) { cancel() }
			}
			runtime, err := New(helper, validator)
			if err != nil {
				t.Fatal(err)
			}
			err = runtime.ExecuteStep(ctx, assignment, assignment.Plan.Steps[0], payload)
			if err == nil || helper.volumeDir != "" || !source.closed {
				t.Fatalf(
					"unsafe preflight reached writer: error=%v, writer=%q, closed=%v",
					err,
					helper.volumeDir,
					source.closed,
				)
			}
		})
	}
}

func TestMaterializationPreflightFollowsRouteComposePrerequisite(t *testing.T) {
	// Rationale: Route mutations seal file -> Compose -> activation, while a
	// Blueprint activation can name the file directly. Both must preflight it.
	assignment, payload, _ := componentPreflightFixture(t)
	activation := assignment.Plan.Steps[1]
	activation.PrerequisiteStepId = "step_01ARZ3NDEKTSV4RRFFQ69G5FAX"
	assignment.Plan.Steps = []*agentpb.ExecutionStep{
		assignment.Plan.Steps[0],
		{StepId: activation.PrerequisiteStepId, PrerequisiteStepId: materializationStepID,
			Payload: &agentpb.ExecutionStep_ComposeApply{
				ComposeApply: &agentpb.ComposeApply{ArtifactId: materializationArtifactID},
			}},
		activation,
	}
	helper := &decodingMaterializationHelper{}
	validator := &componentPreflightValidator{}
	runtime, err := New(helper, validator)
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.ExecuteStep(t.Context(), assignment, assignment.Plan.Steps[0], payload); err != nil {
		t.Fatalf("transitive file prerequisite rejected: %v", err)
	}
	if validator.calls != 1 || helper.volumeDir == "" {
		t.Fatal("Route file was not preflighted and materialized")
	}
}

type componentPreflightValidator struct {
	calls       int
	destination string
	envelope    componentsdk.ActionEnvelope
	check       func([]byte)
	err         error
}

func (validator *componentPreflightValidator) ValidateComponentFile(
	_ context.Context, envelope componentsdk.ActionEnvelope, destination string, content []byte,
) error {
	validator.calls++
	validator.destination, validator.envelope = destination, envelope
	if validator.check != nil {
		validator.check(content)
	}
	return validator.err
}

type componentPreflightSource struct {
	io.Reader
	closed bool
	err    error
}

func (source *componentPreflightSource) Close() error {
	source.closed = true
	return source.err
}

func componentPreflightFixture(t *testing.T) (testtaskassignment.Assignment, Payload, *componentPreflightSource) {
	t.Helper()
	content := []byte("native candidate\n")
	assignment := materializationAssignment(t, content)
	step := assignment.Plan.Steps[0]
	materialization := step.GetMaterializeFile()
	materialization.Destination = "components/example/config"
	digest := sha256.Sum256(content)
	assignment.Plan.Steps = append(assignment.Plan.Steps, &agentpb.ExecutionStep{
		StepId: materializationSecondStepID, PrerequisiteStepId: step.StepId,
		Payload: &agentpb.ExecutionStep_ComponentApply{ComponentApply: &agentpb.ComponentApply{
			ComponentId: "cmp_01ARZ3NDEKTSV4RRFFQ69G5FAV", ArtifactId: materialization.MaterializationId,
			ArtifactDigest: digest[:], DefinitionDigest: digest[:], CatalogDigest: digest[:],
			ActionId: "activate-config", Generation: assignment.Plan.RenderGeneration,
		}},
	})
	header, err := entrymaterialization.NewHeader(entrymaterialization.HeaderSpec{
		TaskID: assignment.TaskID, StepID: step.StepId, EnvironmentID: materializationEnvironmentID,
		Generation: assignment.Plan.RenderGeneration, Destination: materialization.Destination,
		OutputKind: entrymaterialization.OutputPlainFile, Mode: entrymaterialization.ModeReadOnly,
		Length: uint64(len(content)), Digest: digest,
	})
	if err != nil {
		t.Fatal(err)
	}
	source := &componentPreflightSource{Reader: bytes.NewReader(content)}
	return assignment, Payload{Header: header, Source: source}, source
}
