package materialization

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	componentsdk "github.com/AlanD20/groundplane-component-sdk/component"
	"github.com/AlanD20/groundplane/internal/common/componentexecution"
	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// TASK-97 / CMP-05: a missing or rejecting registered validator must stop native
// Component file delivery before the writer replaces the previous serving file.
func TestMaterializationRejectsComponentFileWithoutPreflight(t *testing.T) {
	input, files, source := componentPreflightFixture(t)
	helper := &decodingMaterializationHelper{}
	runtime, err := New(helper, nil)
	if err != nil {
		t.Fatal(err)
	}
	err = runtime.RunComponentFile(t.Context(), input, files, 0, source, false)
	if err == nil || helper.volumeDir != "" || helper.content != nil || !source.closed {
		t.Fatalf("unvalidated Component file reached writer: %v", err)
	}
}

// TASK-97: the validated private bytes must be exactly those sent to the
// descriptor-confined writer, after the original source has been released.
func TestMaterializationPreflightPrecedesWriterAndPreservesExactBytes(t *testing.T) {
	input, files, source := componentPreflightFixture(t)
	helper := &decodingMaterializationHelper{}
	validator := &componentPreflightValidator{check: func(content []byte) {
		if helper.volumeDir != "" || !source.closed || string(content) != "native candidate\n" {
			t.Fatal("validator did not precede the writer with complete owned bytes")
		}
	}}
	runtime, err := New(helper, validator)
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.RunComponentFile(t.Context(), input, files, 0, source, false); err != nil {
		t.Fatal(err)
	}
	if validator.calls != 1 || validator.destination != files[0].Destination() ||
		string(helper.content) != "native candidate\n" ||
		validator.envelope.Artifact().ID().String() != input.Procedure.Files[0].MaterializationId {
		t.Fatal("validated bytes or captured action identity changed before materialization")
	}
}

// TASK-97: validation failure, corrupt input, cancellation or a validator that
// changes its input cannot write files or retain the captured source.
func TestMaterializationPreflightFailureNeverReachesWriter(t *testing.T) {
	for _, name := range []string{"native rejection", "source digest", "source close", "wrong action file",
		"wrong generation", "wrong digest", "changed bytes", "cancel"} {
		t.Run(name, func(t *testing.T) {
			input, files, source := componentPreflightFixture(t)
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
			case "wrong action file":
				input.Procedure.ConfigurationAction.ArtifactId = ids.New(ids.KindConfig)
			case "wrong generation":
				input.Procedure.ConfigurationAction.Generation++
			case "wrong digest":
				input.Procedure.ConfigurationAction.ArtifactDigest[0] ^= 1
			case "changed bytes":
				validator.check = func(content []byte) { content[0] ^= 1 }
			case "cancel":
				validator.check = func([]byte) { cancel() }
			}
			if name == "wrong action file" || name == "wrong generation" || name == "wrong digest" {
				// Independently reject a freshly captured malformed binding; a
				// stale input hash alone would not prove this admission rule.
				_, raw, _, captureErr := executionplan.CaptureComponentWorkInput(ids.New(ids.KindPlan), input.Procedure)
				clear(raw)
				if captureErr == nil {
					t.Fatal("native admission accepted an unbound configuration action")
				}
			}
			runtime, err := New(helper, validator)
			if err != nil {
				t.Fatal(err)
			}
			err = runtime.RunComponentFile(ctx, input, files, 0, source, false)
			if err == nil || helper.volumeDir != "" || !source.closed {
				t.Fatalf(
					"unsafe preflight reached writer: error=%v writer=%q closed=%t",
					err,
					helper.volumeDir,
					source.closed,
				)
			}
		})
	}
}

type componentPreflightValidator struct {
	calls       int
	destination string
	envelope    componentsdk.ActionEnvelope
	check       func([]byte)
	err         error
}

func (validator *componentPreflightValidator) ValidateComponentFile(_ context.Context,
	envelope componentsdk.ActionEnvelope, destination string, content []byte,
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

func componentPreflightFixture(
	t *testing.T,
) (componentexecution.HelperInput, []entrymaterialization.Header, *componentPreflightSource) {
	t.Helper()
	content := []byte("native candidate\n")
	digest := sha256.Sum256(content)
	environment, component := ids.New(ids.KindEnvironment), ids.New(ids.KindComponent)
	yaml := []byte("services:\n  router:\n    image: example.test/router@sha256:" + strings.Repeat("a", 64) + "\n")
	yamlDigest := sha256.Sum256(yaml)
	artifact := &agentpb.ComposeArtifact{ArtifactId: ids.New(ids.KindConfig),
		OwnerKind: agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT, OwnerId: environment,
		ProjectName: "gp-" + strings.ToLower(environment), CanonicalYaml: yaml, YamlSha256: yamlDigest[:],
		AuthorizedVolumeDir: "/var/lib/groundplane/vol/" + ids.New(
			ids.KindTenant,
		) + "/" + ids.New(
			ids.KindProject,
		) + "/" + environment,
		Services: []*agentpb.ComposeService{{ServiceId: ids.New(ids.KindService), ComposeName: "router",
			OwnerComponentId: component, ExpectedReplicas: 1}}}
	file := &agentpb.MaterializeFile{ArtifactId: artifact.ArtifactId, MaterializationId: ids.New(ids.KindConfig),
		EnvironmentId: environment, Destination: "components/example/config", Mode: 0o444,
		Length: uint64(
			len(content),
		), Sha256: digest[:], OutputKind: agentpb.MaterializationOutputKind_MATERIALIZATION_OUTPUT_KIND_PLAIN_FILE}
	procedure := &agentpb.ComponentWorkInput{OperationId: ids.New(ids.KindOperation), EnvironmentId: environment,
		ComponentId: component, RenderGeneration: 7, Candidate: artifact,
		Predecessor: &agentpb.ComponentWorkPredecessor{Absent: true}, Files: []*agentpb.MaterializeFile{file},
		Configuration: &agentpb.WorkConfigurationReference{ConfigurationId: ids.New(ids.KindConfig),
			EnvironmentId: environment, Generation: 7, Sha256: digest[:]},
		ConfigurationAction: &agentpb.ComponentApply{ComponentId: component, ArtifactId: file.MaterializationId,
			ArtifactDigest: digest[:], DefinitionDigest: digest[:], CatalogDigest: digest[:], ActionId: "activate-config", Generation: 7}}
	owned, raw, reference, err := executionplan.CaptureComponentWorkInput(ids.New(ids.KindPlan), procedure)
	clear(raw)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Now().UTC()
	attempt := workexecution.Attempt{
		ID:         ids.New(ids.KindAttempt),
		WorkItemID: ids.New(ids.KindWorkItem),
		TaskID:     ids.New(ids.KindTask),
		Number:     1,
		Direction:  workexecution.Forward,
		Input:      reference,
		AssignedAt: at,
		StartedAt:  &at,
		Deadline:   at.Add(time.Minute),
		Executor: workexecution.Identity{
			Executor:   workexecution.Agent,
			HostID:     ids.New(ids.KindHost),
			AgentID:    ids.New(ids.KindAgent),
			Generation: 1,
		},
	}
	progress, err := componentexecution.NewProgress(attempt, owned, at)
	if err != nil {
		t.Fatal(err)
	}
	progress, err = progress.Begin(attempt, owned, at)
	if err != nil {
		t.Fatal(err)
	}
	checkpoint, err := progress.Checkpoint(attempt, owned)
	if err != nil {
		t.Fatal(err)
	}
	header, err := entrymaterialization.NewHeader(
		entrymaterialization.HeaderSpec{TaskID: attempt.TaskID, StepID: checkpoint.StageID,
			EnvironmentID: environment, Generation: 7, Destination: file.Destination, OutputKind: entrymaterialization.OutputPlainFile,
			Mode: entrymaterialization.ModeReadOnly, Length: uint64(len(content)), Digest: digest},
	)
	if err != nil {
		t.Fatal(err)
	}
	return componentexecution.HelperInput{Attempt: attempt, Procedure: owned, Progress: progress},
		[]entrymaterialization.Header{header}, &componentPreflightSource{Reader: bytes.NewReader(content)}
}
