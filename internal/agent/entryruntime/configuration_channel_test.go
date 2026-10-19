package entryruntime

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/entryexecution"
	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// TASK-92 / ENT-02/09: the native replacement for generic file transfers must
// bind plaintext to the granted attempt/file and wipe rejected or consumed
// delivery buffers. A valid digest alone must not authorize another destination.
func TestNativeEntryConfigurationDeliveryOwnsExactPrivateFile(t *testing.T) {
	for _, scenario := range []string{"valid", "wrong attempt", "wrong generation", "wrong destination",
		"changed bytes", "unknown envelope", "provider failure", "cancelled"} {
		t.Run(scenario, func(t *testing.T) {
			input, header := entryChannelInput(t)
			identity, err := workexecution.EncodeAgentAttempt(input.Attempt)
			if err != nil {
				t.Fatal(err)
			}
			content := []byte("private captured value\n")
			delivery := &agentpb.EntryConfigurationFileDelivery{Attempt: identity, StageId: header.StepID(),
				Configuration: proto.CloneOf(input.Procedure.Configuration),
				File:          entryexecution.EncodeConfigurationFile(header), Content: content}
			exchange := &entryChannelExchange{delivery: delivery}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			switch scenario {
			case "wrong attempt":
				delivery.Attempt.AttemptId = ids.New(ids.KindAttempt)
			case "wrong generation":
				delivery.Configuration.Generation++
			case "wrong destination":
				delivery.File.Destination = "config/other.ini"
			case "changed bytes":
				content[0] ^= 1
			case "unknown envelope":
				delivery.ProtoReflect().SetUnknown([]byte{0xf8, 0x07, 1})
			case "provider failure":
				exchange.err = errors.New("delivery failed after receiving a private body")
			case "cancelled":
				cancel()
			}
			source, err := NewChannelConfigurationSource(exchange)
			if err != nil {
				t.Fatal(err)
			}
			reader, err := source.OpenEntryFile(ctx, input, header)
			if scenario == "valid" {
				if err != nil || reader == nil || delivery.Content != nil {
					t.Fatalf("private file ownership did not transfer: %v", err)
				}
				got, err := io.ReadAll(reader)
				if err != nil || string(got) != "private captured value\n" {
					t.Fatalf("wrong captured file bytes: %v", err)
				}
				clear(got)
				if err := reader.Close(); err != nil {
					t.Fatal(err)
				}
			} else if err == nil || reader != nil {
				t.Fatal("unselected, changed or cancelled private file became readable")
			}
			if delivery.Content != nil || !bytes.Equal(content, make([]byte, len(content))) {
				t.Fatal("delivery retained its owned private plaintext")
			}
			if exchange.request == nil || exchange.request.Destination != header.Destination() ||
				exchange.request.StageId != header.StepID() {
				t.Fatal("request did not select the captured file stage")
			}
		})
	}
}

type entryChannelExchange struct {
	ConfigurationExchange
	delivery *agentpb.EntryConfigurationFileDelivery
	request  *agentpb.EntryConfigurationFileRequest
	err      error
}

func (exchange *entryChannelExchange) EntryConfigurationFile(_ context.Context,
	request *agentpb.EntryConfigurationFileRequest,
) (*agentpb.EntryConfigurationFileDelivery, error) {
	exchange.request = request
	return exchange.delivery, exchange.err
}

func entryChannelInput(t *testing.T) (entryexecution.FileInput, entrymaterialization.Header) {
	t.Helper()
	content, yaml := []byte("private captured value\n"), []byte("services: {}\n")
	digest, yamlDigest := sha256.Sum256(content), sha256.Sum256(yaml)
	environment := ids.New(ids.KindEnvironment)
	baseline := &agentpb.ComposeArtifact{ArtifactId: ids.New(ids.KindConfig),
		OwnerKind: agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT, OwnerId: environment,
		ProjectName: "gp-" + strings.ToLower(environment), CanonicalYaml: yaml, YamlSha256: yamlDigest[:],
		AuthorizedVolumeDir: "/var/lib/groundplane/vol/" + ids.New(
			ids.KindTenant,
		) + "/" + ids.New(
			ids.KindProject,
		) + "/" + environment}
	candidate := proto.CloneOf(baseline)
	candidate.ArtifactId = ids.New(ids.KindConfig)
	procedure := &agentpb.EntryConfigurationWorkInput{OperationId: ids.New(ids.KindOperation),
		EnvironmentId: environment, RenderGeneration: 8, Baseline: baseline, Candidate: candidate,
		Configuration: &agentpb.WorkConfigurationReference{ConfigurationId: ids.New(ids.KindConfig),
			EnvironmentId: environment, Generation: 8, Sha256: bytes.Repeat([]byte{0x32}, 32)},
		Files: []*agentpb.MaterializeFile{{ArtifactId: candidate.ArtifactId,
			MaterializationId: ids.New(ids.KindConfig), EnvironmentId: environment, Destination: "config/settings.ini",
			Uid: 1000, Gid: 1000, Mode: 0o600, Length: uint64(len(content)), Sha256: digest[:],
			OutputKind: agentpb.MaterializationOutputKind_MATERIALIZATION_OUTPUT_KIND_SECRET_FILE}},
	}
	_, raw, reference, err := executionplan.CaptureEntryConfigurationWorkInput(ids.New(ids.KindPlan), procedure)
	clear(raw)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Now().UTC()
	attempt := workexecution.Attempt{ID: ids.New(ids.KindAttempt), WorkItemID: ids.New(ids.KindWorkItem),
		TaskID: ids.New(ids.KindTask), Number: 1, Direction: workexecution.Forward, Input: reference,
		Executor: workexecution.Identity{Executor: workexecution.Agent, HostID: ids.New(ids.KindHost),
			AgentID: ids.New(
				ids.KindAgent,
			), Generation: 1}, AssignedAt: at, StartedAt: &at, Deadline: at.Add(time.Minute)}
	progress, err := entryexecution.NewProgress(attempt, procedure, at)
	if err != nil {
		t.Fatal(err)
	}
	progress, err = progress.Begin(attempt, procedure, at)
	if err != nil {
		t.Fatal(err)
	}
	input := entryexecution.FileInput{Attempt: attempt, Procedure: procedure, Progress: progress}
	_, stage, _, err := entryexecution.ConfigurationSelection(input)
	if err != nil {
		t.Fatal(err)
	}
	header, err := entryexecution.DecodeConfigurationFile(input, &agentpb.EntryConfigurationFile{
		Destination: "config/settings.ini", OutputKind: "secret_file", Uid: 1000, Gid: 1000,
		Mode: 0o600, Length: uint64(len(content)), Sha256: digest[:]})
	if err != nil || header.StepID() != stage {
		t.Fatalf("invalid native file fixture: %v", err)
	}
	return input, header
}
