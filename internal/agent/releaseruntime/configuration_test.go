package releaseruntime

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/agent/checkpointdelivery"
	"github.com/AlanD20/groundplane/internal/agent/materialization"
	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/agentworkjournal"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// TASK-94 / SVC-15: private files require an acknowledged forward boundary;
// partial/failed verification cannot produce evidence. Lost effect ACK replay
// after journal reopen resends exact evidence, not a second file mutation.
// Controller replies and the physical file helper are supplied, not live hosts.
func TestNativeReleaseConfigurationRequiresReceiptAndReplaysEvidence(t *testing.T) {
	for _, scenario := range []string{"success", "missing receipt", "partial write", "verification failure", "lost effect acknowledgement"} {
		t.Run(scenario, func(t *testing.T) {
			recovery := failureHookInput(t, false)
			input := recovery.Input
			input.Attempt = recovery.Forward
			input.Attempt.FinishedAt, input.Attempt.Result, input.Attempt.Outcome = nil, nil, ""
			input.Release.Hooks = nil
			input.Release.ConfigurationDestinations = []string{"config/application.yaml"}
			path := filepath.Join(t.TempDir(), "journal")
			journal, err := agentworkjournal.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := journal.Close(); err != nil {
					t.Error(err)
				}
			})
			checkpoints, err := checkpointdelivery.NewRelease(
				input,
				journal,
				func(_ context.Context, request *agentpb.ReleaseWorkCheckpointRequest) (*agentpb.ReleaseWorkCheckpointAck, error) {
					report, err := releaseexecution.DecodeCheckpointRequest(input, request)
					if err != nil {
						return nil, err
					}
					return releaseexecution.EncodeCheckpointAck(input, report)
				},
			)
			if err != nil {
				t.Fatal(err)
			}
			progress, err := releaseexecution.NewProgress(input, *input.Attempt.StartedAt)
			if err != nil {
				t.Fatal(err)
			}
			progress, err = progress.Begin(input, time.Now().UTC())
			if err != nil {
				t.Fatal(err)
			}
			if err := checkpoints.Checkpoint(t.Context(), progress); err != nil {
				t.Fatal(err)
			}
			report, err := checkpoints.Current(t.Context())
			if err != nil || report == nil {
				t.Fatalf("missing durable file boundary: %v", err)
			}
			if scenario == "missing receipt" {
				report.Ack = nil
			}
			helper := &nativeConfigurationFileHelper{failWrite: scenario == "partial write",
				failVerify: scenario == "verification failure"}
			materializer, err := materialization.New(helper, nil)
			if err != nil {
				t.Fatal(err)
			}
			exchange := &nativeConfigurationExchange{reference: input.Release.Configuration,
				content: []byte("candidate pinned configuration\n")}
			t.Cleanup(func() {
				for _, buffer := range exchange.buffers {
					if !bytes.Equal(buffer, make([]byte, len(buffer))) {
						t.Error("forward file delivery retained private bytes")
					}
				}
			})
			source, err := NewChannelConfigurationSource(exchange)
			if err != nil {
				t.Fatal(err)
			}
			lost := errors.New("effect acknowledgement lost")
			var sent [][]byte
			acknowledge := func(_ context.Context, _ *agentpb.WorkAttemptIdentity, raw []byte, reference workexecution.Input) (workexecution.Input, error) {
				if _, err := releaseexecution.DecodeEffectEvidence(input, *report, raw, reference); err != nil {
					return workexecution.Input{}, err
				}
				sent = append(sent, bytes.Clone(raw))
				if scenario == "lost effect acknowledgement" && len(sent) == 1 {
					return workexecution.Input{}, lost
				}
				return reference, nil
			}
			newConfiguration := func() *Configuration {
				evidence, err := checkpointdelivery.NewEffect(input, journal, acknowledge)
				if err != nil {
					t.Fatal(err)
				}
				configuration, err := NewConfiguration(materializer, source, evidence)
				if err != nil {
					t.Fatal(err)
				}
				return configuration
			}
			proof, err := newConfiguration().Apply(t.Context(), input, *report)
			if scenario == "missing receipt" || scenario == "partial write" || scenario == "verification failure" {
				if err == nil || proof != (workexecution.Input{}) || len(sent) != 0 {
					t.Fatal("ungranted/incomplete files published effect evidence")
				}
				if scenario == "missing receipt" && len(helper.events) != 0 {
					t.Fatal("a file helper ran before durable receipt")
				}
				return
			}
			if scenario == "lost effect acknowledgement" {
				if !errors.Is(err, lost) || !slices.Equal(helper.events, []string{"write", "verify"}) {
					t.Fatalf("lost acknowledgement did not preserve complete file effects: %v", err)
				}
				if err := journal.Close(); err != nil {
					t.Fatal(err)
				}
				journal, err = agentworkjournal.Open(path)
				if err != nil {
					t.Fatal(err)
				}
				proof, err = newConfiguration().Apply(t.Context(), input, *report)
				if len(sent) != 2 || !bytes.Equal(sent[0], sent[1]) {
					t.Fatal("lost ACK replay did not deliver the exact saved evidence")
				}
			}
			if err != nil || proof.Validate() != nil || !slices.Equal(helper.events, []string{"write", "verify"}) {
				t.Fatalf("file execution repeated or lost its evidence: %v, %v", helper.events, err)
			}
			saved, err := journal.Effect(t.Context(), input, *report)
			if err != nil || saved == nil || !saved.Acknowledged || saved.Reference != proof {
				t.Fatalf("configuration evidence has no durable receipt: %v", err)
			}
		})
	}
}
