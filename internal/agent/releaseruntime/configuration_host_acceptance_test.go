//go:build agenthostacceptance

package releaseruntime

import (
	"bytes"
	"errors"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/agent/materialization"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/infra/agentownership"
	"google.golang.org/protobuf/proto"
)

// TASK-93 / SVC-15: the native restoration owner must restore the captured
// snapshot (or absence), only inspect a preserved candidate, and refuse partial
// restoration. Earlier runtime/quiescence observations and Controller receipts
// are supplied; process ownership, source validation and framing are real.
// Run only in the disposable networkless container described by the local gate.
func TestNativeRecoveryConfigurationRestoresOnlyCapturedFiles(t *testing.T) {
	if os.Getenv("GP_AGENT_WORKER_ACCEPTANCE") != "isolated-state" {
		t.Skip("requires disposable isolated Agent state; never run against host state")
	}
	for _, scenario := range []string{"restore", "preserve candidate", "no prior snapshot", "partial write",
		"verification failure", "missing receipt", "changed deadline", "wrong snapshot", "wrong destination", "corrupt content"} {
		t.Run(scenario, func(t *testing.T) {
			input := failureHookInput(t, scenario == "preserve candidate")
			input.Input.Release.Hooks = nil
			input.Input.Release.ConfigurationDestinations = []string{"config/application.yaml"}
			if scenario != "no prior snapshot" {
				input.Input.Release.PriorConfiguration = proto.CloneOf(input.Input.Release.Configuration)
				input.Input.Release.PriorConfiguration.ConfigurationId = ids.New(ids.KindConfig)
				input.Input.Release.PriorConfiguration.Sha256[0] ^= 1
			}
			if err := input.Validate(); err != nil {
				t.Fatal(err)
			}
			lease, err := agentownership.Acquire(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			owner, err := lease.Begin(input.Input.Attempt)
			if err != nil {
				t.Fatal(errors.Join(err, lease.Close()))
			}
			t.Cleanup(func() {
				if err := errors.Join(owner.Close(), lease.Close()); err != nil {
					t.Error(err)
				}
			})
			content := []byte("prior pinned configuration\n")
			if input.PreservesServing() {
				content = []byte("candidate pinned configuration\n")
			}
			if input.RecoveryConfigurationReference() == nil {
				content = nil
			}
			helper := &nativeConfigurationFileHelper{present: true, content: []byte("candidate pinned configuration\n")}
			if input.PreservesServing() {
				helper.content = bytes.Clone(content)
			}
			helper.failWrite, helper.failVerify = scenario == "partial write", scenario == "verification failure"
			materializer, err := materialization.New(helper, nil)
			if err != nil {
				t.Fatal(err)
			}
			exchange := &nativeConfigurationExchange{
				reference: input.RecoveryConfigurationReference(),
				content:   content,
				scenario:  scenario,
			}
			t.Cleanup(func() {
				for _, buffer := range exchange.buffers {
					if !bytes.Equal(buffer, make([]byte, len(buffer))) {
						t.Error("native recovery retained transient file plaintext")
					}
				}
			})
			source, err := NewChannelConfigurationSource(exchange)
			if err != nil {
				t.Fatal(err)
			}
			configuration, err := NewRecoveryConfiguration(materializer, source, lease)
			if err != nil {
				t.Fatal(err)
			}
			report := nativeConfigurationRecoveryBoundary(t, input, releaseexecution.RestoreConfiguration)
			if scenario == "missing receipt" {
				report.Ack = nil
			}
			if scenario == "changed deadline" {
				input.Input.Attempt.Deadline = input.Input.Attempt.Deadline.Add(time.Minute)
			}
			proof, err := configuration.Run(t.Context(), input, report, false)
			if slices.Contains(
				[]string{"missing receipt", "changed deadline", "wrong snapshot", "wrong destination"},
				scenario,
			) {
				if err == nil || len(helper.events) != 0 {
					t.Fatalf("ungranted file selection reached a helper: events=%v err=%v", helper.events, err)
				}
				return
			}
			if scenario == "corrupt content" {
				if err == nil || !slices.Equal(helper.events, []string{"stop", "stop"}) {
					t.Fatalf("changed plaintext reached the file writer: %v, %v", helper.events, err)
				}
				return
			}
			if scenario == "verification failure" {
				if err == nil || proof != (releaseexecution.RecoveryConfigurationProof{}) {
					t.Fatal("failed whole-file verification produced restoration proof")
				}
				return
			}
			if scenario == "partial write" {
				if err == nil || proof != (releaseexecution.RecoveryConfigurationProof{}) {
					t.Fatal("partial file write produced restoration proof")
				}
				if _, err := configuration.Run(t.Context(), input, report, true); err == nil {
					t.Fatal("inspection accepted the incomplete restored file")
				}
				proof, err = configuration.Run(t.Context(), input, report, false)
			}
			if err != nil || proof.Validate(input) != nil || helper.present != (content != nil) ||
				!bytes.Equal(helper.content, content) {
				t.Fatalf("captured restoration did not complete: %v", err)
			}
			if input.PreservesServing() && !slices.Equal(helper.events, []string{"stop", "stop", "verify"}) {
				t.Fatalf("preserving a serving candidate wrote its files: %v", helper.events)
			}
			// Runtime restoration can have happened since the previous file
			// stage. The final verification is read-only and must detect drift.
			verify := nativeConfigurationRecoveryBoundary(t, input, releaseexecution.VerifyRestoration)
			if _, err := configuration.Run(t.Context(), input, verify, true); err != nil {
				t.Fatal(err)
			}
			helper.content, helper.present = []byte("unrelated changed bytes\n"), true
			if _, err := configuration.Run(t.Context(), input, verify, true); err == nil {
				t.Fatal("final verification accepted configuration drift")
			}
		})
	}
}

// Earlier phases are supplied acknowledgements, not host effects. This case
// exercises only the acknowledged file boundary and does not qualify a complete
// runtime recovery, Controller journal or terminal-state publication.
func nativeConfigurationRecoveryBoundary(t *testing.T, input releaseexecution.RecoveryInput,
	phase releaseexecution.RecoveryPhase,
) releaseexecution.RecoveryReport {
	t.Helper()
	progress, err := releaseexecution.NewRecoveryProgress(input, *input.Input.Attempt.StartedAt)
	if err != nil {
		t.Fatal(err)
	}
	for progress.Phase != releaseexecution.RecoveryComplete {
		progress, err = progress.Begin(input, time.Now().UTC())
		if err != nil {
			t.Fatal(err)
		}
		if progress.Phase == phase {
			stage, err := releaseexecution.RecoveryStageID(input.Input.Attempt.Input, phase)
			if err != nil {
				t.Fatal(err)
			}
			report := releaseexecution.RecoveryReport{Sequence: progress.Sequence, StageID: stage, Progress: progress}
			ack, err := report.Checkpoint(input)
			if err != nil {
				t.Fatal(err)
			}
			report.Ack = &ack
			return report
		}
		progress, err = progress.Confirm(input, input.Forward.Input, time.Now().UTC())
		if err != nil {
			t.Fatal(err)
		}
	}
	t.Fatal("configuration recovery phase was not reachable")
	return releaseexecution.RecoveryReport{}
}
