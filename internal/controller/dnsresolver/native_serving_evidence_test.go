package dnsresolver

import (
	"bytes"
	"crypto/sha512"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/dnsproof"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/resolverexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/components"
	"github.com/AlanD20/groundplane/internal/infra/etcd/resolverwork"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func nativeResolverEvidenceFixture(
	t *testing.T,
	enabled, previous bool,
) (resolverexecution.Input, workexecution.Attempt) {
	t.Helper()
	planner, environment := nativeResolverPlanner(t)
	component, request := nativeResolverComponent(), nativeResolverRequest()
	first, _, err := planner.prepareNativeConfiguration(component, nativeResolverSelected(t), request, 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	input := resolverexecution.Input{TaskID: request.TaskID, WorkID: request.WorkID,
		HostID: request.HostID, OperationID: request.OperationID, ComponentID: component.ID,
		Generation: 1, BaselineSHA256: strings.Repeat("b", 64), CreatedAt: request.CreatedAt,
		Deadline: request.Deadline, Enabled: enabled, EnsureRuntime: enabled,
	}
	if previous {
		input.Previous, input.PreviousEnabled, input.Generation = &first, true, 2
		input.PreviousFileOwner = "mct_" + strings.Repeat("a", 64)
	}
	if enabled {
		input.Candidate = &first
		if previous {
			environment.image = "example/new-resolver"
			request.PlanID = ids.New(ids.KindPlan)
			candidate, _, err := planner.prepareNativeConfiguration(
				component,
				nativeResolverSelected(t),
				request,
				2,
				&first,
			)
			if err != nil {
				t.Fatal(err)
			}
			input.Candidate = &candidate
		}
	}
	raw, reference, err := resolverexecution.Capture(request.PlanID, input)
	clear(raw)
	if err != nil {
		t.Fatal(err)
	}
	started := input.CreatedAt.Add(time.Millisecond)
	attempt := workexecution.Attempt{ID: ids.New(ids.KindAttempt), TaskID: input.TaskID, WorkItemID: input.WorkID,
		Number: 1, Direction: workexecution.Forward, Input: reference,
		Executor: workexecution.Identity{Executor: workexecution.Agent, HostID: input.HostID,
			AgentID: ids.New(ids.KindAgent), Generation: 1},
		AssignedAt: input.CreatedAt, StartedAt: &started, Deadline: input.Deadline,
	}
	if err := attempt.Validate(); err != nil {
		t.Fatal(err)
	}
	return input, attempt
}

// Observations are supplied, not actual DNS traffic. Bind them to independent
// expected query/answer/counter data and the planner's captured configuration.
func nativeResolverEvidence(
	t *testing.T,
	configuration resolverexecution.Configuration,
	at time.Time,
) *agentpb.DNSResolverObservationEvidence {
	t.Helper()
	service := configuration.Runtime.Services[0]
	reload := sha512.Sum512(configuration.Corefile)
	proof := &agentpb.DNSResolverObservationEvidence{
		ComponentId: configuration.Apply.ComponentId, ServiceId: service.ServiceId,
		ArtifactId: configuration.Apply.ArtifactId, ArtifactSha256: bytes.Clone(configuration.Apply.ArtifactDigest),
		RenderGeneration: configuration.Generation, ImageReference: service.ImageReference,
		ImageRepository: service.ImageRepository, ImageIndexDigest: bytes.Clone(service.ImageIndexDigest),
		VerifiedImageDigest: bytes.Clone(
			service.ImageChildDigest,
		), ImageConfigDigest: bytes.Clone(service.ImageConfigDigest),
		ImageOs: service.ImageOs, ImageArchitecture: service.ImageArchitecture, ImageVariant: service.ImageVariant,
		ListenEndpoint: "127.0.0.1:53", ReloadSha512: reload[:], ObservedAt: timestamppb.New(at),
		CatchAllQuery: &agentpb.DNSQueryProof{Name: ".", Type: agentpb.DNSQueryType_DNS_QUERY_TYPE_NS,
			RecursionAvailable: true, SelectedUpstream: "1.1.1.1:53", Attempts: 1,
			Answers: []*agentpb.DNSAnswerRecord{{OwnerName: ".", Type: agentpb.DNSQueryType_DNS_QUERY_TYPE_NS,
				NameServer: "a.root-servers.net."}},
			Counters: []*agentpb.DNSForwardCounter{{Upstream: "1.1.1.1:53", Before: 1, After: 2}},
		},
	}
	if err := dnsproof.Seal(proof); err != nil {
		t.Fatal(err)
	}
	return proof
}

// TASK-112: hashing one's own report cannot substitute another image/Corefile,
// another Service or an empty DNS query for proof of the captured configuration.
func TestNativeResolverRejectsSelfHashedForeignServingEvidence(t *testing.T) {
	input, attempt := nativeResolverEvidenceFixture(t, true, true)
	proof := nativeResolverEvidence(t, *input.Candidate, *attempt.StartedAt)
	if err := resolverexecution.ValidateServingProof(*input.Candidate, proof); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"corefile", "service", "image", "empty-query"} {
		t.Run(name, func(t *testing.T) {
			altered := proto.CloneOf(proof)
			switch name {
			case "corefile":
				altered.ArtifactSha256 = bytes.Repeat([]byte{9}, 32)
			case "service":
				altered.ServiceId = ids.New(ids.KindService)
			case "image":
				altered.ImageConfigDigest = bytes.Repeat([]byte{9}, 32)
			case "empty-query":
				altered.CatchAllQuery = &agentpb.DNSQueryProof{}
			}
			digest, err := dnsproof.Digest(altered)
			if err != nil {
				t.Fatal(err)
			}
			altered.ProofSha256 = digest[:]
			if err := resolverexecution.ValidateServingProof(*input.Candidate, altered); err == nil {
				t.Fatal("a self-hashed report replaced captured serving authority")
			}
		})
	}
}

// TASK-112: exact canonical query evidence belongs to applied resource state,
// not a reconstructed Task summary. This store has no Task history to consult.
func TestNativeResolverAppliedStateRetainsCanonicalQueryEvidence(t *testing.T) {
	input, attempt := nativeResolverEvidenceFixture(t, true, true)
	proof := nativeResolverEvidence(t, *input.Candidate, *attempt.StartedAt)
	proof.StaticQuery = &agentpb.DNSQueryProof{Name: "app.internal.", Type: agentpb.DNSQueryType_DNS_QUERY_TYPE_A,
		Attempts: 1, Answers: []*agentpb.DNSAnswerRecord{{OwnerName: "app.internal.",
			Type: agentpb.DNSQueryType_DNS_QUERY_TYPE_A, Ipv4: []byte{10, 20, 0, 2}}}}
	if err := dnsproof.Seal(proof); err != nil {
		t.Fatal(err)
	}
	expected := proto.CloneOf(proof)
	component := nativeResolverComponent()
	component.ID = input.ComponentID
	record, err := components.NewRecord(component)
	if err != nil {
		t.Fatal(err)
	}
	applied := resolverwork.Applied{HostID: input.HostID, ComponentID: input.ComponentID,
		Generation: input.Generation, Desired: record.Desired, Configuration: input.Candidate,
		FileOwner: "mct_" + strings.Repeat("a", 64), BaselineSHA256: input.BaselineSHA256,
		ProjectionSHA256: strings.Repeat("c", 64), ServingProof: proof, ObservedAt: proof.ObservedAt.AsTime()}
	raw, err := resolverwork.EncodeApplied(applied)
	if err != nil {
		t.Fatal(err)
	}
	proof.StaticQuery.Name = "changed.internal."
	if _, err := resolverwork.EncodeApplied(applied); err == nil {
		t.Fatal("resource state accepted mutated evidence without its canonical proof")
	}
	store := nativeResolverReadStore{
		values: map[string][]byte{resolverwork.AppliedKey(input.HostID, input.ComponentID): raw},
	}
	read, found, err := resolverwork.ReadApplied(t.Context(), store, input.HostID, input.ComponentID, 42)
	if err != nil || !found || !proto.Equal(read.Record.ServingProof, expected) {
		t.Fatalf("resource read changed or omitted canonical query evidence: %v", err)
	}
	read.Record.ServingProof.StaticQuery.Answers[0].Ipv4[3] = 9
	again, found, err := resolverwork.ReadApplied(t.Context(), store, input.HostID, input.ComponentID, 42)
	if err != nil || !found || !proto.Equal(again.Record.ServingProof, expected) {
		t.Fatalf("decoded proof mutation changed persisted evidence: %v", err)
	}
}

// TASK-112: completion needs the final cursor AND its acknowledged DNS-stage
// evidence; disabling proves absence rather than claiming a running resolver.
func TestNativeResolverCompletionRequiresAcknowledgedServingEvidence(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		input, attempt := nativeResolverEvidenceFixture(t, enabled, true)
		layout, err := resolverexecution.CaptureLayout(attempt, input)
		if err != nil {
			t.Fatal(err)
		}
		at := *attempt.StartedAt
		progress, err := resolverexecution.NewProgress(attempt, layout, at)
		if err != nil {
			t.Fatal(err)
		}
		var verification *resolverexecution.Progress
		var proof *agentpb.DNSResolverObservationEvidence
		for progress.Stage != resolverexecution.Complete {
			at = at.Add(time.Millisecond)
			progress, err = progress.Begin(attempt, layout, at)
			if err != nil {
				t.Fatal(err)
			}
			observation := resolverexecution.Observation{Input: layout.Input, ComponentID: input.ComponentID,
				Stage: progress.Stage, At: at}
			switch progress.Stage {
			case resolverexecution.WriteConfiguration:
				observation.ConfigurationSHA256 = layout.ConfigurationSHA256
			case resolverexecution.ReconcileRuntime:
				observation.RuntimePresent = true
			case resolverexecution.VerifyDNS:
				proof = nativeResolverEvidence(t, *input.Candidate, at)
				observation, err = resolverexecution.ServingObservation(attempt, input, proof)
				if err != nil {
					t.Fatal(err)
				}
			case resolverexecution.ApplyHostResolver:
				observation.HostResolver = "local_resolver"
			case resolverexecution.RestoreHostResolver:
				observation.HostResolver = "baseline"
			case resolverexecution.RemoveRuntime, resolverexecution.VerifyAbsent:
				observation.RuntimeAbsent = true
			}
			confirmed := progress.Stage
			progress, err = progress.Confirm(attempt, layout, observation, at)
			if err != nil {
				t.Fatal(err)
			}
			if confirmed == resolverexecution.VerifyDNS {
				checkpoint := progress
				verification = &checkpoint
			}
		}
		if _, err := resolverexecution.CompletionEvidence(attempt, input, progress, verification, proof); err != nil {
			t.Fatalf("valid completion enabled=%t: %v", enabled, err)
		}
		if enabled {
			if _, err := resolverexecution.CompletionEvidence(attempt, input, progress, nil, proof); err == nil {
				t.Fatal("completion accepted an unacknowledged DNS observation")
			}
			if _, err := resolverexecution.CompletionEvidence(attempt, input, progress, verification, nil); err == nil {
				t.Fatal("completion accepted missing candidate serving proof")
			}
		} else if _, err := resolverexecution.CompletionEvidence(attempt, input, progress, nil,
			nativeResolverEvidence(t, *input.Previous, at)); err == nil {
			t.Fatal("disabled completion claimed a running serving resolver")
		}
	}
}

// TASK-113: completed cleanup needs the exact predecessor's image/configuration
// proof. Unresolved cleanup is reportable without pretending restoration worked.
func TestNativeResolverRecoveryRequiresExactPredecessorEvidence(t *testing.T) {
	for _, mode := range []string{"update", "disable", "first-enable"} {
		t.Run(mode, func(t *testing.T) {
			input, forward := nativeResolverEvidenceFixture(t, mode != "disable", mode != "first-enable")
			layout, err := resolverexecution.CaptureLayout(forward, input)
			if err != nil {
				t.Fatal(err)
			}
			failedAt := forward.StartedAt.Add(time.Second)
			failure := resolverexecution.Failure{AttemptID: forward.ID, Input: forward.Input,
				Reason: resolverexecution.ExecutionFailed, RecordedAt: failedAt}
			_, evidence, err := resolverexecution.EncodeFailure(forward, layout, failure)
			if err != nil {
				t.Fatal(err)
			}
			forward.FinishedAt, forward.Outcome, forward.Result = &failedAt, workexecution.Failed, &evidence
			source := resolverexecution.Recovery{Forward: forward, Failure: failure, Evidence: evidence}
			attempt := forward
			started := failedAt.Add(time.Millisecond)
			attempt.ID, attempt.Number, attempt.Direction = ids.New(ids.KindAttempt), 2, workexecution.Recovery
			attempt.AssignedAt, attempt.StartedAt, attempt.FinishedAt = failedAt, &started, nil
			attempt.Outcome, attempt.Result = "", nil
			report := resolverexecution.RecoveryReport{AttemptID: attempt.ID, ForwardID: forward.ID,
				Input: attempt.Input, Failure: evidence, Status: resolverexecution.RecoveryComplete,
				HelpersStopped: true, ConfigurationRestored: true, RuntimeRestored: true, HostResolverRestored: true,
				ObservedAt: started.Add(time.Second)}
			if input.PreviousEnabled {
				report.ServingProof = nativeResolverEvidence(t, *input.Previous, started)
			}
			if _, _, err := resolverexecution.EncodeRecoveryReport(attempt, input, source, report); err != nil {
				t.Fatalf("exact captured restoration rejected: %v", err)
			}
			wrong := report
			if input.PreviousEnabled {
				wrong.ServingProof = nil
			} else {
				wrong.ServingProof = nativeResolverEvidence(t, *input.Candidate, started)
			}
			if _, _, err := resolverexecution.EncodeRecoveryReport(attempt, input, source, wrong); err == nil {
				t.Fatal("cleanup accepted missing or impossible predecessor serving evidence")
			}
			if input.PreviousEnabled && input.Candidate != nil {
				wrong.ServingProof = nativeResolverEvidence(t, *input.Candidate, started)
				if _, _, err := resolverexecution.EncodeRecoveryReport(attempt, input, source, wrong); err == nil {
					t.Fatal("cleanup substituted the candidate image for the predecessor")
				}
			}
			unresolved := resolverexecution.RecoveryReport{AttemptID: attempt.ID, ForwardID: forward.ID,
				Input: attempt.Input, Failure: evidence, Status: resolverexecution.RecoveryFailed, ObservedAt: report.ObservedAt}
			if _, _, err := resolverexecution.EncodeRecoveryReport(attempt, input, source, unresolved); err != nil {
				t.Fatalf("unresolved cleanup became a protocol failure: %v", err)
			}
		})
	}
}
