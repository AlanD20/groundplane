package etcd

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testreleaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	testreleases "github.com/AlanD20/groundplane/internal/infra/etcd/releases"
)

// Rationale: even a consistently resealed publication cannot authorize a foreign
// originating attempt, operation kind, or predecessor image; real retries retain
// their original immutable intent rather than pretending to be a fresh deploy.
func TestOrdinaryRecoveryIntentLineageAndPriorImage(t *testing.T) {
	for _, variation := range []string{"original", "retry", "foreign-origin", "foreign-kind", "foreign-image", "foreign-release", "foreign-target", "foreign-replicas", "broken-retry"} {
		t.Run(variation, func(t *testing.T) {
			ctx := context.Background()
			f, assignment, _, revision, renderKey := recoveryProofFixture(t, false)
			task := f.claim.Task.Record
			publication := task.Params[testreleaserender.TaskReleasePublicationParam]
			keys := []string{
				testreleases.ReleaseIntentStagingKey(publication, f.releaseID),
				renderKey,
				testreleases.ReleaseOperationKey(task.OperationID),
			}
			read, err := f.repository.store.GetMany(ctx, testkeyvalue.GetManyRequest{Keys: keys, Revision: revision})
			if err != nil {
				t.Fatal(err)
			}
			intent, err := testreleases.DecodeReleaseRecord[domain.Intent](read.Values[0].Value, "release-intent")
			if err != nil {
				t.Fatal(err)
			}
			raw, err := testreleases.DecodeReleaseRecord[json.RawMessage](read.Values[1].Value, "release-render-input")
			if err != nil {
				t.Fatal(err)
			}
			render, err := testreleaserender.DecodeReleaseRenderInput(raw)
			if err != nil {
				t.Fatal(err)
			}
			head, err := testreleases.DecodeReleaseRecord[testreleases.ReleaseOperationHead](
				read.Values[2].Value,
				"release-operation",
			)
			if err != nil {
				t.Fatal(err)
			}
			origin := ids.NewAt(ids.KindTask, f.now, 211)
			switch variation {
			case "retry", "broken-retry":
				intent.OriginatingTaskID, task.RetryOf = origin, origin
				head.Attempts = []domain.Attempt{
					{ID: origin, TaskID: origin, StartedAt: f.now},
					{ID: task.ID, TaskID: task.ID, RetryOf: origin, StartedAt: f.now},
				}
				if variation == "broken-retry" {
					head.Attempts[1].RetryOf = ""
				}
			case "foreign-origin":
				intent.OriginatingTaskID = origin
			case "foreign-kind":
				intent.OperationKind = domain.OperationRollback
			case "foreign-release":
				intent.PriorServingReleaseID = f.releaseID
			case "foreign-target":
				render.PriorStrategy, render.PriorTarget, render.PriorSlot = domain.StrategyBlueGreen, domain.WorkloadBlue, domain.SlotBlue
				render.PriorWorkload.ReplicaCount = 1
				render.CandidateWorkload.ReplicaCount = 1
				intent.CandidateWorkload = render.CandidateWorkload
			case "foreign-replicas":
				render.PriorWorkload.ReplicaCount++
			case "foreign-image":
				render.PriorWorkload.LocalImageID = render.CandidateWorkload.LocalImageID
				if render.PriorWorkload.LocalImageID == "" {
					t.Fatal("candidate image absent")
				}
			}
			raw, err = testreleaserender.EncodeReleaseRenderInput(render)
			if err != nil {
				t.Fatal(err)
			}
			intent.RenderInputDigest, err = domain.Digest(raw)
			if err != nil {
				t.Fatal(err)
			}
			member := testreleases.ReleaseStagedMemberRef{
				ServiceID:    f.serviceID,
				ReleaseID:    f.releaseID,
				RenderDigest: intent.RenderInputDigest,
			}
			member.IntentDigest, err = domain.Digest(intent)
			if err != nil {
				t.Fatal(err)
			}
			intentBytes, err := testreleases.EncodeReleaseRecord("release-intent", intent)
			if err != nil {
				t.Fatal(err)
			}
			renderBytes, err := testreleases.EncodeReleaseRecord("release-render-input", raw)
			if err != nil {
				t.Fatal(err)
			}
			headBytes, err := testreleases.EncodeReleaseRecord("release-operation", head)
			if err != nil {
				t.Fatal(err)
			}
			txn, err := f.repository.store.Transact(ctx, nil, []testkeyvalue.Mutation{
				{Type: testkeyvalue.MutationPut, Key: keys[0], Value: intentBytes},
				{Type: testkeyvalue.MutationPut, Key: keys[1], Value: renderBytes},
				{Type: testkeyvalue.MutationPut, Key: keys[2], Value: headBytes},
			})
			if err != nil {
				t.Fatal(err)
			}
			kind, conditions, err := f.repository.ordinaryRecoveryProofKindAtRevision(
				ctx,
				task,
				assignment,
				member,
				txn.Revision,
			)
			valid := variation == "original" || variation == "retry"
			if !valid {
				if err == nil {
					t.Fatal("foreign or broken recovery inputs accepted")
				}
				return
			}
			if err != nil || kind.kind != releaseRecoveryProofRecreate ||
				kind.priorTopologyArtifactID != render.PriorArtifactID ||
				len(conditions) != 3 {
				t.Fatalf("valid lineage rejected: %v", err)
			}
		})
	}
}
