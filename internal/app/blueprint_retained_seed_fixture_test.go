package app

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testreleaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	testreleases "github.com/AlanD20/groundplane/internal/infra/etcd/releases"
)

// SeedRetainedRollback uses real staging codecs to model the durable native
// deploy B/3 and successful rollback C/2 heads without advancing Blueprint A.
func (fixture *ExecutedArtifactFixture) SeedRetainedRollback(
	t *testing.T,
	original testreleaserender.ReleaseRenderInput,
	originalIntent domain.Intent,
) (testreleaserender.ReleaseRenderInput, domain.Intent) {
	t.Helper()
	ctx := context.Background()
	priorID := original.ReleaseID
	var result testreleaserender.ReleaseRenderInput
	var intent domain.Intent
	for index, replicas := range []uint32{3, 2} {
		result = testreleaserender.CloneReleaseRenderInput(original)
		result.ReleaseID, result.PlanID, result.ArtifactID = ids.New(
			ids.KindDeployment,
		), ids.New(
			ids.KindPlan,
		), ids.New(
			ids.KindConfig,
		)
		result.CandidateWorkload.ReplicaCount = replicas
		if len(result.ProxyPorts) > 0 {
			config, err := domain.RenderProxyConfig(
				result.ServiceName,
				result.ReleaseID,
				result.CandidateTarget,
				result.ProxyGeneration,
				result.ProxyPorts,
			)
			if err != nil {
				t.Fatal(err)
			}
			result.ProxyConfigDigest = hex.EncodeToString(config.SHA256[:])
		}
		intent = originalIntent
		intent.ID, intent.OperationID, intent.OriginatingTaskID = result.ReleaseID, ids.New(
			ids.KindOperation,
		), ids.New(
			ids.KindTask,
		)
		intent.RenderInputID, intent.CandidateWorkload = result.ArtifactID, result.CandidateWorkload
		intent.PriorServingReleaseID, intent.OperationKind = priorID, domain.OperationDeploy
		if index == 1 {
			intent.OperationKind, intent.RollbackSourceReleaseID = domain.OperationRollback, original.ReleaseID
		}
		raw, err := testreleaserender.EncodeReleaseRenderInput(result)
		if err != nil {
			t.Fatal(err)
		}
		intent.RenderInputDigest, err = domain.Digest(json.RawMessage(raw))
		if err != nil {
			t.Fatal(err)
		}
		publication := ids.NewULID()
		manifest, err := fixture.Ledger.Stage(
			ctx, testreleases.ReleaseStage{
				PublicationID: publication,
				OperationID:   intent.OperationID,
				CreatedAt:     intent.CreatedAt,
				Members: []testreleases.ReleaseStageMember{
					{
						Intent:      intent,
						RenderInput: raw,
						Checkpoint: domain.Checkpoint{
							ReleaseID: intent.ID,
							State:     domain.StatePending,
							UpdatedAt: intent.CreatedAt,
						},
					},
				},
			},
		)
		if err != nil {
			t.Fatal(err)
		}
		marker, _ := testreleases.EncodeReleaseRecord(
			"release-publication", testreleases.ReleasePublicationMarker{
				PublicationID:  publication,
				OperationID:    intent.OperationID,
				ManifestDigest: manifest.Record.Digest,
				PublishedAt:    intent.CreatedAt,
			},
		)
		indexBytes, _ := json.Marshal(testreleases.ReleaseServiceIndexValue{Schema: 1, PublicationID: publication})
		projection, _ := testreleases.EncodeReleaseRecord(
			"service-release-projection",
			domain.ServiceProjection{
				EnvironmentID:              intent.EnvironmentID,
				ServiceID:                  intent.ServiceID,
				ServingReleaseID:           intent.ID,
				CurrentSuccessfulReleaseID: intent.ID,
			},
		)
		head, _ := testreleases.EncodeReleaseRecord(
			"release-operation", testreleases.ReleaseOperationHead{
				OperationID:   intent.OperationID,
				PublicationID: publication,
				EnvironmentID: intent.EnvironmentID,
				LatestTaskID:  intent.OriginatingTaskID,
				State:         domain.StateCompleted,
				Attempts: []domain.Attempt{
					{ID: intent.OriginatingTaskID, TaskID: intent.OriginatingTaskID, StartedAt: intent.CreatedAt},
				},
			},
		)
		_, err = fixture.store.Transact(
			ctx,
			nil,
			[]testkeyvalue.Mutation{
				{Type: testkeyvalue.MutationPut, Key: testreleases.ReleasePublicationKey(publication), Value: marker},
				{
					Type:  testkeyvalue.MutationPut,
					Key:   testreleases.ReleaseServiceIndexKey(intent.EnvironmentID, intent.ServiceID, intent.ID),
					Value: indexBytes,
				},
				{
					Type:  testkeyvalue.MutationPut,
					Key:   testreleases.ReleaseProjectionKey(intent.ServiceID),
					Value: projection,
				},
				{
					Type:  testkeyvalue.MutationPut,
					Key:   testreleases.ReleaseOperationKey(intent.OperationID),
					Value: head,
				},
			},
		)
		if err != nil {
			t.Fatal(err)
		}
		priorID = intent.ID
	}
	return result, intent
}

// Rationale: publication must atomically reject a replaced or pruned inactive
// render, including all mutations already carried by its candidate fragment.

// SVC-15/BP-04: Rationale: source-only authority must reach the actual owning publisher and
// compare both acknowledged artifact and absent Release projection atomically.
