package releasequeries

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releases"
)

// TASK-133/SVC-09: native publication must remain readable by both history and
// intent selection. The previous readers expected a record no current publisher
// writes. A valid marker for another publication/operation/resource cannot make
// this Release visible. This uses actual codecs/readers, not atomic publication.
func TestNativePublicationReadersBindExactReleaseOwners(t *testing.T) {
	intent, publication := nativePublicationFixture(t)
	for _, change := range []string{"unchanged", "publication", "operation", "environment", "service", "missing marker", "corrupt marker"} {
		t.Run(change, func(t *testing.T) {
			marker := publication
			marker.NativePredecessors = append(
				[]releases.BlueprintNativePredecessorReference(nil),
				publication.NativePredecessors...)
			switch change {
			case "publication":
				marker.PublicationID = ids.NewULID()
			case "operation":
				marker.OperationID = ids.New(ids.KindOperation)
			case "environment":
				marker.EnvironmentID = ids.New(ids.KindEnvironment)
			case "service":
				marker.ServiceID = ids.New(ids.KindService)
				marker.NativePredecessors[0].ServiceID = marker.ServiceID
			}
			encoded, err := releases.EncodeWorkPublication(marker)
			if err != nil {
				t.Fatal(err)
			}
			if change == "corrupt marker" {
				encoded = []byte("incomplete publication")
			}
			checkpoint := domain.Checkpoint{
				ReleaseID: intent.ID,
				State:     domain.StatePending,
				UpdatedAt: intent.CreatedAt,
			}
			intentBytes, err := releases.EncodeReleaseRecord("release-intent", intent)
			if err != nil {
				t.Fatal(err)
			}
			checkpointBytes, err := releases.EncodeReleaseRecord("release-checkpoint", checkpoint)
			if err != nil {
				t.Fatal(err)
			}
			store := &nativePublicationReadStore{t: t, values: map[string][]byte{
				releases.ReleaseIntentStagingKey(publication.PublicationID, intent.ID):     intentBytes,
				releases.ReleaseCheckpointStagingKey(publication.PublicationID, intent.ID): checkpointBytes,
				releases.ReleasePublicationKey(publication.PublicationID):                  encoded,
			}}
			if change == "missing marker" {
				delete(store.values, releases.ReleasePublicationKey(publication.PublicationID))
			}
			reader := NewReader(store)
			selected, intentErr := reader.GetIntent(t.Context(), publication.PublicationID, intent.ID)
			view, historyErr := reader.ReadViewAt(t.Context(), publication.PublicationID, intent.ID, 42)
			if change != "unchanged" {
				if intentErr == nil || historyErr == nil || selected.Record.ID != "" || view.Intent.ID != "" {
					t.Fatalf(
						"unrelated or unavailable publication became visible: intent=%v history=%v",
						intentErr,
						historyErr,
					)
				}
				return
			}
			if intentErr != nil || historyErr != nil || selected.Record.ID != intent.ID ||
				selected.Record.OperationID != intent.OperationID || selected.Revision != 40 || selected.ReadRevision != 42 ||
				view.Intent.ID != intent.ID || view.Checkpoint.State != domain.StatePending || view.Revision != 42 {
				t.Fatalf(
					"native Release could not be read: selected=%+v view=%+v intent=%v history=%v",
					selected,
					view,
					intentErr,
					historyErr,
				)
			}
			if len(store.revisions) != 3 || store.revisions[0] != 0 || store.revisions[1] != 42 ||
				store.revisions[2] != 42 {
				t.Fatalf("history read escaped its fixed snapshot: %v", store.revisions)
			}
		})
	}
}

func nativePublicationFixture(t *testing.T) (domain.Intent, releases.WorkPublication) {
	t.Helper()
	at := time.Now().UTC()
	environment, service := ids.New(ids.KindEnvironment), ids.New(ids.KindService)
	intent := domain.Intent{ID: ids.New(ids.KindDeployment), EnvironmentID: environment, ServiceID: service,
		OperationID: ids.New(ids.KindOperation), OperationKind: domain.OperationBlueprintApply,
		CandidateWorkload: domain.WorkloadSeal{
			RequestedReference: "nginx:latest",
			LocalImageID:       "sha256:" + strings.Repeat("a", 64),
			ReplicaCount:       1,
		},
		Tag: "latest", Strategy: domain.StrategyRecreate, OnFailure: domain.OnFailureSwitchBack,
		RenderInputID: ids.New(
			ids.KindConfig,
		), RenderInputDigest: strings.Repeat("b", 64), CreatedAt: at, Actor: "operator",
		OriginatingTaskID: ids.New(ids.KindTask), Workspace: domain.Workspace{Kind: domain.WorkspaceTenant,
			TenantID: ids.New(ids.KindTenant), ProjectID: ids.New(ids.KindProject), EnvironmentID: environment}}
	if err := domain.ValidateIntent(intent); err != nil {
		t.Fatal(err)
	}
	publication := releases.WorkPublication{PublicationID: ids.NewULID(), OperationID: intent.OperationID,
		TaskID: intent.OriginatingTaskID, WorkItemID: ids.New(ids.KindWorkItem), EnvironmentID: environment, ServiceID: service,
		ManifestDigest: strings.Repeat(
			"c",
			64,
		), Input: workexecution.Input{ID: ids.New(ids.KindPlan), SHA256: strings.Repeat("d", 64)},
		NativePredecessors: []releases.BlueprintNativePredecessorReference{
			{ServiceID: service, FixedReadRevision: 40},
		}, PublishedAt: at}
	return intent, publication
}

type nativePublicationReadStore struct {
	t         *testing.T
	values    map[string][]byte
	revisions []int64
}

func (store *nativePublicationReadStore) GetMany(
	_ context.Context,
	request keyvalue.GetManyRequest,
) (*keyvalue.GetManyResult, error) {
	store.revisions = append(store.revisions, request.Revision)
	result := &keyvalue.GetManyResult{
		ReadRevision:     42,
		ResponseRevision: 99,
		Values:           make([]*keyvalue.KeyValue, len(request.Keys)),
	}
	for i, key := range request.Keys {
		if value, exists := store.values[key]; exists {
			result.Values[i] = &keyvalue.KeyValue{
				Key:         key,
				Value:       append([]byte(nil), value...),
				ModRevision: 40,
				Version:     1,
			}
		}
	}
	return result, nil
}

func (store *nativePublicationReadStore) Get(context.Context, string) (*keyvalue.GetResult, error) {
	store.t.Fatal("native Release read unexpectedly escaped its multi-key snapshot")
	return nil, nil
}

func (store *nativePublicationReadStore) Range(context.Context, keyvalue.RangeRequest) (*keyvalue.RangeResult, error) {
	store.t.Fatal("native Release read unexpectedly scanned unrelated history")
	return nil, nil
}
