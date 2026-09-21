package etcd

import (
	"context"
	"encoding/json"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testreleaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	testreleases "github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type blueprintNativeReferenceReadFault struct {
	*releasePlanningTestStore
	key   string
	value []byte
}

func (fault *blueprintNativeReferenceReadFault) GetMany(
	ctx context.Context,
	request testkeyvalue.GetManyRequest,
) (*testkeyvalue.GetManyResult, error) {
	read, err := fault.releasePlanningTestStore.GetMany(ctx, request)
	if err != nil {
		return read, err
	}
	for index, key := range request.Keys {
		if key == fault.key && read.Values[index] != nil {
			if fault.value == nil {
				read.Values[index] = nil
			} else {
				read.Values[index].Value = slices.Clone(fault.value)
			}
		}
	}
	return read, nil
}

func (fixture *ExecutedArtifactFixture) AssertBlueprintNativeReferenceRejection(t *testing.T, task TaskRecord) {
	t.Helper()
	ctx := context.Background()
	key := testreleases.ReleasePublicationKey(task.Params[testreleaserender.TaskReleasePublicationParam])
	stored, err := fixture.store.Get(ctx, key)
	if err != nil || stored.Entry == nil {
		t.Fatalf("native reference fixture marker: %v", err)
	}
	marker, err := testreleases.DecodeReleaseRecord[testreleases.ReleasePublicationMarker](
		stored.Entry.Value,
		"release-publication",
	)
	if err != nil || len(marker.NativePredecessors) != 2 || marker.NativePredecessors[0].Serving == nil {
		t.Fatalf("native reference fixture must own two running predecessors: %v", err)
	}
	marker.NativePredecessors[0].PriorRuntimeSHA256 = strings.Repeat("b", 64)
	changedMarker, err := testreleases.EncodeReleaseRecord("release-publication", marker)
	if err != nil {
		t.Fatal(err)
	}
	input, err := fixture.Ledger.GetBlueprintTaskRenderInput(ctx, task)
	if err != nil {
		t.Fatal(err)
	}
	renderKey := testreleases.ReleaseRenderInputStagingKey(input.PublicationID, input.Members[0].Intent.ID)
	render := input.Members[0].Render
	render.PriorRuntime.CurrentArtifact = slices.Clone(input.Members[1].Render.PriorRuntime.CurrentArtifact)
	changedRaw, err := json.Marshal(render)
	if err != nil {
		t.Fatal(err)
	}
	changedRender, err := testreleases.EncodeReleaseRecord("release-render-input", json.RawMessage(changedRaw))
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name  string
		key   string
		value []byte
	}{{"missing render", renderKey, nil}, {"foreign native bytes", renderKey, changedRender},
		{"changed marker reference", key, changedMarker}} {
		t.Run(test.name, func(t *testing.T) {
			fault := &blueprintNativeReferenceReadFault{
				releasePlanningTestStore: fixture.store,
				key:                      test.key,
				value:                    test.value,
			}
			tasks, err := NewTaskRepository(fault)
			if err != nil {
				t.Fatal(err)
			}
			ledger, err := NewReleaseLedger(fault, tasks)
			if err != nil {
				t.Fatal(err)
			}
			before := fixture.ReadRevision()
			if _, err := ledger.GetBlueprintTaskRenderInput(ctx, task); !isKind(err, errs.KindInternal) {
				t.Fatalf("corrupt native source admitted for plan reconstruction: %v", err)
			}
			if _, found, err := tasks.ClaimNextTask(ctx, ids.New(ids.KindAgent), 1, task.CreatedAt.Add(time.Second)); !isKind(
				err,
				errs.KindInternal,
			) ||
				found {
				t.Fatalf("corrupt native source admitted for assignment: found=%t err=%v", found, err)
			}
			if fixture.ReadRevision() != before {
				t.Fatal("rejected native source changed durable authority")
			}
		})
	}
}

func (fixture *ExecutedArtifactFixture) AssertBlueprintNativePublicationSourceFences(
	t *testing.T, task TaskRecord, publication BlueprintReleasePublication,
) {
	t.Helper()
	ctx := context.Background()
	manifestRead, err := fixture.store.Get(
		ctx,
		testreleases.ReleaseManifestStagingKey(task.Params[testreleaserender.TaskReleasePublicationParam]),
	)
	if err != nil || manifestRead.Entry == nil {
		t.Fatalf("native staged manifest: %v", err)
	}
	manifest, err := testreleases.DecodeReleaseRecord[testreleases.ReleaseStagedManifest](
		manifestRead.Entry.Value,
		"release-staged-manifest",
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, member := range manifest.Members {
		key := testreleases.ReleaseRenderInputStagingKey(manifest.PublicationID, member.ReleaseID)
		read, err := fixture.store.Get(ctx, key)
		if err != nil || read.Entry == nil ||
			!slices.Contains(
				publication.conditions,
				testkeyvalue.Condition{Key: key, ModRevision: read.Entry.ModRevision},
			) {
			t.Fatalf("staged native bytes lack exact final-publication CAS: %v", err)
		}
		runtimeKey := serviceruntimerecord.Key(member.ServiceID)
		runtimeRead, err := fixture.store.Get(ctx, runtimeKey)
		if err != nil || runtimeRead.Entry == nil ||
			!slices.Contains(
				publication.conditions,
				testkeyvalue.Condition{Key: runtimeKey, ModRevision: runtimeRead.Entry.ModRevision},
			) {
			t.Fatal("acknowledged native runtime lacks exact final-publication CAS")
		}
	}
}
