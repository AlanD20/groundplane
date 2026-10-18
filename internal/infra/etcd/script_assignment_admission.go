package etcd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	scriptrecord "github.com/AlanD20/groundplane/internal/infra/etcd/scripts"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type ScriptWorkArtifacts struct {
	Input      ScriptWorkInput
	Snapshot   *agentpb.ResolvedRunnerSnapshot
	Projection *agentpb.ScriptRunnerProjection
	Run        *agentpb.RunScript
	Body       *agentpb.ScriptBodyArtifact
}

func (artifacts *ScriptWorkArtifacts) Clear() {
	if artifacts == nil {
		return
	}
	artifacts.Input.Execution.Clear()
	if artifacts.Body != nil {
		clear(artifacts.Body.Body)
		artifacts.Body.Body = nil
	}
}

// ResolveScriptWorkArtifacts reads only captured generations at the native work
// revision. It never reconstructs a generic plan or chooses current desired input.
func (repository *ScriptRepository) ResolveScriptWorkArtifacts(ctx context.Context, view workqueue.View) (*ScriptWorkArtifacts, error) {
	input, err := repository.LoadRetainedScriptWork(ctx, view)
	if err != nil {
		return nil, err
	}
	return repository.resolveScriptWorkArtifacts(ctx, view, input)
}

func (repository *ScriptRepository) resolveScriptWorkArtifacts(ctx context.Context, view workqueue.View, input ScriptWorkInput) (*ScriptWorkArtifacts, error) {
	artifacts := &ScriptWorkArtifacts{
		Input: input, Snapshot: &agentpb.ResolvedRunnerSnapshot{},
		Projection: &agentpb.ScriptRunnerProjection{}, Run: &agentpb.RunScript{},
		Body: &agentpb.ScriptBodyArtifact{Metadata: &agentpb.ScriptBodyArtifactMetadata{}},
	}
	fail := func(err error) (*ScriptWorkArtifacts, error) { artifacts.Clear(); return nil, err }
	execution := input.Execution
	if proto.Unmarshal(execution.Snapshot, artifacts.Snapshot) != nil ||
		proto.Unmarshal(execution.RunnerProjection, artifacts.Projection) != nil ||
		proto.Unmarshal(execution.Run, artifacts.Run) != nil ||
		proto.Unmarshal(execution.BodyMetadata, artifacts.Body.Metadata) != nil {
		return fail(errs.New(errs.KindInternal, "Script captured artifact metadata is invalid"))
	}
	metadata := artifacts.Body.Metadata
	key := scriptrecord.ScriptSetBodyGenerationKey(execution.EnvironmentID, execution.ScriptSetGeneration,
		metadata.ScriptId, metadata.Generation)
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: view.ReadRevision})
	if err != nil {
		return fail(err)
	}
	if read != nil {
		defer keyvalue.ClearValues(read.Values)
	}
	if read == nil || read.ReadRevision != view.ReadRevision || len(read.Values) != 1 || read.Values[0] == nil ||
		read.Values[0].Key != key || read.Values[0].ModRevision <= 0 || read.Values[0].ModRevision > view.ReadRevision {
		return fail(errs.New(errs.KindStateConflict, "Script captured body generation is unavailable"))
	}
	body, err := scriptrecord.DecodeScriptBodyGeneration(read.Values[0].Value)
	if err != nil || body.ScriptID != metadata.ScriptId || body.Generation != metadata.Generation ||
		body.BodySize != metadata.Size || body.BodySHA256 != hex.EncodeToString(metadata.Sha256) ||
		execution.BodySHA256 != body.BodySHA256 {
		return fail(errs.New(errs.KindInternal, "Script body generation does not match captured work"))
	}
	artifacts.Body.Body = []byte(body.Body)
	digest := sha256.Sum256(artifacts.Body.Body)
	if len(artifacts.Body.Body) != int(metadata.Size) || hex.EncodeToString(digest[:]) != body.BodySHA256 {
		return fail(errs.New(errs.KindInternal, "Script body content does not match captured digest"))
	}
	artifacts.Input.Conditions = append(artifacts.Input.Conditions, keyvalue.Condition{Key: key, ModRevision: read.Values[0].ModRevision})
	return artifacts, nil
}
