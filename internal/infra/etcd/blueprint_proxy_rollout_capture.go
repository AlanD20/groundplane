package etcd

import (
	"bytes"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	"github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourcequeries"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// Supplemental sources and executable input must describe the same Release.
// The shared projection is restored from its immutable candidate, not a current
// Environment read. This also checks compact records before they are staged.
func validateProxyRolloutCapture(preparation BlueprintProxyServiceRollout,
	capture releaserender.ServiceRolloutCapture,
	projection projectionrecord.EnvironmentComposeProjection,
) error {
	if capture.Action != workexecution.ActionApply || len(preparation.Members) != 1 || len(preparation.Manifest.Record.Members) != 1 {
		return errs.New(errs.KindStateConflict, "captured proxy rollout requires one Service")
	}
	render, err := releaserender.OpenServiceRolloutCapture(capture)
	if err != nil {
		return err
	}
	member := preparation.Members[0]
	intent := member.Intent
	manifest := preparation.Manifest.Record.Members[0]
	if domain.ValidateIntent(intent) != nil || intent.ID != render.ReleaseID ||
		intent.ServiceID != render.ServiceID || intent.EnvironmentID != capture.Scope.EnvironmentID ||
		intent.OperationID != capture.OperationID || intent.OriginatingTaskID != capture.TaskID ||
		intent.OperationKind != domain.OperationBlueprintApply ||
		manifest.ReleaseID != intent.ID || manifest.ServiceID != intent.ServiceID ||
		preparation.Manifest.ReadRevision < preparation.Manifest.Revision ||
		len(preparation.Hooks) != len(render.Hooks) {
		return errs.New(errs.KindStateConflict, "captured proxy rollout Release or hook membership changed")
	}
	intentDigest, err := domain.Digest(intent)
	if err != nil {
		return err
	}
	if manifest.IntentDigest != intentDigest {
		return errs.New(errs.KindStateConflict, "captured proxy rollout intent differs from its staged Release")
	}
	member.Render.Projection = projectionrecord.CloneEnvironmentComposeProjection(projection)
	expected, err := releaserender.EncodeReleaseRenderInput(render)
	if err != nil {
		return err
	}
	defer clear(expected)
	actual, err := releaserender.EncodeReleaseRenderInput(member.Render)
	if err != nil {
		return err
	}
	defer clear(actual)
	if !bytes.Equal(expected, actual) {
		return errs.New(errs.KindStateConflict, "captured proxy rollout input differs from its retained sources")
	}
	for _, hook := range render.Hooks {
		sources, found := preparation.Hooks[hook.ScriptExecutionID]
		if !found {
			return errs.New(errs.KindStateConflict, "captured proxy rollout hook sources are missing")
		}
		sources.DesiredProjection.Record = projectionrecord.CloneEnvironmentComposeProjection(projection)
		sources.RenderInput.Record.Projection = projectionrecord.CloneEnvironmentComposeProjection(projection)
		snapshot := &agentpb.ResolvedRunnerSnapshot{}
		if err := (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(hook.RunnerSnapshot, snapshot); err != nil ||
			snapshot.GetScriptExecutionId() != hook.ScriptExecutionID || snapshot.GetSnapshotId() != hook.RunnerSnapshotID ||
			snapshot.GetServiceId() != render.ServiceID || snapshot.GetReleaseId() != render.ReleaseID ||
			sources.Script.Record.ScriptSetGeneration != capture.TaskID ||
			scriptsourcequeries.ValidateScriptContextSources(sources, snapshot) != nil {
			return errs.New(errs.KindStateConflict, "captured proxy rollout hook sources differ from its execution input")
		}
	}
	return nil
}
