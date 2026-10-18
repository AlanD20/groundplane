package etcd

import (
	"crypto/sha256"
	"encoding/hex"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runtimeconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourcequeries"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const directReleasePreparationPrefix = "/v1/execution/direct-release-preparations/"

func DirectReleasePreparationKey(workItemID string) string {
	return directReleasePreparationPrefix + workItemID
}

// DirectReleaseSource is the fixed hierarchy and authored input selected by an
// explicit Deploy/Rollback. Revisions are publication comparisons, not permission
// to resolve a later Blueprint, configuration, image or Script generation.
type DirectReleaseSource struct {
	ReadRevision        int64 `json:"read_revision"`
	EnvironmentRevision int64 `json:"environment_revision"`
	ProjectRevision     int64 `json:"project_revision"`
	TenantRevision      int64 `json:"tenant_revision"`
	DesiredRevision     int64 `json:"desired_revision"`
	EpochRevision       int64 `json:"epoch_revision"`
}

type DirectReleaseHookSources struct {
	ExecutionID string                                     `json:"execution_id"`
	Sources     scriptsourcequeries.ScriptExecutionSources `json:"sources"`
}

// DirectReleasePreparation supplements a digest-bound native rollout. Large
// rollout bytes live only in that rollout. Hook sources keep their own exact
// provenance: a failure hook can target the preceding Release, whose projection
// must not be replaced with the new candidate's projection.
type DirectReleasePreparation struct {
	Rollout       workexecution.Input               `json:"rollout"`
	Source        DirectReleaseSource               `json:"source"`
	Configuration runtimeconfiguration.WorkCapture  `json:"configuration"`
	Intent        domain.Intent                     `json:"intent"`
	Manifest      releases.VersionedReleaseManifest `json:"manifest"`
	Predecessor   BlueprintNativePredecessorCapture `json:"predecessor"`
	Hooks         []DirectReleaseHookSources        `json:"hooks,omitempty"`
}

func (request DirectReleasePreparation) validate() error {
	if request.Rollout.Validate() != nil || request.Configuration.Validate() != nil || domain.ValidateIntent(request.Intent) != nil ||
		request.Intent.OperationKind != domain.OperationDeploy && request.Intent.OperationKind != domain.OperationRollback ||
		request.Configuration.TaskID != request.Intent.OriginatingTaskID ||
		request.Configuration.Current.EnvironmentID != request.Intent.EnvironmentID ||
		request.Source.ReadRevision <= 0 || request.Configuration.ReadRevision != request.Source.ReadRevision ||
		request.Manifest.Revision <= 0 || request.Manifest.ReadRevision < request.Manifest.Revision ||
		request.Manifest.Revision < request.Source.ReadRevision ||
		request.Manifest.Record.OperationID != request.Intent.OperationID ||
		releases.ValidatePublicationID(request.Manifest.Record.PublicationID) != nil ||
		len(request.Manifest.Record.Members) != 1 || len(request.Hooks) > 16 ||
		request.Predecessor.ServiceID != request.Intent.ServiceID ||
		request.Predecessor.FixedReadRevision != request.Source.ReadRevision {
		return errs.New(errs.KindValidationFailed, "Direct Release preparation identity or source is invalid")
	}
	for _, revision := range []int64{request.Source.EnvironmentRevision, request.Source.ProjectRevision,
		request.Source.DesiredRevision, request.Source.EpochRevision} {
		if revision <= 0 || revision > request.Source.ReadRevision {
			return errs.New(errs.KindValidationFailed, "Direct Release source revision is invalid")
		}
	}
	if request.Source.TenantRevision < 0 || request.Source.TenantRevision > request.Source.ReadRevision ||
		(request.Intent.Workspace.TenantID == "") != (request.Source.TenantRevision == 0) {
		return errs.New(errs.KindValidationFailed, "Direct Release Tenant source is invalid")
	}
	member := request.Manifest.Record.Members[0]
	intentDigest, err := domain.Digest(request.Intent)
	if err != nil || member.ReleaseID != request.Intent.ID || member.ServiceID != request.Intent.ServiceID ||
		member.IntentDigest != intentDigest || member.RenderDigest != request.Intent.RenderInputDigest {
		return errs.New(errs.KindStateConflict, "Direct Release preparation differs from its staged intent")
	}
	seen := make(map[string]bool, len(request.Hooks))
	for _, hook := range request.Hooks {
		if hook.ExecutionID == "" || seen[hook.ExecutionID] || hook.Sources.Revision != request.Manifest.ReadRevision ||
			hook.Sources.Environment.Record.ID != request.Intent.EnvironmentID ||
			hook.Sources.Service.Record.Desired.ID != request.Intent.ServiceID ||
			hook.Sources.DesiredProjection.Record.EnvironmentID != request.Intent.EnvironmentID ||
			hook.Sources.RenderInput.Record.EnvironmentID != request.Intent.EnvironmentID {
			return errs.New(errs.KindValidationFailed, "Direct Release hook source membership is invalid")
		}
		seen[hook.ExecutionID] = true
	}
	return nil
}

func EncodeDirectReleasePreparation(request DirectReleasePreparation) ([]byte, workexecution.Input, error) {
	if err := request.validate(); err != nil {
		return nil, workexecution.Input{}, err
	}
	value, err := recordcodec.Encode("direct-release-preparation", request)
	if err != nil {
		return nil, workexecution.Input{}, err
	}
	if len(value) > keyvalue.MaximumBytes {
		clear(value)
		return nil, workexecution.Input{}, errs.New(errs.KindReleasePlanTooLarge, "Direct Release preparation exceeds the storage budget")
	}
	digest := sha256.Sum256(value)
	return value, workexecution.Input{ID: request.Rollout.ID, SHA256: hex.EncodeToString(digest[:])}, nil
}

func DecodeDirectReleasePreparation(value []byte, expected workexecution.Input) (DirectReleasePreparation, error) {
	if len(value) == 0 || len(value) > keyvalue.MaximumBytes || expected.Validate() != nil {
		return DirectReleasePreparation{}, errs.New(errs.KindValidationFailed, "Direct Release preparation reference is invalid")
	}
	digest := sha256.Sum256(value)
	if hex.EncodeToString(digest[:]) != expected.SHA256 {
		return DirectReleasePreparation{}, errs.New(errs.KindStateConflict, "Direct Release preparation digest changed")
	}
	request, err := recordcodec.Decode[DirectReleasePreparation](value, "direct-release-preparation")
	if err != nil {
		return DirectReleasePreparation{}, err
	}
	if err := request.validate(); err != nil {
		return DirectReleasePreparation{}, err
	}
	if request.Rollout.ID != expected.ID {
		return DirectReleasePreparation{}, errs.New(errs.KindStateConflict, "Direct Release preparation identity changed")
	}
	return request, nil
}

func (request DirectReleasePreparation) Matches(task workexecution.Task, item workexecution.Item, capture releaserender.ServiceRolloutCapture) error {
	if err := request.validate(); err != nil {
		return err
	}
	if err := capture.MatchesWork(task, item); err != nil {
		return err
	}
	if err := request.Configuration.Matches(task, item); err != nil {
		return err
	}
	operation, err := releaserender.ReleaseOperationKind(task.Action)
	if err != nil || operation != request.Intent.OperationKind {
		return errs.New(errs.KindStateConflict, "Direct Release preparation belongs to another Task action")
	}
	value, reference, err := EncodeDirectReleasePreparation(request)
	clear(value)
	if err != nil {
		return err
	}
	rollout, err := releaserender.ServiceRolloutInput(capture)
	if err != nil {
		return err
	}
	if task.Intent != reference || request.Rollout != rollout || capture.SourceReadRevision != request.Source.ReadRevision ||
		capture.OperationID != request.Intent.OperationID || capture.PublicationID != request.Manifest.Record.PublicationID ||
		request.Intent.Workspace.TenantID != task.Scope.TenantID || request.Intent.Workspace.ProjectID != task.Scope.ProjectID ||
		request.Intent.Workspace.EnvironmentID != task.Scope.EnvironmentID ||
		request.Intent.Actor != string(task.Actor) || !request.Intent.CreatedAt.Equal(capture.CreatedAt) ||
		request.Intent.GroupOperationID != "" || request.Intent.GroupMemberOrdinal != 0 ||
		capture.Render.ReleaseID != request.Intent.ID || capture.Render.ServiceID != request.Intent.ServiceID ||
		capture.Render.CandidateWorkload != request.Intent.CandidateWorkload || capture.Render.Strategy != request.Intent.Strategy ||
		capture.Render.Slot != request.Intent.Slot || request.Configuration.Generation != capture.Render.Projection.RenderGeneration ||
		len(request.Hooks) != len(capture.Render.Hooks) {
		return errs.New(errs.KindStateConflict, "Direct Release executable input differs from its accepted preparation")
	}
	for _, capturedHook := range capture.Render.Hooks {
		found := false
		for _, hook := range request.Hooks {
			if hook.ExecutionID == capturedHook.ScriptExecutionID {
				found = true
				break
			}
		}
		if !found {
			return errs.New(errs.KindStateConflict, "Direct Release hook is absent from its captured source set")
		}
	}
	return nil
}

// MatchesInput prevents a final preparation from replacing the file authority
// accepted with the action. Matching Task/item IDs alone is insufficient.
func (request DirectReleasePreparation) MatchesInput(input releaserender.ReleaseWorkInput) error {
	expected, actual := request.Configuration, input.Configuration
	if input.Validate() != nil || input.OperationID != request.Intent.OperationID || input.Rollout != request.Rollout ||
		expected.TaskID != actual.TaskID || expected.WorkItemID != actual.WorkItemID || expected.Placement != actual.Placement ||
		expected.ReadRevision != actual.ReadRevision || expected.Generation != actual.Generation || expected.Current != actual.Current ||
		expected.PriorRevision != actual.PriorRevision || (expected.Prior == nil) != (actual.Prior == nil) ||
		!slices.Equal(expected.Destinations, actual.Destinations) ||
		expected.Prior != nil && actual.Prior != nil && *expected.Prior != *actual.Prior {
		return errs.New(errs.KindStateConflict, "Direct Release executable input replaced its accepted configuration")
	}
	return nil
}
