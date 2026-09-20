package scriptrunner

import (
	"bytes"
	"crypto/sha256"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/scriptexecution"
	"github.com/AlanD20/groundplane/internal/common/workloadimage"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"path/filepath"
	"strings"
)

func validateRequest(request scriptexecution.Request) error {
	if ids.Validate(ids.KindTask, request.TaskID) != nil ||
		ids.Validate(ids.KindAssignment, request.AssignmentID) != nil ||
		ids.Validate(ids.KindOperation, request.OperationID) != nil ||
		ids.Validate(ids.KindStep, request.StepID) != nil ||
		len(request.PlanHash) != sha256.Size ||
		request.Projection == nil ||
		request.BodyMetadata == nil ||
		request.ExecutionID == "" ||
		request.BodyMetadata.ScriptExecutionId != request.ExecutionID ||
		len(request.Body) == 0 ||
		len(request.Body) != int(request.BodyMetadata.Size) ||
		len(request.Body) > executionplan.MaximumScriptBodyBytes ||
		request.Projection.Name != "gp-script-"+strings.ToLower(request.ExecutionID) ||
		!workloadimage.LocalIDValid(request.Projection.Image) ||
		len(request.Projection.Entrypoint) != 1 ||
		request.Projection.Entrypoint[0] != "/bin/sh" ||
		len(request.Projection.Command) != 1 ||
		request.Projection.Command[0] != bodyTarget ||
		request.Projection.StopGraceSeconds != stopSeconds {
		return errs.New(errs.KindInternal, "Script runner: request is invalid")
	}
	digest := sha256.Sum256(request.Body)
	if !bytes.Equal(digest[:], request.BodyMetadata.Sha256) {
		return errs.New(errs.KindInternal, "Script runner: body digest does not match")
	}
	for _, entry := range request.Entries {
		if entry == nil || entry.Binding == nil || len(entry.Binding.Sha256) != sha256.Size ||
			len(entry.Value) > 256<<10 {
			return errs.New(errs.KindInternal, "Script runner: Entry artifact is invalid")
		}
		entryDigest := sha256.Sum256(entry.Value)
		if !bytes.Equal(entryDigest[:], entry.Binding.Sha256) {
			return errs.New(errs.KindInternal, "Script runner: Entry artifact digest does not match")
		}
		switch entry.Binding.Kind {
		case agentpb.ScriptEntryBindingKind_SCRIPT_ENTRY_BINDING_KIND_ENV:
			if entry.Binding.EnvironmentKey == "" || entry.Binding.FileTarget != "" {
				return errs.New(errs.KindInternal, "Script runner: Environment Entry binding is invalid")
			}
		case agentpb.ScriptEntryBindingKind_SCRIPT_ENTRY_BINDING_KIND_FILE:
			if entry.Binding.EnvironmentKey != "" || !filepath.IsAbs(entry.Binding.FileTarget) ||
				filepath.Clean(
					entry.Binding.FileTarget,
				) != entry.Binding.FileTarget || entry.Binding.FileTarget == bodyTarget ||
				(entry.Binding.Mode != 0o444 && entry.Binding.Mode != 0o600) {
				return errs.New(errs.KindInternal, "Script runner: file Entry binding is invalid")
			}
		default:
			return errs.New(errs.KindInternal, "Script runner: Entry binding kind is invalid")
		}
	}
	return nil
}
