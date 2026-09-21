package localagent

import (
	"encoding/base64"
	"errors"
	"fmt"
	"github.com/AlanD20/groundplane/internal/common/agentprotocol"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/imageref"
	"github.com/AlanD20/groundplane/pkg/errs"
	"strings"
	"time"
	"unicode/utf8"
)

func validateEnrollRequest(request EnrollRequest) error {
	if err := validateAgentID(request.AgentID); err != nil {
		return err
	}
	if err := ids.Validate(ids.KindTask, request.EnrollmentTaskID); err != nil {
		return errs.New(errs.KindValidationFailed, "agent enrollment Task id is invalid")
	}
	if !imageref.IsDigestPinned(request.Image) {
		return errs.New(errs.KindValidationFailed, "agent image must be a non-empty digest-pinned reference")
	}
	if request.Config.PullIntervalSeconds <= 0 {
		return errs.New(errs.KindValidationFailed, "agent pull interval must be positive")
	}
	if request.Config.MaxConcurrentTasks <= 0 {
		return errs.New(errs.KindValidationFailed, "agent maximum concurrent tasks must be positive")
	}
	if !validLabels(request.Config.Labels) {
		return errs.New(errs.KindValidationFailed, "agent labels must contain valid NUL-free UTF-8")
	}
	return nil
}

func validateUpdateRequest(request UpdateRequest) error {
	if err := validateAgentID(request.AgentID); err != nil {
		return err
	}
	if !imageref.IsDigestPinned(request.PreviousImage) ||
		!imageref.IsDigestPinned(request.DesiredImage) {
		return errs.New(errs.KindValidationFailed, "agent update images must be digest-pinned references")
	}
	if request.PreviousImage == request.DesiredImage {
		return errs.New(errs.KindStateConflict, "agent already runs the configured image")
	}
	if request.StartingGeneration == 0 || request.StartingGeneration > ^uint64(0)-2 {
		return errs.New(errs.KindValidationFailed, "agent update starting generation is invalid")
	}
	return nil
}

func validateAgentID(agentID string) error {
	if err := ids.Validate(ids.KindAgent, agentID); err != nil {
		return errs.New(errs.KindValidationFailed, "agent id is invalid")
	}
	return nil
}

func validateStored(stored StoredRecord) error {
	if stored.Revision <= 0 {
		return errs.New(errs.KindInternal, "local agent record has an invalid revision")
	}
	if err := ids.Validate(ids.KindAgent, stored.Record.ID); err != nil {
		return errs.New(errs.KindInternal, "local agent record has an invalid id")
	}
	if err := ids.Validate(ids.KindTask, stored.Record.EnrollmentTaskID); err != nil {
		return errs.New(errs.KindInternal, "local agent record has an invalid enrollment Task id")
	}
	if !imageref.IsDigestPinned(stored.Record.Image) {
		return errs.New(errs.KindInternal, "local agent record has an invalid image")
	}
	if stored.Record.Generation == 0 {
		return errs.New(errs.KindInternal, "local agent record has an invalid generation")
	}
	if !isNonzeroUTC(stored.Record.CreatedAt) {
		return errs.New(errs.KindInternal, "local agent record has an invalid creation time")
	}
	switch stored.Record.Phase {
	case PhaseProvisioning:
		if !stored.Record.ReadyAt.IsZero() {
			return errs.New(errs.KindInternal, "provisioning local agent has a Ready timestamp")
		}
	case PhaseReady, PhaseUpdating, PhaseDeleting:
		if !isNonzeroUTC(stored.Record.ReadyAt) || stored.Record.ReadyAt.Before(stored.Record.CreatedAt) {
			return errs.New(errs.KindInternal, "local agent record has an invalid Ready timestamp")
		}
	}
	if stored.Record.Config.PullIntervalSeconds <= 0 || stored.Record.Config.MaxConcurrentTasks <= 0 {
		return errs.New(errs.KindInternal, "local agent record has an invalid runtime config")
	}
	if !validLabels(stored.Record.Config.Labels) {
		return errs.New(errs.KindInternal, "local agent record has invalid labels")
	}
	switch stored.Record.Phase {
	case PhaseProvisioning, PhaseReady, PhaseUpdating:
		if err := validateCredential(stored.Record.Credential, false); err != nil {
			return errs.Wrap(errs.KindInternal, fmt.Errorf("local agent record has an invalid credential: %w", err))
		}
	case PhaseDeleting:
		if err := validateCredential(stored.Record.Credential, true); err != nil {
			return errs.Wrap(errs.KindInternal, fmt.Errorf("local agent record has an invalid credential: %w", err))
		}
	default:
		return errs.New(errs.KindInternal, "local agent record has an invalid lifecycle phase")
	}
	return nil
}

func validateCredential(credential Credential, revoked bool) error {
	if len(credential.EncryptedToken) == 0 {
		return errors.New("encrypted token is empty")
	}
	if revoked {
		if credential.Digest != "" {
			return errors.New("revoked token digest is present")
		}
		return nil
	}
	decoded, err := base64.RawURLEncoding.Strict().DecodeString(credential.Digest)
	if err != nil || len(decoded) != agentprotocol.RawTokenBytes {
		return errors.New("token digest is not canonical unpadded base64url")
	}
	if base64.RawURLEncoding.EncodeToString(decoded) != credential.Digest {
		return errors.New("token digest is not canonical unpadded base64url")
	}
	return nil
}

func validLabels(labels map[string]string) bool {
	for key, value := range labels {
		if !utf8.ValidString(key) || strings.IndexByte(key, 0) >= 0 ||
			!utf8.ValidString(value) || strings.IndexByte(value, 0) >= 0 {
			return false
		}
	}
	return true
}

func isNonzeroUTC(value time.Time) bool {
	return !value.IsZero() && value.Location() == time.UTC
}

func isAgentNotFound(err error) bool {
	return errors.Is(err, errs.New(errs.KindAgentNotFound, ""))
}

func agentNotFound(agentID string) error {
	return errs.Newf(errs.KindAgentNotFound, "agent %s was not found", agentID)
}
