package scriptrunner

import (
	"context"
	"crypto/sha256"
	"fmt"
	"github.com/AlanD20/groundplane/internal/common/scriptexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"sort"
	"strings"
)

func containerEvidence(request scriptexecution.Request, containerID string) scriptexecution.ContainerEvidence {
	return containerEvidenceForProjection(request.Projection, containerID)
}

func containerEvidenceForProjection(
	projection *agentpb.ScriptRunnerProjection,
	containerID string,
) scriptexecution.ContainerEvidence {
	labels := environmentList(projection.Labels)
	sort.Strings(labels)
	digest := sha256.Sum256([]byte(strings.Join(labels, "\x00")))
	return scriptexecution.ContainerEvidence{ID: containerID, OwnershipLabelsSHA256: append([]byte(nil), digest[:]...)}
}

func scriptExitResult(exitCode int64) (scriptexecution.RunResult, error) {
	if exitCode < 0 || exitCode > int64(^uint32(0)>>1) {
		return scriptexecution.RunResult{}, errs.New(
			errs.KindInternal,
			"Script runner: container exit status is out of range",
		)
	}
	return scriptexecution.RunResult{ExitCode: int32(exitCode)}, nil
}

func validDockerContainerID(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, character := range value {
		if character < '0' || character > '9' && character < 'a' || character > 'f' {
			return false
		}
	}
	return true
}

func operationError(ctx context.Context, operation string, err error) error {
	if contextErr := ctx.Err(); contextErr != nil {
		return contextErr
	}
	return errs.Wrap(errs.KindInternal, fmt.Errorf("script runner: %s: %w", operation, err))
}
