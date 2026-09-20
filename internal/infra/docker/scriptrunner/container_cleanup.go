package scriptrunner

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/AlanD20/groundplane/internal/common/scriptexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	containerderrdefs "github.com/containerd/errdefs"
	"github.com/moby/moby/client"
)

func (runner *Runner) Cleanup(
	request scriptexecution.Request,
	body *scriptexecution.BodyEvidence,
	containerEvidence *scriptexecution.ContainerEvidence,
) (scriptexecution.CleanupProof, error) {
	var proof scriptexecution.CleanupProof
	if runner == nil || runner.client == nil || runner.bodies == nil {
		return proof, errs.New(errs.KindInternal, "Script runner: runtime is not configured")
	}
	if err := validateRequest(request); err != nil {
		return proof, err
	}
	var prepared *preparedBody
	if body != nil {
		value, err := preparedBodyForEvidence(runner.bodies, request, *body)
		if err != nil {
			return proof, err
		}
		prepared = &value
		proof.BodyDevice, proof.BodyInode, proof.BodyLeaf = body.Device, body.Inode, body.Leaf
	}
	containerID := ""
	if containerEvidence != nil {
		expected := containerEvidenceForRequest(request, containerEvidence.ID)
		if !bytes.Equal(containerEvidence.OwnershipLabelsSHA256, expected.OwnershipLabelsSHA256) {
			return proof, errs.New(errs.KindStateConflict, "Script runner: cleanup ownership digest differs")
		}
		containerID = containerEvidence.ID
		proof.ContainerID = containerID
	}
	if err := runner.cleanup(request, containerID, prepared); err != nil {
		return proof, errs.Wrap(errs.KindInternal, fmt.Errorf("script runner: cleanup failed: %w", err))
	}
	proof.ContainerAbsent = true
	proof.BodyAbsent = true
	proof.ExecutionDirectoryAbsent = true
	return proof, nil
}

func (runner *Runner) cleanup(
	request scriptexecution.Request,
	containerID string,
	prepared *preparedBody,
) error {
	ctx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
	defer cancel()
	var cleanupErrors []error
	containerAbsent := containerID == ""
	containerOwned := containerID != ""
	if containerID != "" {
		inspected, err := runner.inspectCleanupContainer(ctx, containerID, request)
		if err != nil {
			if containerderrdefs.IsNotFound(err) {
				containerAbsent = true
			} else {
				cleanupErrors = append(cleanupErrors, fmt.Errorf("inspect container before cleanup: %w", err))
				containerOwned = false
			}
		} else if inspected.Container.State != nil && inspected.Container.State.Running {
			timeout := stopSeconds
			if _, stopErr := runner.client.ContainerStop(ctx, containerID, client.ContainerStopOptions{Signal: "SIGTERM", Timeout: &timeout}); stopErr != nil && !containerderrdefs.IsNotFound(stopErr) {
				cleanupErrors = append(cleanupErrors, fmt.Errorf("stop container: %w", stopErr))
			}
		}

		if !containerAbsent && containerOwned {
			inspected, err = runner.inspectCleanupContainer(ctx, containerID, request)
			if err != nil {
				if containerderrdefs.IsNotFound(err) {
					containerAbsent = true
				} else {
					cleanupErrors = append(cleanupErrors, fmt.Errorf("inspect container after stop: %w", err))
					containerOwned = false
				}
			} else if inspected.Container.State != nil && inspected.Container.State.Running {
				if _, killErr := runner.client.ContainerKill(ctx, containerID, client.ContainerKillOptions{Signal: "SIGKILL"}); killErr != nil && !containerderrdefs.IsNotFound(killErr) {
					cleanupErrors = append(cleanupErrors, fmt.Errorf("kill container: %w", killErr))
				}
			}
		}

		if !containerAbsent && containerOwned {
			if _, err = runner.inspectCleanupContainer(ctx, containerID, request); err != nil {
				if containerderrdefs.IsNotFound(err) {
					containerAbsent = true
				} else {
					cleanupErrors = append(cleanupErrors, fmt.Errorf("inspect container before removal: %w", err))
					containerOwned = false
				}
			} else if _, removeErr := runner.client.ContainerRemove(ctx, containerID, client.ContainerRemoveOptions{Force: true}); removeErr != nil && !containerderrdefs.IsNotFound(removeErr) {
				cleanupErrors = append(cleanupErrors, fmt.Errorf("remove container: %w", removeErr))
			}
		}

		if !containerAbsent && containerOwned {
			if _, err = runner.client.ContainerInspect(ctx, containerID, client.ContainerInspectOptions{}); err == nil {
				cleanupErrors = append(cleanupErrors, errors.New("container remains after removal"))
			} else if containerderrdefs.IsNotFound(err) {
				containerAbsent = true
			} else {
				cleanupErrors = append(cleanupErrors, fmt.Errorf("prove container removal: %w", err))
			}
		}
	}
	if prepared != nil && containerAbsent {
		if err := runner.bodies.RemoveEntries(request.AssignmentID, request.ExecutionID, request.Entries); err != nil {
			cleanupErrors = append(cleanupErrors, fmt.Errorf("remove Entry evidence: %w", err))
		}
		if err := runner.bodies.Remove(*prepared); err != nil {
			cleanupErrors = append(cleanupErrors, fmt.Errorf("remove body evidence: %w", err))
		}
	} else if prepared != nil {
		cleanupErrors = append(cleanupErrors, errors.New("container absence is not proven; Script artifacts retained"))
	}
	if containerAbsent {
		if err := runner.bodies.ProveExecutionAbsent(request.AssignmentID, request.ExecutionID); err != nil {
			cleanupErrors = append(cleanupErrors, fmt.Errorf("prove execution directory absence: %w", err))
		}
	}
	return errors.Join(cleanupErrors...)
}
