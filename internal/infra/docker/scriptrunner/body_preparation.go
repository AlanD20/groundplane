package scriptrunner

import (
	"bytes"
	"context"
	"crypto/sha256"
	"github.com/AlanD20/groundplane/internal/common/scriptexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"path/filepath"
)

func (runner *Runner) PrepareBody(
	ctx context.Context,
	request scriptexecution.Request,
) (scriptexecution.BodyEvidence, error) {
	if ctx == nil || runner == nil || runner.client == nil || runner.bodies == nil {
		return scriptexecution.BodyEvidence{}, errs.New(errs.KindInternal, "Script runner: runtime is not configured")
	}
	if err := validateRequest(request); err != nil {
		return scriptexecution.BodyEvidence{}, err
	}
	prepared, err := runner.bodies.Prepare(
		request.AssignmentID,
		request.ExecutionID,
		request.Body,
		request.BodyMetadata.Uid,
		request.BodyMetadata.Gid,
	)
	if err != nil {
		return scriptexecution.BodyEvidence{}, err
	}
	evidence := bodyEvidence(prepared)
	if err := runner.bodies.PrepareEntries(request.AssignmentID, request.ExecutionID, request.Entries); err != nil {
		return evidence, err
	}
	return evidence, nil
}

func bodyEvidence(prepared preparedBody) scriptexecution.BodyEvidence {
	return scriptexecution.BodyEvidence{
		SHA256: append([]byte(nil), prepared.identity.SHA256[:]...), UID: prepared.identity.UID,
		GID: prepared.identity.GID, Device: prepared.identity.Device, Inode: prepared.identity.Inode,
		Leaf: prepared.identity.Leaf,
	}
}

func preparedBodyForEvidence(
	store *bodyStore,
	request scriptexecution.Request,
	evidence scriptexecution.BodyEvidence,
) (preparedBody, error) {
	if store == nil || evidence.Leaf != bodyLeaf || evidence.Device == 0 || evidence.Inode == 0 ||
		len(evidence.SHA256) != sha256.Size || !bytes.Equal(evidence.SHA256, request.BodyMetadata.Sha256) ||
		evidence.UID != request.BodyMetadata.Uid || evidence.GID != request.BodyMetadata.Gid {
		return preparedBody{}, errs.New(errs.KindStateConflict, "Script runner: durable body evidence is invalid")
	}
	var digest [sha256.Size]byte
	copy(digest[:], evidence.SHA256)
	return preparedBody{
		hostPath:     filepath.Join(store.rootPath, request.AssignmentID, request.ExecutionID, bodyLeaf),
		assignmentID: request.AssignmentID, executionID: request.ExecutionID,
		identity: bodyIdentity{Device: evidence.Device, Inode: evidence.Inode, UID: evidence.UID,
			GID: evidence.GID, Size: uint32(len(request.Body)), SHA256: digest, Leaf: evidence.Leaf},
	}, nil
}
