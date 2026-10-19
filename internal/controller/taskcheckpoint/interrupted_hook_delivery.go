package taskcheckpoint

import (
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func interruptedHookDelivery(
	attempt workexecution.Attempt,
	captured *etcd.ScriptWorkOwnership,
	proof workexecution.Input,
) (*agentpb.ReleaseHookDelivery, error) {
	identity, err := workexecution.EncodeAgentAttempt(attempt)
	if err != nil {
		return nil, err
	}
	digest, err := hex.DecodeString(captured.Input.SHA256)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	cleanupDigest, err := hex.DecodeString(proof.SHA256)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	return proto.CloneOf(&agentpb.ReleaseHookDelivery{Attempt: identity, OperationId: captured.OperationID,
		Input: &agentpb.WorkInputReference{InputId: captured.Input.ID, Sha256: digest},
		Run:   captured.Run, Projection: captured.Projection, Cleanup: captured.Cleanup,
		InterruptedCleanup: &agentpb.WorkInputReference{InputId: proof.ID, Sha256: cleanupDigest}}), nil
}
