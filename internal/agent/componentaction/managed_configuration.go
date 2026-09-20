package componentaction

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"github.com/AlanD20/groundplane/internal/agent"
	taskassignment "github.com/AlanD20/groundplane/internal/agent/taskassignment"
	"github.com/AlanD20/groundplane/internal/infra/docker/managedconfighelper"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"strconv"
)

func managedConfigTransactionState(
	request *agentpb.ManagedConfigHelperRequest,
	response *agentpb.ManagedConfigHelperResponse,
) (agent.ManagedConfigTransactionState, error) {
	if request == nil || response == nil || response.GetSchema() != managedconfighelper.SchemaVersion ||
		response.GetTransactionId() != request.GetTransactionId() ||
		response.GetOperation() != request.GetOperation() ||
		response.GetDisposition() == agentpb.ManagedConfigReplayDisposition_MANAGED_CONFIG_REPLAY_DISPOSITION_UNSPECIFIED {
		return agent.ManagedConfigTransactionState{}, errs.New(
			errs.KindStateConflict,
			"agent: managed-config helper response does not match its transaction",
		)
	}
	live, err := managedConfigFileState(response.GetLiveSha256())
	if err != nil {
		return agent.ManagedConfigTransactionState{}, err
	}
	previous, err := managedConfigFileState(response.GetPreviousSha256())
	if err != nil {
		return agent.ManagedConfigTransactionState{}, err
	}
	state := agent.ManagedConfigTransactionState{Live: live, Previous: previous}
	if !managedConfigStateMatches(previous, request.GetExpectedPreviousSha256()) {
		return agent.ManagedConfigTransactionState{}, errs.New(
			errs.KindStateConflict,
			"agent: managed-config helper predecessor proof changed",
		)
	}
	expectedLive := request.GetSha256()
	if request.GetOperation() == agentpb.ManagedConfigOperation_MANAGED_CONFIG_OPERATION_ROLLBACK {
		expectedLive = request.GetExpectedPreviousSha256()
	}
	if !managedConfigStateMatches(live, expectedLive) {
		return agent.ManagedConfigTransactionState{}, errs.New(
			errs.KindStateConflict,
			"agent: managed-config helper live proof changed",
		)
	}
	return state, nil
}

func managedConfigFileState(digest []byte) (agent.ManagedConfigFileState, error) {
	if len(digest) == 0 {
		return agent.ManagedConfigFileState{}, nil
	}
	if len(digest) != sha256.Size {
		return agent.ManagedConfigFileState{}, errs.New(
			errs.KindStateConflict,
			"agent: managed-config helper digest proof is invalid",
		)
	}
	state := agent.ManagedConfigFileState{Present: true}
	copy(state.SHA256[:], digest)
	return state, nil
}

func managedConfigStateMatches(state agent.ManagedConfigFileState, digest []byte) bool {
	if len(digest) == 0 {
		return !state.Present
	}
	return len(digest) == sha256.Size && state.Present &&
		subtle.ConstantTimeCompare(state.SHA256[:], digest) == 1
}

func managedConfigRequest(
	assignment taskassignment.Assignment,
	action *agentpb.ComponentApply,
	relativePath string,
	operation agentpb.ManagedConfigOperation,
) *agentpb.ManagedConfigHelperRequest {
	digest := sha256.New()
	for _, value := range []string{
		assignment.OperationID, assignment.TaskID, action.GetComponentId(), action.GetArtifactId(),
		strconv.FormatUint(action.GetGeneration(), 10),
	} {
		_, _ = digest.Write([]byte(value))
		_, _ = digest.Write([]byte{0})
	}
	return &agentpb.ManagedConfigHelperRequest{
		Schema: managedconfighelper.SchemaVersion, ArtifactId: action.GetArtifactId(),
		RelativePath: relativePath, Sha256: append([]byte(nil), action.GetArtifactDigest()...),
		ExpectedPreviousSha256: append([]byte(nil), action.GetExpectedPreviousArtifactDigest()...),
		Operation:              operation, TransactionId: "mct_" + hex.EncodeToString(digest.Sum(nil)),
		Generation: action.GetGeneration(),
	}
}
