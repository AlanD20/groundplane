package taskassignments

import (
	"crypto/sha256"
	"encoding/hex"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

const componentRecoveryPrefix = "/v1/records/component-recoveries/"

func ComponentRecoveryKey(taskID string) string { return componentRecoveryPrefix + taskID }

func ComponentRecoveryProcedureSHA256(procedure *agentpb.ComponentRecoveryProcedure) (string, error) {
	if procedure == nil {
		return "", errs.New(errs.KindValidationFailed, "Component recovery procedure is absent")
	}
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(procedure)
	if err != nil {
		return "", errs.Wrap(errs.KindInternal, err)
	}
	defer clear(encoded)
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}
