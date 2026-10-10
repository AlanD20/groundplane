package taskassignments

import (
	"crypto/sha256"
	"encoding/hex"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

const serviceAbsenceRecoveryPrefix = "/v1/records/service-absence-recoveries/"

func ServiceAbsenceRecoveryKey(taskID string) string { return serviceAbsenceRecoveryPrefix + taskID }

func ServiceAbsenceProcedureSHA256(procedure *agentpb.ServiceAbsenceRecoveryProcedure) (string, error) {
	if procedure == nil {
		return "", errs.New(errs.KindValidationFailed, "Service absence procedure is absent")
	}
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(procedure)
	if err != nil {
		return "", errs.Wrap(errs.KindInternal, err)
	}
	defer clear(encoded)
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}
