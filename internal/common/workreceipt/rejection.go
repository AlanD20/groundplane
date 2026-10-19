package workreceipt

import (
	"bytes"
	"crypto/sha256"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// Reject identifies a refused terminal report without acknowledging its outcome
// or releasing any durable ownership. Codes/details must be public error text.
func Reject(request *agentpb.AgentMessage, code, detail string) (*agentpb.ControllerMessage, error) {
	identity, kind, supported, err := Describe(request)
	if err != nil {
		return nil, err
	}
	if !supported || code == "" || len(code) > 128 || len(detail) > 4096 {
		return nil, invalid()
	}
	raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(request)
	if err != nil {
		return nil, invalid()
	}
	defer clear(raw)
	digest := sha256.Sum256(raw)
	return &agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_WorkReportRejection{
		WorkReportRejection: &agentpb.WorkReportRejection{Attempt: proto.CloneOf(identity), Kind: kind,
			ReportSha256: digest[:], FailureCode: code, FailureDetail: detail},
	}}, nil
}

// MatchRejection binds the refusal to the complete report, not merely its
// attempt ID. It is deliberately separate from successful receipt validation.
func MatchRejection(request *agentpb.AgentMessage, response *agentpb.ControllerMessage) error {
	rejection := response.GetWorkReportRejection()
	if rejection == nil || executionplan.RejectUnknown(response) != nil || proto.Size(response) > MaximumBytes {
		return invalid()
	}
	expected, err := Reject(request, rejection.FailureCode, rejection.FailureDetail)
	if err != nil {
		return err
	}
	want := expected.GetWorkReportRejection()
	if !proto.Equal(want.Attempt, rejection.Attempt) || want.Kind != rejection.Kind ||
		!bytes.Equal(want.ReportSha256, rejection.ReportSha256) {
		return invalid()
	}
	return nil
}
