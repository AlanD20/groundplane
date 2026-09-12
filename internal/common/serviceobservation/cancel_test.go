package serviceobservation

import (
	"testing"

	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// Rationale: only a canonical correlation id may cancel the named read;
// generated wire encoding must preserve that closed request.
func TestObservationCancelIsClosed(t *testing.T) {
	request := &agentpb.CancelServiceObservation{RequestId: observationRequest().RequestId}
	message := &agentpb.ControllerMessage{
		Payload: &agentpb.ControllerMessage_CancelServiceObservation{CancelServiceObservation: request},
	}
	encoded, err := proto.Marshal(message)
	if err != nil {
		t.Fatal(err)
	}
	decoded := &agentpb.ControllerMessage{}
	if err := proto.Unmarshal(encoded, decoded); err != nil {
		t.Fatal(err)
	}
	if err := ValidateCancel(decoded.GetCancelServiceObservation()); err != nil {
		t.Fatal(err)
	}
	request.ProtoReflect().SetUnknown([]byte{0x78, 1})
	for _, invalid := range []*agentpb.CancelServiceObservation{nil, {}, {RequestId: "wrong"}, request} {
		if err := ValidateCancel(invalid); err == nil {
			t.Fatal("accepted invalid cancel")
		}
	}
}
