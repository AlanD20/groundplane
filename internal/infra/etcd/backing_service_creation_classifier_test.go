package etcd

import (
	"strings"
	"testing"
)

func TestClassifyBackingServiceCreationUsesCurrentConditionLayout(t *testing.T) {
	publication := environmentBlueprintPublicationEvidence{
		rootRevision: 11, descriptorRevision: 12, locatorRevision: 13,
	}
	values := make([]*KeyValue, 30)
	values[5] = &KeyValue{ModRevision: publication.rootRevision}
	values[6] = &KeyValue{ModRevision: publication.descriptorRevision}
	values[7] = &KeyValue{ModRevision: publication.locatorRevision}

	err := classifyBackingServiceCreation(BackingServiceCreation{}, publication, len(values))(1, values)
	if err == nil || !strings.Contains(err.Error(), "Backing-service creation raced") {
		t.Fatalf("classification error = %v, want final race classification", err)
	}
}
