package localagents

import (
	bytes "bytes"
	errors "errors"
	ids "github.com/AlanD20/groundplane/internal/common/ids"
	errs "github.com/AlanD20/groundplane/pkg/errs"
	testing "testing"
	"time"
)

func TestLocalAgentReferenceCodecRejectsUnknownAndDuplicateFields(t *testing.T) {
	t.Parallel()

	agentID := ids.NewAt(ids.KindAgent, time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC), 31)
	value, err := encodeLocalAgentReference(agentID)
	if err != nil {
		t.Fatalf("testlocalagents.EncodeLocalAgentReference() error = %v", err)
	}
	for name, malformed := range map[string][]byte{
		"duplicate": bytes.Replace(value, []byte(`"schema":1`), []byte(`"schema":1,"schema":1`), 1),
		"unknown":   bytes.Replace(value, []byte(`"schema":1`), []byte(`"schema":1,"extra":true`), 1),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := DecodeLocalAgentReference(malformed); !errors.Is(
				err,
				errs.New(errs.KindInternal, ""),
			) {
				t.Fatalf("decodeLocalAgentReference() error = %v, want internal", err)
			}
		})
	}
}
