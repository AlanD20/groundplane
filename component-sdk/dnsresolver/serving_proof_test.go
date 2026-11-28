package dnsresolver

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"testing"
)

// A captured file can only claim the digest of its own closed managed config.
// Neither a copied marker nor a valid-looking wrapper containing an import
// may reach the independent DNS proof path.
func TestCorefileServingProofBindsCompleteManagedProfile(t *testing.T) {
	t.Parallel()
	base := []byte(". {\n    bind 127.0.0.1 172.17.0.1\n    forward . 1.1.1.1\n    reload\n" +
		"    prometheus 127.0.0.1:9153\n    log\n    errors\n}\n")
	if _, marked, err := CorefileServingProof(base); marked || err != nil {
		t.Fatalf("legacy profile has marked=%v, err=%v", marked, err)
	}
	file, err := AddCorefileServingProof(base)
	if err != nil {
		t.Fatal(err)
	}
	want := sha256.Sum256(base)
	if digest, marked, err := CorefileServingProof(file); !marked || err != nil ||
		digest != hex.EncodeToString(want[:]) {
		t.Fatalf("managed profile got digest=%q, marked=%v, err=%v", digest, marked, err)
	}
	mutations := [][]byte{
		bytes.Replace(file, []byte("forward . 1.1.1.1"), []byte("forward . 8.8.8.8"), 1),
		bytes.Replace(file, []byte("    log"), []byte("    import extra.conf\n    log"), 1),
		bytes.Replace(file, []byte(" 0 IN TXT "), []byte(" 1 IN TXT "), 1),
	}
	for _, mutation := range mutations {
		if _, marked, err := CorefileServingProof(mutation); !marked || err == nil {
			t.Errorf("changed managed profile got marked=%v, err=%v", marked, err)
		}
	}
}
