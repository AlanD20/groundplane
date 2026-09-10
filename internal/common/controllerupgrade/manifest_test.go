package controllerupgrade

import (
	"crypto/sha256"
	"fmt"
	"strings"
	"testing"
)

// Rationale: a release is immutable input, not a path/URL or a mutable tag;
// malformed, duplicate and incompatible metadata must fail before execution.
func TestManifestBoundary(t *testing.T) {
	valid := []byte(`{"agent_image":"registry.example/agent@sha256:` + strings.Repeat("a", 64) +
		`","channel_schema":1,"controller_sha256":"sha256:` + strings.Repeat("b", 64) +
		`","controller_version":"0.1.0","schema":1,"storage_epoch":1}`)
	id := Digest(fmt.Sprintf("sha256:%x", sha256.Sum256(valid)))
	manifest, err := ParseManifest(valid, id)
	if err != nil || manifest.ControllerVersion != "0.1.0" {
		t.Fatalf("valid release = %#v, %v", manifest, err)
	}
	for name, raw := range map[string][]byte{
		"digest mismatch":           append([]byte(" "), valid...),
		"duplicate":                 []byte(strings.Replace(string(valid), `"schema":1`, `"schema":1,"schema":1`, 1)),
		"unknown":                   []byte(strings.Replace(string(valid), `"schema":1`, `"other":1,"schema":1`, 1)),
		"tag":                       []byte(strings.Replace(string(valid), "@sha256:"+strings.Repeat("a", 64), ":latest", 1)),
		"storage":                   []byte(strings.Replace(string(valid), `"storage_epoch":1`, `"storage_epoch":2`, 1)),
		"protocol":                  []byte(strings.Replace(string(valid), `"channel_schema":1`, `"channel_schema":2`, 1)),
		"version control character": []byte(strings.Replace(string(valid), "0.1.0", `0.1.0\n`, 1)),
		"oversized":                 []byte(strings.Repeat(" ", MaxManifestBytes+1)),
	} {
		t.Run(name, func(t *testing.T) {
			candidateID := Digest(fmt.Sprintf("sha256:%x", sha256.Sum256(raw)))
			if name == "digest mismatch" {
				candidateID = id
			}
			if _, err := ParseManifest(raw, candidateID); err == nil {
				t.Fatal("invalid release accepted")
			}
		})
	}
}

// Rationale: release identities become directory leaves, so only canonical
// lowercase SHA256 is accepted, never a path-like or alternate encoding.
func TestDigestBoundary(t *testing.T) {
	for _, value := range []string{"", "../release", "sha256:" + strings.Repeat("A", 64),
		"sha256:" + strings.Repeat("0", 63), "sha256:" + strings.Repeat("0", 64) + "/"} {
		if Digest(value).Valid() {
			t.Fatalf("accepted invalid digest %q", value)
		}
	}
	if !Digest("sha256:" + strings.Repeat("0", 64)).Valid() {
		t.Fatal("canonical digest rejected")
	}
}
