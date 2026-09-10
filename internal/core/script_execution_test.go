package core

import (
	"errors"
	"strconv"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: explicit setup is a bounded opt-in; it never inherits resources
// and accepts root only as an authored numeric user with a digest-pinned image.
func TestScriptExecutionValidatesClosedChoice(t *testing.T) {
	for _, execution := range []ScriptExecution{
		{Mode: ScriptExecutionInherited},
		validExplicitScriptExecution(),
		{Mode: ScriptExecutionExplicit, Image: scriptExecutionTestImage, User: "4294967295:4294967295"},
	} {
		if err := execution.Validate(); err != nil {
			t.Fatalf("valid choice rejected: %v", err)
		}
	}
	for _, test := range []struct {
		name string
		edit func(*ScriptExecution)
	}{
		{"missing mode", func(value *ScriptExecution) { value.Mode = "" }},
		{"unknown mode", func(value *ScriptExecution) { value.Mode = "host" }},
		{"mixed inherited", func(value *ScriptExecution) { value.Mode = ScriptExecutionInherited }},
		{"mutable image", func(value *ScriptExecution) { value.Image = "example/setup:latest" }},
		{"local image", func(value *ScriptExecution) { value.Image = "sha256:" + strings.Repeat("a", 64) }},
		{"tag and digest", func(value *ScriptExecution) { value.Image = "example/setup:1@sha256:" + strings.Repeat("a", 64) }},
		{"upper digest", func(value *ScriptExecution) { value.Image = "example/setup@sha256:" + strings.Repeat("A", 64) }},
		{"missing user", func(value *ScriptExecution) { value.User = "" }},
		{"named user", func(value *ScriptExecution) { value.User = "root:root" }},
		{"missing gid", func(value *ScriptExecution) { value.User = "0" }},
		{"leading zero", func(value *ScriptExecution) { value.User = "00:0" }},
		{"signed uid", func(value *ScriptExecution) { value.User = "+1:0" }},
		{"negative uid", func(value *ScriptExecution) { value.User = "-1:0" }},
		{"overflow gid", func(value *ScriptExecution) { value.User = "0:4294967296" }},
		{"space in user", func(value *ScriptExecution) { value.User = "0:0 " }},
		{"foreign id kind", func(value *ScriptExecution) { value.Volumes[0].VolumeID = scriptExecutionTestEntry }},
		{"duplicate volume", func(value *ScriptExecution) { value.Volumes = append(value.Volumes, value.Volumes[0]) }},
		{"duplicate entry", func(value *ScriptExecution) { value.EntryIDs = append(value.EntryIDs, value.EntryIDs[0]) }},
		{"non-entry id", func(value *ScriptExecution) { value.EntryIDs[0] = scriptExecutionTestVolume }},
		{"nested mount", func(value *ScriptExecution) {
			value.Volumes = append(value.Volumes, ScriptVolumeGrant{
				VolumeID: "vol_01ARZ3NDEKTSV4RRFFQ69G5FAW", Target: "/data/child", ReadOnly: true,
			})
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			execution := validExplicitScriptExecution()
			test.edit(&execution)
			if err := execution.Validate(); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
				t.Fatalf("invalid context error = %v", err)
			}
		})
	}
}

// Rationale: managed storage cannot replace the interpreter, kernel/runtime
// paths or fixed body; component-prefix siblings remain independent targets.
func TestScriptExecutionMountTargetsAreCanonicalAndIsolated(t *testing.T) {
	for _, target := range []string{
		"", "/", "relative", "/data/../other", "/data/", "//data", "/data//child", "/data\x00",
		"/proc", "/proc/x", "/sys/x", "/dev", "/run/secrets", "/var/run", "/var",
		"/bin/sh", "/sbin", "/usr/lib", "/lib", "/lib64", "/etc/hosts", "/etc",
		"/groundplane-script-body", "/groundplane-script-body/child",
	} {
		t.Run(target, func(t *testing.T) {
			execution := validExplicitScriptExecution()
			execution.Volumes[0].Target = target
			if err := execution.Validate(); err == nil {
				t.Fatal("unsafe target accepted")
			}
		})
	}
	for _, target := range []string{"/data", "/data-other", "/etc/tls", "/var/lib/app", "/processor"} {
		execution := validExplicitScriptExecution()
		execution.Volumes[0].Target = target
		if err := execution.Validate(); err != nil {
			t.Fatalf("safe target %s rejected: %v", target, err)
		}
	}
}

// Rationale: grant lists are bounded before resolution and cannot amplify
// one Script into an unbounded source or publication request.
func TestScriptExecutionGrantBounds(t *testing.T) {
	execution := validExplicitScriptExecution()
	execution.Volumes = nil
	execution.EntryIDs = nil
	for index := range MaximumScriptExecutionVolumes {
		execution.Volumes = append(execution.Volumes, ScriptVolumeGrant{
			VolumeID: ids.New(ids.KindVolume), Target: "/data/" + strconv.Itoa(index), ReadOnly: true,
		})
	}
	for range MaximumScriptExecutionEntries {
		execution.EntryIDs = append(execution.EntryIDs, ids.New(ids.KindEnvEntry))
	}
	if err := execution.Validate(); err != nil {
		t.Fatalf("maximum grants rejected: %v", err)
	}
	execution.Volumes = append(execution.Volumes, ScriptVolumeGrant{
		VolumeID: ids.New(ids.KindVolume), Target: "/extra", ReadOnly: false,
	})
	if err := execution.Validate(); err == nil {
		t.Fatal("excess Volume grants accepted")
	}
	execution.Volumes = execution.Volumes[:MaximumScriptExecutionVolumes]
	execution.EntryIDs = append(execution.EntryIDs, ids.New(ids.KindEnvEntry))
	if err := execution.Validate(); err == nil {
		t.Fatal("excess Entry grants accepted")
	}
}

const (
	scriptExecutionTestImage  = "example/setup@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	scriptExecutionTestVolume = "vol_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	scriptExecutionTestEntry  = "ev_01ARZ3NDEKTSV4RRFFQ69G5FAV"
)

func validExplicitScriptExecution() ScriptExecution {
	return ScriptExecution{
		Mode: ScriptExecutionExplicit, Image: scriptExecutionTestImage, User: "0:0",
		Volumes:  []ScriptVolumeGrant{{VolumeID: scriptExecutionTestVolume, Target: "/data", ReadOnly: false}},
		EntryIDs: []string{scriptExecutionTestEntry},
	}
}
