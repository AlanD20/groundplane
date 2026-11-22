package composehelper

import (
	"bytes"
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// A hash that excludes env_file values falsely rejects healthy consumers and
// can leave both Entry recovery and software activation permanently blocked.
func TestEntryHashResolvesFileValuesAndClearsCredentialBuffers(t *testing.T) {
	resolved := []byte(`{"name":"test","services":{"app":{"image":"busybox","environment":{"TOKEN":"literal$VALUE"}}}}`)
	expected := strings.Repeat("a", 64)
	fake := runner.NewFake()
	count := 0
	var hashInput []byte
	fake.RunFunc = func(_ context.Context, command runner.RunCmdOpts) (runner.Result, error) {
		count++
		if count == 1 {
			if !slices.Equal(command.Args[len(command.Args)-3:], []string{"config", "--format", "json"}) {
				t.Fatal("configuration was not resolved before hash verification")
			}
			return runner.Result{Stdout: resolved}, nil
		}
		if count != 2 || !bytes.Contains(command.Stdin, []byte("literal$$VALUE")) ||
			bytes.Contains(command.Stdin, []byte("env_file")) {
			t.Fatal("hash did not receive the resolved literal credential value")
		}
		hashInput = command.Stdin
		return runner.Result{Stdout: []byte("app " + expected + "\n")}, nil
	}
	result, err := entryConsumerHash(t.Context(), fake, &agentpb.ComposeArtifact{
		ProjectName: "test", CanonicalYaml: []byte("services:\n  app:\n    image: busybox\n    env_file: secret.env\n"),
	}, "app")
	if err != nil || string(result.Stdout) != "app "+expected+"\n" || count != 2 {
		t.Fatalf("hash failed: count=%d error=%v", count, err)
	}
	for _, buffer := range [][]byte{resolved, hashInput} {
		if !bytes.Equal(buffer, make([]byte, len(buffer))) {
			t.Fatal("resolved credentials survived hash verification")
		}
	}
}

func TestEntryHashResolutionFailureNeverReachesHashOrExposesOutput(t *testing.T) {
	secret := []byte("private-resolution-output")
	fake := runner.NewFake()
	fake.RunFunc = func(_ context.Context, _ runner.RunCmdOpts) (runner.Result, error) {
		return runner.Result{ExitCode: 1, Stdout: secret, Stderr: []byte("private-error")}, nil
	}
	result, err := entryConsumerHash(
		t.Context(),
		fake,
		&agentpb.ComposeArtifact{ProjectName: "test", CanonicalYaml: []byte("services: {}")},
		"app",
	)
	if err == nil || len(result.Stdout) != 0 || strings.Contains(err.Error(), "private") ||
		len(fake.RecordedCalls()) != 1 ||
		!bytes.Equal(secret, make([]byte, len(secret))) {
		t.Fatal("failed resolution leaked output or reached consumer verification")
	}
}
