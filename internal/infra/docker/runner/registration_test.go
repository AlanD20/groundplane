package runner

import (
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// Rationale: execute the real entrypoint against a recording GitHub configurator
// so a custom group cannot silently fall back to Default or consume token input.
func TestRegistrationPassesCapturedGroupAsOneArgument(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	files := map[string]string{
		".groundplane-runner-version": "0.0.1\n",
		"cat":                         "#!/bin/sh\nprintf '0.0.1\\n'\n",
		"config.sh":                   "#!/bin/sh\ntest \"$ACTIONS_RUNNER_INPUT_TOKEN\" = transient-token || exit 1\nprintf '%s\\n' \"$@\" > registration-arguments\n",
		"run.sh":                      "#!/bin/sh\nexit 0\n",
	}
	for name, content := range files {
		if err := os.WriteFile(filepath.Join(directory, name), []byte(content), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	entrypoint, err := filepath.Abs("../../../../release/runner/entrypoint.sh")
	if err != nil {
		t.Fatal(err)
	}
	plan := validRuntimePlan(t)
	plan.Container.RunnerGroup = "Project builders"
	plan.Container.RunnerName = "project-ci"
	command := exec.Command("sh", entrypoint)
	command.Dir = directory
	command.Env = append(os.Environ(), "PATH="+directory+":"+os.Getenv("PATH"))
	command.Stdin = strings.NewReader(string(registrationDocument(plan, []byte("transient-token"))))
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("entrypoint: %v: %s", err, output)
	}
	arguments, err := os.ReadFile(filepath.Join(directory, "registration-arguments"))
	if err != nil {
		t.Fatal(err)
	}
	values := strings.Split(strings.TrimSpace(string(arguments)), "\n")
	index := slices.Index(values, "--runnergroup")
	if index < 0 || index+1 >= len(values) || values[index+1] != "Project builders" {
		t.Fatalf("registered arguments = %q", values)
	}
	if slices.Contains(values, "transient-token") {
		t.Fatal("registration token leaked into process arguments")
	}
	nameIndex := slices.Index(values, "--name")
	if nameIndex < 0 || nameIndex+1 >= len(values) || values[nameIndex+1] != "project-ci" {
		t.Fatalf("registered name = %q", values)
	}
}
