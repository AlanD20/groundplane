package systemd

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: recovery must be manager-owned rather than a child/scope killed
// with Controller. Its executable, environment and mutation target are closed.
func TestNativeRecoveryCommand(t *testing.T) {
	fake := runner.NewFake()
	control, err := NewControllerUpgrade(fake)
	if err != nil {
		t.Fatal(err)
	}
	id := ids.NewAt(ids.KindTask, time.Date(2026, 9, 10, 0, 0, 0, 0, time.UTC), 1)
	if err := control.Launch(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	if len(fake.Calls) != 1 {
		t.Fatalf("commands = %d", len(fake.Calls))
	}
	call := fake.Calls[0]
	joined := strings.Join(call.Args, " ")
	for _, required := range []string{"--property=Type=exec", "--property=Restart=on-failure",
		"--expand-environment=no", "/var/lib/groundplane/controller-updates/previous --upgrade-run " + id} {
		if !strings.Contains(joined, required) {
			t.Fatalf("missing %q in %s", required, joined)
		}
	}
	if call.Name != "/usr/bin/systemd-run" || strings.Contains(joined, "--scope") ||
		!call.ReplaceEnv || call.Timeout <= 0 || call.CaptureLimitBytes <= 0 {
		t.Fatalf("unsafe launch: %#v", call)
	}
	if err := control.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := control.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, call := range fake.Calls[1:] {
		if call.Name != "/usr/bin/systemctl" ||
			call.Args[len(call.Args)-1] != "groundplane-controller.service" {
			t.Fatalf("wrong target: %#v", call)
		}
	}
	before := len(fake.Calls)
	if err := control.Launch(context.Background(), "../other"); err == nil ||
		len(fake.Calls) != before {
		t.Fatal("invalid Task reached subprocess boundary")
	}
}

// Rationale: a lost launcher response can leave a real watchdog running. Only
// the same Task's fixed predecessor command may prove that handoff succeeded.
func TestNativeRecoveryLaunchResolvesSameTaskUnit(t *testing.T) {
	id := ids.NewAt(ids.KindTask, time.Date(2026, 9, 10, 0, 0, 0, 0, time.UTC), 1)
	for _, matching := range []bool{false, true} {
		fake := runner.NewFake()
		fake.RunFunc = func(_ context.Context, call runner.RunCmdOpts) (runner.Result, error) {
			if call.Name == "/usr/bin/systemd-run" {
				return runner.Result{}, errors.New("response lost")
			}
			command := "/unrelated/helper"
			if matching {
				command = "/var/lib/groundplane/controller-updates/previous --upgrade-run " + id
			}
			return runner.Result{Stdout: []byte("{ argv[]=" + command + " ; }")}, nil
		}
		control, err := NewControllerUpgrade(fake)
		if err != nil {
			t.Fatal(err)
		}
		err = control.Launch(context.Background(), id)
		if (err == nil) != matching {
			t.Fatalf("matching=%t error=%v", matching, err)
		}
	}
}

// Rationale: the first upgrade-capable bootstrap is required; without its
// configured pre-start guard, a broken candidate could prevent native recovery.
func TestNativeRecoveryRequiresInstalledGuard(t *testing.T) {
	fake := runner.NewFake()
	control, err := NewControllerUpgrade(fake)
	if err != nil {
		t.Fatal(err)
	}
	if err := control.VerifyBootstrap(context.Background()); !errors.Is(
		err,
		errs.New(errs.KindStateConflict, ""),
	) {
		t.Fatalf("unguarded bootstrap: %v", err)
	}
	fake.RunFunc = func(_ context.Context, call runner.RunCmdOpts) (runner.Result, error) {
		command := "/usr/local/libexec/groundplane/controller"
		if strings.Contains(strings.Join(call.Args, " "), "ExecStartPre") {
			command = "/usr/local/libexec/groundplane/controller-recovery --upgrade-guard"
		}
		return runner.Result{Stdout: []byte("{ argv[]=" + command + " ; }")}, nil
	}
	if err := control.VerifyBootstrap(context.Background()); err != nil {
		t.Fatal(err)
	}
}
