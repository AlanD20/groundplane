package publicingress

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/moby/moby/client"
)

// guardedReload persists Docker's exact exec identity before starting the
// reload. Recovery can accept a positive terminal response independently of
// whether the previous Agent observed it.
type guardedReload struct {
	*Caddy
	state string
}

const reloadMarker = "reload-pending.json"

type reloadProcess struct {
	RuntimeID string `json:"runtime_id"`
	ExecID    string `json:"exec_id"`
}

func (runtime *guardedReload) Reload(ctx context.Context, id string, source []byte) error {
	cfg, err := caddyConfiguration(source)
	if err != nil {
		return err
	}
	if err := runtime.verify(ctx, id); err != nil {
		return err
	}
	if err := settlePriorReload(ctx, runtime.state, id); err != nil {
		return err
	}
	bounded, cancel := context.WithTimeout(ctx, dockerTimeout)
	defer cancel()
	engine, err := client.New(client.WithHost(hostDockerSocket))
	if err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	defer engine.Close()
	command := []string{"caddy", "reload", "--address", adminListen, "--config", "-"}
	exec, err := engine.ExecCreate(bounded, id, client.ExecCreateOptions{
		AttachStdin: true, AttachStdout: true, AttachStderr: true, Cmd: command,
	})
	if err != nil || !validRuntimeID(exec.ID) {
		return conflict("public ingress could not capture the exact reload process")
	}
	process := reloadProcess{RuntimeID: id, ExecID: exec.ID}
	if err := writeReloadProcess(runtime.state, &process); err != nil {
		return err
	}
	if err := runReloadProcess(bounded, engine, exec.ID, cfg); err != nil {
		return err
	}
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := settlePriorReload(bounded, runtime.state, id); err == nil {
			return runtime.verify(ctx, id)
		}
		select {
		case <-bounded.Done():
			return conflict("public ingress reload completion could not be confirmed")
		case <-ticker.C:
		}
	}
}

func runReloadProcess(ctx context.Context, engine *client.Client, id string, cfg []byte) error {
	stream, err := engine.ExecAttach(ctx, id, client.ExecAttachOptions{})
	if err != nil || stream.Conn == nil {
		return conflict("public ingress could not start the captured reload")
	}
	defer stream.Close()
	stopClose := context.AfterFunc(ctx, stream.Close)
	defer stopClose()
	n, err := stream.Conn.Write(cfg)
	if err != nil || n != len(cfg) || stream.CloseWrite() != nil || stream.Reader == nil {
		return conflict("public ingress could not send the full captured reload configuration")
	}
	count, err := io.Copy(io.Discard, io.LimitReader(stream.Reader, maxOutput+1))
	if err != nil || count > maxOutput {
		return conflict("public ingress could not read the bounded reload response")
	}
	return nil
}

func writeReloadProcess(path string, process *reloadProcess) error {
	// Store.Apply and Store.Recover already hold the directory lock.
	root, err := os.OpenRoot(path)
	if err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	defer root.Close()
	file, err := root.Open(".")
	if err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	defer file.Close()
	if process != nil {
		value, err := json.Marshal(process)
		if err != nil {
			return errs.Wrap(errs.KindInternal, err)
		}
		return (&stateDirectory{root: root, file: file}).replace(reloadMarker, value)
	}
	if err := root.Remove(reloadMarker); err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	return file.Sync()
}

func settlePriorReload(ctx context.Context, path, runtimeID string) error {
	root, err := os.OpenRoot(path)
	if err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	defer root.Close()
	raw, found, err := (&stateDirectory{root: root}).read(reloadMarker, 512)
	if err != nil || !found {
		return err
	}
	var process reloadProcess
	if err := json.Unmarshal(raw, &process); err != nil ||
		process.RuntimeID != runtimeID || !validRuntimeID(process.ExecID) {
		return conflict("public ingress reload marker has a different runtime identity")
	}
	if err := inspectReloadExit(ctx, process); err != nil {
		return err
	}
	return writeReloadProcess(path, nil)
}

func inspectReloadExit(ctx context.Context, process reloadProcess) error {
	// The narrow SDK result flattens Docker's nullable ExitCode to zero. A
	// never-started process can therefore appear successful through that port.
	// Preserve null in this one bounded read of the exact persisted exec.
	transport := &http.Transport{
		DisableKeepAlives: true,
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", "/var/run/docker.sock")
		},
	}
	httpClient := &http.Client{Transport: transport, Timeout: 2 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://docker/exec/"+process.ExecID+"/json", nil)
	if err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	response, err := httpClient.Do(req)
	if err != nil {
		return conflict("public ingress reload has no independent terminal observation")
	}
	defer response.Body.Close()
	var status struct {
		ID, ContainerID string
		Running         bool
		ExitCode        *int
	}
	decoder := json.NewDecoder(io.LimitReader(response.Body, 16<<10))
	if response.StatusCode != http.StatusOK || decoder.Decode(&status) != nil ||
		!errors.Is(decoder.Decode(&struct{}{}), io.EOF) || status.ID != process.ExecID ||
		status.ContainerID != process.RuntimeID || status.Running || status.ExitCode == nil || *status.ExitCode != 0 {
		return conflict("public ingress reload has no independent successful process result")
	}
	return nil
}
