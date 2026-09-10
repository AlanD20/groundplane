"""Bounded OCI archive transport; no Controller or application lifecycle actions."""
import shlex
import re
import subprocess
import time

BYTES_PER_SECOND = 4 << 20
CHUNK_BYTES = 64 << 10


def prepare_images(ssh, images, *, run=subprocess.run):
    missing = []
    for name in images:
        if re.fullmatch(r"groundplane-(?:agent|runner):deploy-[0-9a-f]{32}", name) is None:
            raise ValueError("image reuse requires an invocation-owned image name")
        result = run(["docker", "image", "inspect", "--format", "{{.Id}}", name],
                     check=True, capture_output=True, text=True, timeout=30)
        identity = result.stdout.strip()
        if re.fullmatch(r"sha256:[0-9a-f]{64}", identity) is None:
            raise ValueError("local image content identity is invalid")
        inspection = shlex.join(["docker", "image", "inspect", "--format", "{{.Id}}", identity])
        result = run([*ssh, inspection], check=False, capture_output=True, text=True, timeout=30)
        if result.returncode == 1:
            missing.append(name)
            continue
        if result.returncode != 0:
            raise subprocess.CalledProcessError(result.returncode, [*ssh, inspection])
        if result.stdout.strip() != identity:
            raise ValueError("target image content identity differs")
        # This aliases verified local content for the existing remote publisher;
        # runtime identity is still the registry-reported RepoDigest, never this id.
        run([*ssh, shlex.join(["docker", "tag", identity, name])], check=True, timeout=30)
        print(f"Reusing target image content: {name} ({identity})", flush=True)
    return tuple(missing)


def copy_paced(source, destination, rate, now=time.monotonic, sleep=time.sleep):
    if type(rate) is not int or rate <= 0:
        raise ValueError("image transfer rate must be positive")
    next_write = now()
    total = 0
    while chunk := source.read(CHUNK_BYTES):
        delay = next_write - now()
        if delay > 0:
            sleep(delay)
        offset = 0
        while offset < len(chunk):
            written = destination.write(chunk[offset:])
            if not written:
                raise BrokenPipeError("image receiver stopped accepting bytes")
            offset += written
        destination.flush()
        total += len(chunk)
        # A slow receiver does not earn a later catch-up burst.
        next_write = now() + len(chunk) / rate
    return total


def stop_child(process):
    if process.poll() is None:
        process.terminate()
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


def transfer_images(ssh, images, *, spawn=subprocess.Popen):
    if not images:
        return
    save_command = ["docker", "save", *images]
    load_command = [*ssh[:-1], "-o", "ServerAliveInterval=15", "-o", "ServerAliveCountMax=2",
                    ssh[-1], "docker", "load"]
    print(f"+ {shlex.join(save_command)} | paced 4 MiB/s | {shlex.join(load_command)}", flush=True)
    processes = []
    try:
        save = spawn(save_command, stdout=subprocess.PIPE)
        processes.append(save)
        load = spawn(load_command, stdin=subprocess.PIPE)
        processes.append(load)
        if save.stdout is None or load.stdin is None:
            raise RuntimeError("image transfer did not expose its owned pipes")
        count = copy_paced(save.stdout, load.stdin, BYTES_PER_SECOND)
        load.stdin.close()
        save.stdout.close()
        save_status = save.wait(timeout=60)
        load_status = load.wait(timeout=300)
        if save_status != 0:
            raise subprocess.CalledProcessError(save_status, save_command)
        if load_status != 0:
            raise subprocess.CalledProcessError(load_status, load_command)
        print(f"Image archive transferred: {count} bytes (bounded 4 MiB/s)", flush=True)
    except BaseException:
        cleanup_errors = []
        for process in processes:
            try:
                stop_child(process)
            except (OSError, subprocess.TimeoutExpired) as error:
                cleanup_errors.append(error)
        if cleanup_errors:
            raise RuntimeError("image transfer child cleanup failed") from cleanup_errors[0]
        raise
    finally:
        for process in processes:
            for pipe in (process.stdin, process.stdout):
                if pipe is not None:
                    pipe.close()
