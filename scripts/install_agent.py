"""Install an independently selected Agent through the existing Controller Task."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import subprocess
import time

from controller_update import Receipt, TASK_ID, TERMINAL, Transport


def current_image():
    result = subprocess.run(["docker", "inspect", "groundplane-agent"], capture_output=True, text=True, timeout=30)
    if result.returncode:
        raise ValueError("no installed Agent; enroll it through the Controller first")
    runtime = json.loads(result.stdout)[0]
    labels = runtime["Config"].get("Labels") or {}
    if (not runtime.get("State", {}).get("Running") or labels.get("com.groundplane.managed") != "true"
            or labels.get("com.groundplane.kind") != "agent"):
        raise ValueError("installed Agent runtime is stopped or has foreign ownership")
    image = runtime["Config"]["Image"]
    if not re.fullmatch(r"[a-z0-9][a-z0-9._:/\[\]-]*@sha256:[0-9a-f]{64}", image):
        raise ValueError("installed Agent image is not digest-pinned")
    return image


def installed_image():
    status, document = Transport().request("GET", "/agents")
    if status != 200 or not isinstance(document, dict) or not isinstance(document.get("items"), list):
        raise ValueError("cannot read Agent inventory")
    if len(document["items"]) == 0:
        return None
    if len(document["items"]) != 1:
        raise ValueError("Agent singleton invariant violated")
    return current_image()


def install(image, *, allow_enroll=False):
    if not re.fullmatch(r"[a-z0-9][a-z0-9._:/\[\]-]*@sha256:[0-9a-f]{64}", image):
        raise ValueError("Agent installation requires an immutable image")
    transport = Transport()
    status, agents = transport.request("GET", "/agents")
    if status != 200 or not isinstance(agents, dict) or not isinstance(agents.get("items"), list):
        raise ValueError("Agent inventory unavailable")
    absent = len(agents["items"]) == 0
    if absent and not allow_enroll or len(agents["items"]) > 1:
        raise ValueError("Agent-only installation requires an enrolled local Agent and running Controller")
    agent = agents["items"][0] if not absent else None
    identity = agent["id"] if agent else "enrollment"
    if not absent and not re.fullmatch(r"agt_[0-7][0-9A-HJKMNP-TV-Z]{25}", identity):
        raise ValueError("Controller returned an invalid Agent identity")
    root = Path("/var/lib/groundplane/agent-updates")
    root.mkdir(mode=0o700, exist_ok=True)
    receipt = Receipt(root, 0)
    try:
        active = receipt.read()
        enrollment_release = "sha256:" + hashlib.sha256(("enrollment\n" + image).encode()).hexdigest()
        enrolling = absent or (active is not None and active["status"] not in TERMINAL and
                               active["release"] == enrollment_release)
        if not absent and (active is None or active["status"] in TERMINAL) and current_image() == image and agent["status"] == "healthy":
            print("Agent is already installed and healthy; skipped.")
            return
        subprocess.run(["docker", "pull", image], check=True, timeout=600)
        release = enrollment_release if enrolling else "sha256:" + hashlib.sha256((identity + "\n" + image).encode()).hexdigest()
        value = receipt.begin(release, "agent-install-" + secrets.token_hex(16))
        if not value["task_id"]:
            status, accepted = transport.request("POST", "/agents" if enrolling else f"/agents/{identity}/update",
                                                 None if enrolling else {"image": image}, value["key"])
            if status != 202 or not isinstance(accepted, dict) or not TASK_ID.fullmatch(accepted.get("task_id", "")):
                raise ValueError(f"Agent update not accepted (HTTP {status}); receipt retained")
            value["task_id"] = accepted["task_id"]
            receipt.write(value)
        print(f"Agent update Task: {value['task_id']}", flush=True)
        deadline = time.monotonic() + 330
        while time.monotonic() < deadline:
            status, task = transport.request("GET", "/tasks/" + value["task_id"])
            if status != 200:
                raise ValueError("Agent Task outcome unavailable; receipt retained")
            state = task.get("status")
            if state in TERMINAL:
                value["status"] = state
                receipt.write(value)
                if state != "completed":
                    raise ValueError(f"Agent update {state}; inspect retained Task and recovery evidence")
                if current_image() != image:
                    raise ValueError("completed Agent update has a different runtime image")
                print("Agent update completed.")
                return
            if state not in {"pending", "running"}:
                raise ValueError("unknown Agent Task state; receipt retained")
            time.sleep(1)
        raise ValueError("Agent update deadline reached; receipt retained, Task not cancelled")
    finally:
        receipt.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", required=True)
    args = parser.parse_args()
    if os.geteuid() != 0:
        parser.error("Agent installation requires root")
    install(args.image)


if __name__ == "__main__":
    main()
