# Groundplane

A small self-hosted tool to run many projects on one machine without
hand-maintaining Docker Compose.

- **Controller** - holds the desired state, serves the Console, and tells the
  Agent what should be running.
- **Agent** - runs on the machine, applies that state through Docker, and
  reports observed state back.

The model: **Tenant -> Project -> Environment -> Services**, where Projects are
either tenant-scoped applications (possibly microservice architectures) or
**backing services** that are shared infrastructure built once and attached by
any tenant. See docs/mvp.md for the contract.

This repository currently contains only the **prototype** and the product
definition in docs/mvp.md. There is no Controller or Agent
implementation yet. The implementation contract (Go, Cobra CLI, unified
logging, shared common project) lives in docs/architecture.md.

## Run the prototype

    cd prototype
    python3 -m http.server 8091 --bind 127.0.0.1

Then open http://127.0.0.1:8091/.
