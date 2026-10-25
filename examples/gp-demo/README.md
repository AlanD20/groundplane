# Frontend and API demo

Deploy two small Services with Groundplane and publish them through Caddy and
Cloudflare Tunnel. The frontend is static HTML served by nginx; the API uses
Node.js's built-in HTTP server, with no package installation or database.

```text
Browser → Cloudflare Tunnel → Caddy → frontend  /
                                  → api       /api/*
```

The page calls `GET /api/info` and submits messages to `POST /api/echo`. Responses
show the API version, container hostname and request count. The count lives in
memory and resets when the API restarts. `/api/health` checks API readiness.

## Prepare a working copy

Use a GP installation with its CLI, Docker and a Cloudflare Tunnel token. Run
the build commands on the host managed by GP's Agent: images built in another
machine's Docker daemon are not automatically available to GP.

From the repository root, make an editable copy outside the tracked example:

```sh
mkdir -p .tmp
cp -R examples/gp-demo .tmp/gp-demo-example
cd .tmp/gp-demo-example
```

In `blueprint/groundplane.yml`, replace both `app.example.com` hostnames with
your public hostname. Choose an unused network pool; if you change it, update
the two network subnets to stay inside it. The sample uses `10.42.0.0/24`.

Only `blueprint/` is the import bundle. Do not import the whole project directory
or put credentials in that bundle.

## Build the images

```sh
docker build -t gp-demo-api:0.1.0 backend
docker build -t gp-demo-frontend:0.1.0 frontend
```

These tags match the Blueprint. GP captures the selected image content when
deploying. Both Services include explicit healthchecks and use Recreate, which
can interrupt requests during a deployment.

## Create and deploy the Environment

The commands below use a fresh `demo` Tenant, `gp-demo` Project and `live`
Environment. If those already exist, use them or choose different labels and
update the Blueprint metadata to match.

```sh
groundplane tenant create demo --name Demo
groundplane --tenant demo project create gp-demo --name "GP Demo"
groundplane --tenant demo --project gp-demo environment create live \
  --network-pool 10.42.0.0/24
```

Wait for Environment creation to succeed, then validate and apply:

```sh
groundplane --tenant demo --project gp-demo environment blueprint validate live \
  --bundle-dir ./blueprint --root groundplane.yml
groundplane --tenant demo --project gp-demo environment blueprint apply live \
  --bundle-dir ./blueprint --root groundplane.yml
```

Follow the returned Task in the Console or with `groundplane task show TASK_ID`,
using the actual returned ID. Wait for success before configuring ingress.
The Services are reachable only on their private network at this stage; their
ports are not published on the host.

## Enable Caddy and Tunnel

Open **Demo → GP Demo → live → Proxy & tunnel** in the Console.

1. Enable the HTTP router using **Caddy** and its default template. Select
   `edge` first, then `application`, and set the alias to `gp-demo-router`.
   Wait for the Task to succeed.
2. Enable **Cloudflare Tunnel**, select the `edge` Zone, and supply your Tunnel
   token using a Project Secret. Wait for the Task to succeed.

The `application` Zone is internal. Caddy joins it to reach the two Services;
the Tunnel joins `edge` to reach Caddy and make outbound Cloudflare connections.
Neither application Service needs direct outbound access.

Component images must be present on the Agent host. If one is missing, use
**Host → Images → Fetch** to fetch the exact version shown for that Component,
then retry the appropriate action. The example's own images were built above.
See [image management](../../docs/features/platform.md) and
[Component configuration](../../docs/features/components.md) for these actions.

Enabling Components adds their declarations to the Environment Blueprint.
They are configured after the initial import because their Zone and Secret IDs
belong to your installation. Export the resulting complete Blueprint before
making later changes; reapplying the starter file without those declarations
would disable the Components:

```sh
groundplane --tenant demo --project gp-demo environment blueprint show live \
  > blueprint/groundplane.yml
```

## Publish the hostname

On the same Cloudflare Tunnel whose token you supplied, add a published
application for your hostname with origin **`http://gp-demo-router:80`**.
Use the DNS record Cloudflare provides. Preserve the original HTTP Host header
so Caddy can select the Routes; do not override it with the router's internal name.

Open `https://YOUR_HOSTNAME` and try both buttons. You can also check:

```sh
curl https://YOUR_HOSTNAME/api/info
curl https://YOUR_HOSTNAME/api/echo \
  -H 'Content-Type: application/json' \
  --data '{"message":"Hello through Groundplane"}'
```

GP manages the application containers, networks, Caddy and Tunnel connector.
Cloudflare owns public DNS and hostname forwarding. This setup publishes only
the demo; keep the GP Console and management API on their trusted private listener.

For application updates, build a new image tag, change the corresponding Service
image in the saved Blueprint, then Apply or Deploy through GP. For removal, use
the Project's normal deletion workflow in the Console.
