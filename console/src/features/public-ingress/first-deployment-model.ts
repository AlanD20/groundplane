import { isMap, parseDocument } from "yaml";
import {
  applyFirstDeploymentAttachments,
  type FirstDeploymentAttachment,
  type FirstDeploymentBackingOption,
} from "./first-deployment-attachment-model.ts";

export type FirstDeploymentEntry = {
  id: string;
  key: string;
  source: "literal" | "secret_ref";
  value: string;
};

export type FirstDeploymentVolume = {
  id: string;
  key: string;
  target: string;
  existing: boolean;
  readOnly: boolean;
};

export type FirstDeploymentMigration = {
  slug: string;
  script: string;
};

export type FirstDeploymentDraft = {
  service: string;
  image: string;
  port: string;
  healthPath: string;
  zone: string;
  subnet: string;
  newService: boolean;
  newZone: boolean;
  entries: FirstDeploymentEntry[];
  volumes: FirstDeploymentVolume[];
  migration?: FirstDeploymentMigration;
  attachments?: FirstDeploymentAttachment[];
};

const resourceSlug = /^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$/;

export function firstDeploymentBlueprint(
  source: string,
  draft: FirstDeploymentDraft,
  backingOptions: readonly FirstDeploymentBackingOption[] = [],
): string {
  const document = parseDocument(source, { intAsBigInt: true });
  if (
    document.errors.length ||
    document.warnings.length ||
    !isMap(document.contents)
  )
    throw new Error(
      "The current Environment Blueprint is unavailable for this workflow.",
    );

  const service = draft.service.trim();
  const image = draft.image.trim();
  if (
    !/^[a-z0-9]+(?:[-_][a-z0-9]+)*$/.test(service) ||
    !image ||
    /\s/.test(image)
  )
    throw new Error("Enter a Service name and an OCI image reference.");
  const servicePath = ["services", service];
  if (draft.newService === document.hasIn(servicePath))
    throw new Error(
      "The selected Service changed. Reload its Environment and review again.",
    );

  if (draft.newService) {
    const port = Number(draft.port);
    if (!/^\d+$/.test(draft.port) || port < 1 || port > 65535)
      throw new Error("Enter a container port between 1 and 65535.");
    if (!draft.healthPath.startsWith("/") || /[\s#]/.test(draft.healthPath))
      throw new Error(
        "Enter a health path beginning with / and without a fragment.",
      );
    const zone = draft.zone.trim();
    if (!/^[a-z0-9]+(?:[-_][a-z0-9]+)*$/.test(zone))
      throw new Error("Select a Zone or enter a private Zone name.");
    const zonePath = ["networks", zone];
    if (draft.newZone) {
      if (document.hasIn(zonePath))
        throw new Error("That Zone already exists. Select it from the list.");
      const subnet = draft.subnet.trim();
      if (!subnet) throw new Error("Enter the private Zone subnet.");
      document.setIn(zonePath, {
        internal: true,
        ipam: { config: [{ subnet }] },
      });
    } else if (!document.hasIn(zonePath)) {
      throw new Error(
        "The selected Zone is missing from the current Blueprint.",
      );
    }
    const healthURL = new URL(`http://127.0.0.1:${port}${draft.healthPath}`);
    if (healthURL.hostname !== "127.0.0.1")
      throw new Error("Use a local application health path.");
    document.setIn(
      servicePath,
      document.createNode({
        image,
        expose: [String(port)],
        networks: [zone],
        healthcheck: {
          test: ["CMD", "curl", "-fsS", "--", healthURL.href],
          interval: "10s",
          timeout: "3s",
          retries: 5,
        },
        "x-gp-release": { default_strategy: "recreate" },
      }),
    );
    addStorageAndMigration(document, service, draft);
  } else {
    document.setIn([...servicePath, "image"], image);
  }

  for (const entry of draft.entries) {
    const key = entry.key.trim();
    const path = ["x-gp-entry", key];
    if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(key))
      throw new Error(
        "Configuration keys must be valid environment variable names.",
      );
    if (document.hasIn(path))
      throw new Error(
        `Configuration already contains ${key}. Manage shared values in the Environment settings.`,
      );
    if (entry.source === "secret_ref" && !entry.value.trim())
      throw new Error(`Choose an existing Secret reference for ${key}.`);
    document.setIn(path, {
      kind: "env",
      source: {
        [entry.source]:
          entry.source === "secret_ref" ? entry.value.trim() : entry.value,
      },
      exposure: [service],
      secret: entry.source === "secret_ref",
    });
  }

  applyFirstDeploymentAttachments(
    document,
    service,
    draft.attachments ?? [],
    backingOptions,
  );
  return document.toString({ indent: 2, lineWidth: 100 });
}

function addStorageAndMigration(
  document: ReturnType<typeof parseDocument>,
  service: string,
  { volumes, migration }: FirstDeploymentDraft,
) {
  const targets = new Set<string>();
  const declaredVolumes = document.get("volumes", true);
  const existingVolumeSlugs = isMap(declaredVolumes)
    ? declaredVolumes.items.map(
        ({ key, value }) =>
          (isMap(value) ? value.get("x-gp-slug") : undefined) ?? String(key),
      )
    : [];
  const mounts = volumes.map((volume) => {
    const key = volume.key.trim();
    const target = volume.target.trim();
    const path = ["volumes", key];
    if (
      !target.startsWith("/") ||
      target
        .slice(1)
        .split("/")
        .some((part) => !part || part === "." || part === "..") ||
      target.includes("\0")
    )
      throw new Error("Use a clean absolute container path such as /app/data.");
    if (targets.has(target))
      throw new Error(
        `More than one Volume uses the container path ${target}.`,
      );
    targets.add(target);

    if (volume.existing) {
      if (!key || !document.hasIn(path))
        throw new Error("Select an existing Volume from this Environment.");
    } else {
      if (!resourceSlug.test(key))
        throw new Error(
          "Use up to 63 lowercase letters, digits, and hyphens for the Volume name. Start and end with a letter or digit.",
        );
      if (document.hasIn(path) || existingVolumeSlugs.includes(key))
        throw new Error(`Volume ${key} already exists. Select it to mount it.`);
      document.setIn(path, {});
    }
    return { type: "volume", source: key, target, read_only: volume.readOnly };
  });
  if (mounts.length) document.setIn(["services", service, "volumes"], mounts);

  if (!migration) return;
  const slug = migration.slug.trim();
  const script = migration.script;
  const path = ["x-gp-scripts", slug];
  if (!resourceSlug.test(slug))
    throw new Error(
      "Use up to 63 lowercase letters, digits, and hyphens for the Script name. Start and end with a letter or digit.",
    );
  if (
    !script.trim() ||
    script.includes("\0") ||
    new TextEncoder().encode(script).length > 65_536
  )
    throw new Error(
      "Enter a migration script up to 64 KiB without NUL characters.",
    );
  const scripts = document.get("x-gp-scripts", true);
  if (
    document.hasIn(path) ||
    (isMap(scripts) &&
      scripts.items.some(
        ({ value }) => isMap(value) && value.get("slug") === slug,
      ))
  )
    throw new Error(`Script ${slug} already exists in this Environment.`);
  document.setIn(path, { slug, service, when: "pre-deploy", script });
}
