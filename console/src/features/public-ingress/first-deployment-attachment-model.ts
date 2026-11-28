import { isMap, type Document } from "yaml";

export type FirstDeploymentBackingOption = {
  value: string;
  label: string;
  backingProject: string;
  backingService: string;
  description?: string;
  urlFact?: { key: string; secret: boolean };
};

export type FirstDeploymentAttachment = {
  id: string;
  name: string;
  backing: string;
  environmentKey?: string;
};

const attachmentsKey = "x-gp-attachments";
const entriesKey = "x-gp-entry";

export function applyFirstDeploymentAttachments(
  document: Document,
  service: string,
  attachments: readonly FirstDeploymentAttachment[],
  options: readonly FirstDeploymentBackingOption[],
) {
  if (attachments.length === 0) return;
  if (document.has(attachmentsKey) && !isMap(document.get(attachmentsKey)))
    throw new Error(
      "Review the existing Backing connections in the Blueprint.",
    );

  const names = new Set<string>();
  const entryKeys = new Set<string>();
  const connections = attachments.map((attachment) => {
    const name = attachment.name.trim();
    if (name.length > 255 || !/^[a-z0-9]+(?:-[a-z0-9]+)*$/.test(name))
      throw new Error(
        "Enter a connection name of up to 255 lowercase letters, digits, and single hyphens between words.",
      );
    if (names.has(name) || document.hasIn([attachmentsKey, name]))
      throw new Error(`A Backing connection named ${name} already exists.`);
    names.add(name);

    const option = options.find(({ value }) => value === attachment.backing);
    if (!option)
      throw new Error(`Choose an available Backing Service for ${name}.`);

    const environmentKey = attachment.environmentKey?.trim();
    if (!environmentKey) return { name, option };
    if (!option.urlFact)
      throw new Error(
        `The selected Backing Service has no connection URL for ${name}.`,
      );
    if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(environmentKey))
      throw new Error(
        "Enter a valid environment variable name for the connection URL.",
      );
    if (document.has(entriesKey) && !isMap(document.get(entriesKey)))
      throw new Error("Review the existing Configuration in the Blueprint.");
    if (
      entryKeys.has(environmentKey) ||
      document.hasIn([entriesKey, environmentKey])
    )
      throw new Error(`Configuration already contains ${environmentKey}.`);
    entryKeys.add(environmentKey);
    return { name, option, mapping: { environmentKey, fact: option.urlFact } };
  });

  for (const { name, option, mapping } of connections) {
    document.setIn([attachmentsKey, name], {
      backing_project: option.backingProject,
      backing_service: option.backingService,
      service,
      credential: { mode: "new" },
    });
    if (mapping) {
      document.setIn([entriesKey, mapping.environmentKey], {
        kind: "env",
        source: { fact: { attach: name, key: mapping.fact.key } },
        exposure: [service],
        secret: mapping.fact.secret,
      });
    }
  }
}
