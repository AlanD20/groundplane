import type {
  Environment,
  EnvironmentEntry,
  ReusableSecret,
} from "@/lib/types";

export function entrySourceLabel(
  entry: EnvironmentEntry,
  env: Environment,
  secrets: ReusableSecret[],
  projectId?: string,
): string {
  const source = entry.source;
  if (source.kind === "literal") return "Stored in this Entry";
  if (source.kind === "fact") {
    const connection = env.attaches.find(
      (attach) => attach.id === source.attachId,
    );
    return `Connection value: ${connection?.name ?? source.attachId} / ${source.fact}`;
  }
  const matches = (secret: ReusableSecret) =>
    secret.id === source.secretRef || secret.key === source.secretRef;
  const selected =
    secrets.find(
      (secret) =>
        secret.scope === "project" &&
        secret.projectId === projectId &&
        matches(secret),
    ) ??
    secrets.find((secret) => secret.scope === "platform" && matches(secret));
  if (!selected) return `Configuration reference: ${source.secretRef}`;
  return `${selected.scope === "project" ? "Project" : "Platform"} ${selected.sensitive ? "Secret" : selected.kind === "file" ? "file" : "variable"}: ${selected.key}`;
}
