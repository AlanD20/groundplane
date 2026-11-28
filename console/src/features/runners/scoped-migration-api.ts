import type { components, operations } from "@/lib/api.generated";
import { controllerRequest } from "@/lib/controller-json-request";
import { requireTaskId } from "@/features/task/journal-model";
import { newULID } from "@/lib/utils";

export type MigrationStatus =
  components["schemas"]["RunnerScopedMigrationStatus"];
type MigrationRequest = components["schemas"]["RunnerScopedMigrationRequest"];
type Accepted =
  operations["runner.scoped-migration.start"]["responses"][202]["content"]["application/json"];

export type MigrationObservation = {
  status: MigrationStatus;
  revision: string;
};
type Submission = { key: string; revision: string; body: MigrationRequest };

// A lost response still needs the original request when the Runner inspector reopens.
const pending = new Map<string, Submission>();
const path = (runnerId: string) =>
  `/runners/${encodeURIComponent(runnerId)}/scoped-migration`;
export const pendingScopedMigration = (runnerId: string) =>
  pending.get(runnerId);

export function scopedMigrationSubmission(
  runnerId: string,
  revision: string,
  body: MigrationRequest,
) {
  const previous = pending.get(runnerId);
  if (previous) return previous;
  const submission = { key: newULID(), revision, body };
  pending.set(runnerId, submission);
  return submission;
}

export function releaseScopedMigration(
  runnerId: string,
  submission: Submission,
) {
  if (pending.get(runnerId) === submission) pending.delete(runnerId);
}

export async function readScopedMigration(
  runnerId: string,
  signal: AbortSignal,
): Promise<MigrationObservation> {
  let revision = "";
  const status = await controllerRequest<MigrationStatus>(path(runnerId), 200, {
    signal,
    onResponseHeaders: (headers) => {
      revision = headers.get("ETag") ?? "";
    },
  });
  if (status.runner_id !== runnerId || !/^"[1-9]\d*"$/.test(revision)) {
    throw new Error(
      "The Controller did not return a valid revision for this Runner.",
    );
  }
  return { status, revision };
}

export async function startScopedMigration(
  runnerId: string,
  submission: Submission,
) {
  const accepted = await controllerRequest<Accepted>(path(runnerId), 202, {
    method: "POST",
    body: submission.body,
    idempotencyKey: submission.key,
    ifMatch: submission.revision,
  });
  return requireTaskId(accepted, "Runner migration");
}
