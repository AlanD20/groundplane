import type { components } from "@/lib/api.generated";
import type { ScriptExecution } from "./types";
import { scriptExecutionError } from "./execution.ts";

type ExecutionWire = components["schemas"]["ScriptExecution"];

function invalidExecution(): never {
  throw new Error("Controller returned an invalid Script execution context");
}

export function scriptExecutionFromAPI(
  execution: ExecutionWire,
): ScriptExecution {
  if (!execution || typeof execution !== "object" || Array.isArray(execution))
    return invalidExecution();
  if (execution.mode === "inherited") {
    if (Object.keys(execution).some((key) => key !== "mode"))
      return invalidExecution();
    return { mode: "inherited" };
  }
  if (
    execution.mode !== "explicit" ||
    typeof execution.image !== "string" ||
    typeof execution.user !== "string" ||
    Object.keys(execution).some(
      (key) => !["mode", "image", "user", "volumes", "entry_ids"].includes(key),
    ) ||
    (execution.volumes !== undefined && !Array.isArray(execution.volumes)) ||
    (execution.entry_ids !== undefined && !Array.isArray(execution.entry_ids))
  )
    return invalidExecution();
  const result: ScriptExecution = {
    mode: "explicit",
    image: execution.image,
    user: execution.user,
    volumes: (execution.volumes ?? []).map((grant) => {
      if (
        !grant ||
        typeof grant !== "object" ||
        typeof grant.volume_id !== "string" ||
        typeof grant.target !== "string" ||
        typeof grant.read_only !== "boolean" ||
        Object.keys(grant).some(
          (key) => !["volume_id", "target", "read_only"].includes(key),
        )
      )
        return invalidExecution();
      return {
        volumeId: grant.volume_id,
        target: grant.target,
        readOnly: grant.read_only,
      };
    }),
    entryIds: (execution.entry_ids ?? []).map((id) =>
      typeof id === "string" ? id : invalidExecution(),
    ),
  };
  if (scriptExecutionError(result)) return invalidExecution();
  return result;
}

export function scriptExecutionToAPI(
  execution: ScriptExecution,
): ExecutionWire {
  if (execution.mode === "inherited") return { mode: "inherited" };
  return {
    mode: "explicit",
    image: execution.image,
    user: execution.user,
    volumes: execution.volumes.map((grant) => ({
      volume_id: grant.volumeId,
      target: grant.target,
      read_only: grant.readOnly,
    })),
    entry_ids: [...execution.entryIds],
  };
}
