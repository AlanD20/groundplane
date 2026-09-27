"use client";

import { useRequiredParams } from "@/lib/router";
import { BlueprintWorkspace } from "@/features/blueprint/blueprint-workspace";
import type { Environment } from "@/lib/types";

// ---- Blueprint ----

export function BlueprintState({ env }: { env: Environment }) {
  const params = useRequiredParams("tenant");
  return <BlueprintWorkspace key={env.id} environment={env} workspace={params.tenant} />;
}
