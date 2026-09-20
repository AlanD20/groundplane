export type ScriptHook =
  | "manual"
  | "pre-deploy"
  | "post-deploy"
  | "pre-rollback"
  | "post-rollback"
  | "on-failure";

export type ScriptVolumeGrant = {
  volumeId: string;
  target: string;
  readOnly: boolean;
};
export type ScriptExecution =
  | { mode: "inherited" }
  | {
      mode: "explicit";
      image: string;
      user: string;
      volumes: ScriptVolumeGrant[];
      entryIds: string[];
    };

export type Script = {
  id: string;
  environmentId: string;
  slug: string;
  serviceId: string;
  service: string;
  when: ScriptHook;
  order: number;
  execution: ScriptExecution;
  body: string;
  origin: "api" | "blueprint";
  reconciliationKey?: string;
  activeGeneration: number;
};

export type ScriptInput = Pick<
  Script,
  "slug" | "service" | "body" | "when" | "order" | "execution"
>;
export type ScriptPatch = Partial<Omit<ScriptInput, "service">>;
