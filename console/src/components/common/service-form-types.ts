import type { BackingHooks } from "@/features/backing-service/api";
import type { Environment, Service } from "@/lib/types";

export type ServicePatch = {
  name: string;
  image: string;
  role: string;
  zones: string[];
  strategy: Service["strategy"];
  onFailure: "switch_back" | "leave_active";
  healthcheck: Service["healthcheck"];
  resources: { mem: string; cpus: string };
  expose: string[];
  restart: Service["restart"];
  replicas: number;
  hooks?: BackingHooks;
};

type BackingFields = {
  adapterKey: string;
  prefix: string;
  onAdapterChange: (key: string) => void;
  onPrefixChange: (value: string) => void;
};

export type ServiceFormSection =
  "workload" | "network" | "runtime" | "healthcheck" | "hooks";

export type ServiceFormBodyProps = {
  inline?: boolean;
  onSaved?: () => void;
} & (
  | {
      env: Environment;
      workspace: string;
      initial?: Service;
      section?: ServiceFormSection;
      backing?: never;
      onCreateBacking?: never;
      onClose: () => void;
    }
  | {
      env?: never;
      workspace?: never;
      initial?: never;
      section?: never;
      backing: BackingFields;
      onCreateBacking: (
        patch: ServicePatch,
        adapter: string,
        prefix: string,
      ) => void;
      onClose: () => void;
    }
);
