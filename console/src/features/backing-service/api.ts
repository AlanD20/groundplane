import type { operations } from "@/lib/api.generated";

type GeneratedCreate =
  operations["backing-service.create"]["requestBody"]["content"]["application/json"];
type CreateBase = Omit<
  GeneratedCreate,
  "adapter" | "adapter_version" | "authentication" | "image" | "hooks"
>;
export type BackingHooks = NonNullable<GeneratedCreate["hooks"]>;

export type BackingServiceCreateRequest = CreateBase &
  (
    | {
        adapter: "postgres" | "mysql";
        adapter_version: string;
        authentication?: never;
        image?: never;
      }
    | {
        adapter: "valkey";
        adapter_version: string;
        authentication: "username_password" | "password" | "none";
        image?: never;
      }
    | {
        adapter: "custom";
        adapter_version?: never;
        image: string;
        authentication?: never;
        hooks?: BackingHooks;
      }
  );

export type BackingAdapterCatalog =
  operations["backing-service.adapters"]["responses"][200]["content"]["application/json"];

export type BackingServiceCreatedResponse =
  operations["backing-service.create"]["responses"][201]["content"]["application/json"];
