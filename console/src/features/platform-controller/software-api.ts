import type { operations } from "@/lib/api.generated";

export type SoftwareRequest = operations["software.prepare"]["requestBody"]["content"]["application/json"];
export type SoftwarePreparation = operations["software.preparation.show"]["responses"][200]["content"]["application/json"];
export type SoftwarePreparationPage = operations["software.preparations"]["responses"][200]["content"]["application/json"];
export type SoftwareReleaseCatalog = operations["software.releases"]["responses"][200]["content"]["application/json"];
export type SoftwareActivation = operations["software.activation.show"]["responses"][200]["content"]["application/json"];
