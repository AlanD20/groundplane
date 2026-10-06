import { ControllerTransportError, controllerResponseError } from "@/lib/controller-request-errors";

// Private identity bytes belong to a transient download, never the Console store.
export async function exportBackupKey(environmentId: string, signal?: AbortSignal): Promise<void> {
  const path = `/environments/${encodeURIComponent(environmentId)}/export-key`;
  let objectURL: string | null = null;
  let anchor: HTMLAnchorElement | null = null;
  try {
    let response: Response;
    try {
      response = await fetch(`/api/v1${path}`, {
        method: "POST", headers: { Accept: "text/plain" }, cache: "no-store", signal,
      });
    } catch (error) {
      throw new ControllerTransportError(
        `POST ${path}: ${error instanceof Error ? error.message : "request failed before an HTTP response"}`, error,
      );
    }
    if (response.status !== 200) throw await controllerResponseError(response, "POST", path);
    if (response.headers.get("Cache-Control")?.toLowerCase() !== "no-store")
      throw new Error("Controller backup key export response is not marked no-store");
    if (response.headers.get("Content-Type")?.toLowerCase() !== "text/plain; charset=utf-8")
      throw new Error("Controller backup key export response has an invalid content type");
    const disposition = response.headers.get("Content-Disposition") ?? "";
    const filename = disposition.match(new RegExp(
      '^attachment; filename="(groundplane-' + environmentId + '-age-era-[1-9][0-9]*-identity\\.txt)"$',
    ))?.[1];
    if (!filename) throw new Error("Controller backup key export response has an invalid attachment name");
    objectURL = URL.createObjectURL(await response.blob());
    anchor = document.createElement("a");
    anchor.href = objectURL;
    anchor.download = filename;
    anchor.click();
  } finally {
    anchor?.removeAttribute("href");
    anchor?.remove();
    if (objectURL) URL.revokeObjectURL(objectURL);
  }
}
