export function gitHubUrl(value: string | undefined): string | undefined {
  if (!value) return;
  try {
    const url = new URL(value);
    if (
      url.protocol === "https:" &&
      url.host === "github.com" &&
      !url.username &&
      !url.password
    )
      return url.href;
  } catch {
    return;
  }
}
