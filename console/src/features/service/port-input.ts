export function portInputError(value: string): string | null {
  if (!value.trim()) return null;
  for (const raw of value.split(",")) {
    const port = raw.trim();
    const match = /^(\d{1,5})(?:-(\d{1,5}))?(?:\/(tcp|udp))?$/.exec(port);
    if (!match) {
      return `Invalid port ${JSON.stringify(port)}. Use 80, 80/tcp or 53/udp; separate ports with commas.`;
    }
    const first = Number(match[1]);
    const last = match[2] ? Number(match[2]) : first;
    if (first < 1 || last < first || last > 65535) {
      return `Invalid port ${JSON.stringify(port)}. Ports must be 1–65535; a range must end at or above its start.`;
    }
  }
  return null;
}
