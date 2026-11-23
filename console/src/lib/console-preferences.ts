export const tablePageSizes = [5, 10, 25, 50] as const;
export type TablePageSize = (typeof tablePageSizes)[number];

export function readDefaultTablePageSize(): TablePageSize {
  try {
    const stored = localStorage.getItem("groundplane-default-table-page-size");
    return tablePageSizes.find((size) => String(size) === stored) ?? 5;
  } catch {
    return 5;
  }
}

const chartTimeZoneKey = "groundplane-chart-time-zone";
export type ChartTimeZone = "utc" | "local";

export function readChartTimeZone(): ChartTimeZone {
  try {
    return localStorage.getItem(chartTimeZoneKey) === "local" ? "local" : "utc";
  } catch {
    return "utc";
  }
}

export function saveChartTimeZone(value: ChartTimeZone) {
  try {
    localStorage.setItem(chartTimeZoneKey, value);
  } catch {
    // The current view still uses the chosen time zone.
  }
}

const editorWordWrapKey = "groundplane-editor-word-wrap";

export function readEditorWordWrap(): boolean | undefined {
  try {
    const value = localStorage.getItem(editorWordWrapKey);
    if (value === "on" || value === "off") return value === "on";
  } catch {
    return undefined;
  }
}

export function saveEditorWordWrap(wrap: boolean) {
  try {
    localStorage.setItem(editorWordWrapKey, wrap ? "on" : "off");
  } catch {
    // The current editor still uses the selected setting.
  }
}
