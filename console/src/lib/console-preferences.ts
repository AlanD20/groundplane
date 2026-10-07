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
