import { useEffect, useState, type ComponentProps } from 'react'
import { ArrowDown, ArrowUp, ArrowUpDown, ChevronLeft, ChevronRight } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { Select } from '@/components/ui/select'
import { TableHead } from '@/components/ui/table'

export const tablePageSizes = [5, 10, 25, 50] as const
export type TablePageSize = typeof tablePageSizes[number]
type Direction = 'asc' | 'desc'
type SortState = { field: string; direction: Direction; sortBy: (field: string) => void }

export function useTableView<T>(items: readonly T[], values: Record<string, (item: T) => string | number>, initialField: string, initialDirection: Direction = 'asc', resetKey = '') {
  const [sort, setSort] = useState({ field: initialField, direction: initialDirection })
  const [pageSize, setPageSize] = useState<TablePageSize>(5)
  const [position, setPosition] = useState({ key: resetKey, page: 0 })
  useEffect(() => { setPosition({ key: resetKey, page: 0 }) }, [resetKey])
  const sorted = [...items].sort((a, b) => {
    const left = values[sort.field](a), right = values[sort.field](b)
    const order = typeof left === 'number' && typeof right === 'number' ? left - right : String(left).localeCompare(String(right), undefined, { numeric: true })
    return sort.direction === 'asc' ? order : -order
  })
  const pages = Math.max(1, Math.ceil(sorted.length / pageSize))
  const page = Math.min(position.key === resetKey ? position.page : 0, pages - 1)
  const setPage = (next: number) => setPosition({ key: resetKey, page: Math.max(0, Math.min(next, pages - 1)) })
  return {
    rows: sorted.slice(page * pageSize, (page + 1) * pageSize), total: sorted.length, page, pages, pageSize, setPage,
    setPageSize: (size: TablePageSize) => { setPageSize(size); setPage(0) },
    ...sort,
    sortBy: (field: string) => {
      if (!values[field]) return
      setSort({ field, direction: sort.field === field && sort.direction === 'asc' ? 'desc' : 'asc' })
      setPage(0)
    },
  }
}

export function TableSortHead({ sort, field, children, ...props }: ComponentProps<typeof TableHead> & { sort: SortState; field: string }) {
  const active = sort.field === field
  const Icon = active ? sort.direction === 'asc' ? ArrowUp : ArrowDown : ArrowUpDown
  return <TableHead {...props} aria-sort={active ? sort.direction === 'asc' ? 'ascending' : 'descending' : 'none'}>
    <Button variant="ghost" size="content" className="gap-1 text-inherit" onClick={() => sort.sortBy(field)}>
      {children}<Icon aria-hidden className={`size-3.5 shrink-0 ${active ? 'text-primary' : 'opacity-50'}`} />
    </Button>
  </TableHead>
}

export function TablePagination({ table, label = 'Rows' }: { table: { page: number; pages: number; total: number; pageSize: TablePageSize; setPage: (page: number) => void; setPageSize: (size: TablePageSize) => void }; label?: string }) {
  return <nav aria-label={`${label} pagination`} className="flex flex-wrap items-center justify-between gap-3 border-t border-border pt-3">
    <label className="flex items-center gap-2 text-xs text-muted-foreground">{label} per page
      <Select aria-label={`${label} per page`} className="h-8 w-20" value={String(table.pageSize)} options={tablePageSizes.map(size => ({ value: String(size), label: String(size) }))} onValueChange={value => { const size = tablePageSizes.find(size => String(size) === value); if (size) table.setPageSize(size) }} />
    </label>
    <div className="flex flex-wrap items-center gap-2">
      <span role="status" className="text-xs text-muted-foreground">{table.total ? table.page * table.pageSize + 1 : 0}–{Math.min((table.page + 1) * table.pageSize, table.total)} of {table.total}</span>
      <Button variant="outline" size="sm" aria-label={`Previous ${label.toLowerCase()} page`} disabled={table.page === 0} onClick={() => table.setPage(table.page - 1)}><ChevronLeft className="size-4" /></Button>
      <span className="text-xs text-muted-foreground">{table.page + 1} / {table.pages}</span>
      <Button variant="outline" size="sm" aria-label={`Next ${label.toLowerCase()} page`} disabled={table.page + 1 >= table.pages} onClick={() => table.setPage(table.page + 1)}><ChevronRight className="size-4" /></Button>
    </div>
  </nav>
}
