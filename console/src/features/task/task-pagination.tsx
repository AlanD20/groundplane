import { ChevronLeft, ChevronRight } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { Select } from '@/components/ui/select'
import { useStore } from '@/lib/store'
import type { TaskJournalScope, TaskJournalSurface, TaskPageSize } from '@/lib/types'

const pageSizes: TaskPageSize[] = [10, 25, 50, 100]

export function TaskPagination({ scope, surface }: {
  scope: TaskJournalScope
  surface: TaskJournalSurface
}) {
  const store = useStore()
  const journal = store.getTaskJournal(scope)
  const loading = journal.loading || journal.loadingMore
  const load = (cursor?: string, pageSize = journal.pageSize) => {
    void store.loadTaskJournal(surface, scope, cursor, pageSize).catch(() => undefined)
  }

  return (
    <nav aria-label="Task pagination" className="flex flex-wrap items-center justify-between gap-3 border-t border-border pt-3">
      <label className="flex items-center gap-2 text-xs text-muted-foreground">
        Tasks per page
        <Select
          aria-label="Tasks per page"
          className="h-8 w-20"
          value={String(journal.pageSize)}
          disabled={loading}
          options={pageSizes.map(value => ({ value: String(value), label: String(value) }))}
          onValueChange={value => {
            const size = pageSizes.find(size => String(size) === value)
            if (size !== undefined) load(undefined, size)
          }}
        />
      </label>
      <div className="flex items-center gap-3">
        <span role="status" className="text-xs text-muted-foreground">
          {loading ? 'Loading…' : `Page ${journal.pageIndex + 1}`}
        </span>
        <Button variant="outline" size="sm" disabled={loading || !journal.loaded || journal.pageIndex === 0}
          onClick={() => load(journal.pageCursors[journal.pageIndex - 1])}>
          <ChevronLeft className="size-4" /> Previous
        </Button>
        <Button variant="outline" size="sm" disabled={loading || !journal.nextCursor || !!journal.loadError}
          onClick={() => load(journal.nextCursor ?? undefined)}>
          Next <ChevronRight className="size-4" />
        </Button>
      </div>
    </nav>
  )
}
