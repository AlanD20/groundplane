import { useSearchParams } from 'react-router-dom'
import { X } from 'lucide-react'
import { TaskDetailDrawer } from '@/components/common/task-detail-drawer'
import { TaskLink } from '@/components/common/task-link'
import { Button } from '@/components/ui/button'
import { dismissAcceptedTask, useAcceptedTasks } from './accepted-tasks'

export function AcceptedTasks() {
  const tasks = useAcceptedTasks()
  if (!tasks.length) return null
  return <section aria-label="Recently triggered tasks" aria-live="polite" className="mb-5 flex flex-col gap-2 rounded-lg border border-border bg-card p-3">
    {tasks.map(id => <div key={id} className="flex min-w-0 items-center gap-3 text-xs">
      <span className="shrink-0 text-muted-foreground">Task accepted</span>
      <TaskLink taskId={id}>{id}</TaskLink>
      <Button variant="ghost" size="icon" className="ml-auto shrink-0" aria-label={`Dismiss task ${id}`} onClick={() => dismissAcceptedTask(id)}><X className="size-3.5" /></Button>
    </div>)}
  </section>
}

export function LinkedTask() {
  const [search, setSearch] = useSearchParams()
  const id = search.get('task')
  if (!id) return null
  return <TaskDetailDrawer key={id} taskId={id} scope={{ kind: 'all' }} surface="tasks" onOpenChange={open => {
    if (!open) setSearch(current => { const next = new URLSearchParams(current); next.delete('task'); return next })
  }} />
}
