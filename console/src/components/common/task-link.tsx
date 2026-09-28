import { Link, useLocation } from 'react-router-dom'
import { ArrowUpRight } from 'lucide-react'

export function TaskLink({ taskId, onClick, children = 'Open task' }: {
  taskId: string
  onClick?: () => void
  children?: React.ReactNode
}) {
  const location = useLocation()
  const search = new URLSearchParams(location.search)
  search.set('task', taskId)
  return <Link to={{ pathname: location.pathname, search: `?${search}`, hash: location.hash }}
    onClick={onClick} aria-label={`Open task ${taskId}`}
    className="inline-flex max-w-full items-center gap-1 rounded-sm text-xs text-primary underline underline-offset-4 focus-visible:outline-2 focus-visible:outline-ring">
    <span className="min-w-0 break-all">{children}</span><ArrowUpRight className="size-3.5 shrink-0" />
  </Link>
}
