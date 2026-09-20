import { Link } from 'react-router-dom'
import { Diamond } from 'lucide-react'

export function Brand() {
  return (
    <Link to="/platform/overview" className="flex items-center gap-3 rounded-md text-xl font-semibold tracking-tight">
      <Diamond className="size-6 text-primary" strokeWidth={1.5} />
      Groundplane{' '}
      <span className="hidden border-l border-border pl-3 text-xs font-normal tracking-normal text-muted-foreground sm:inline">
        Console
      </span>
    </Link>
  )
}
