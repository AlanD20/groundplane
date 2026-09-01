import {
  ArrowUpCircle,
  DatabaseBackup,
  History,
  KeyRound,
  Link2,
  Play,
  Plus,
  Power,
  PowerOff,
  RotateCw,
  ScrollText,
  Settings2,
  Trash2,
} from 'lucide-react'
import { cn } from '@/lib/utils'
import type { TaskStatus, TaskType } from '@/lib/types'

const ICONS: Record<TaskType, React.ReactNode> = {
  deploy: <ArrowUpCircle />,
  rollback: <History />,
  backup: <DatabaseBackup />,
  backup_prune: <Trash2 />,
  restore: <RotateCw />,
  attach: <Link2 />,
  detach: <Link2 />,
  run: <Play />,
  script: <ScrollText />,
  provision: <Settings2 />,
  create: <Plus />,
  start: <Power />,
  stop: <PowerOff />,
  destroy: <Trash2 />,
  remove: <Trash2 />,
  update: <Settings2 />,
  rotate: <KeyRound />,
}

export function ActivityIcon({ type, status }: { type: TaskType; status: TaskStatus }) {
  const tone =
    status === 'failed' || status === 'timed_out'
      ? 'bg-destructive/12 text-destructive'
      : status === 'running' || status === 'pending'
        ? 'bg-info/12 text-info'
        : 'bg-secondary text-muted-foreground'
  return (
    <span className={cn('flex size-7 shrink-0 items-center justify-center rounded-md [&_svg]:size-3.5', tone)}>
      {ICONS[type] ?? <ScrollText />}
    </span>
  )
}
