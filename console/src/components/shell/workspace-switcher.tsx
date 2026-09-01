'use client'

import { useNavigate } from 'react-router-dom'
import { Boxes, Check, ChevronsUpDown, Globe, Plus } from 'lucide-react'
import { useStore } from '@/lib/store'
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu'
import { cn } from '@/lib/utils'

// The primary selection (Vercel team / Neon org style): the dropdown holds
// Platform (the workspace) and every Tenant; the sidebar and every page
// recontextualize to the selection.
export function WorkspaceSwitcher({
  workspace,
  onNewTenant,
}: {
  workspace: { kind: 'platform' | 'tenant'; slug: string }
  onNewTenant: () => void
}) {
  const navigate = useNavigate()
  const { tenants } = useStore()
  const current =
    workspace.kind === 'platform'
      ? { name: 'Platform', sub: 'workspace', icon: <Globe className="size-4" /> }
      : (() => {
          const t = tenants.find((x) => x.slug === workspace.slug)
          return { name: t?.name ?? workspace.slug, sub: 'Tenant', icon: <Boxes className="size-4" /> }
        })()

  return (
    <DropdownMenu>
      <DropdownMenuTrigger className="flex w-full items-center gap-2.5 rounded-lg border border-border bg-surface px-2.5 py-2 text-left outline-none transition-colors hover:bg-muted focus-visible:ring-2 focus-visible:ring-ring">
        <span
          className={cn(
            'flex size-8 shrink-0 items-center justify-center rounded-md',
            workspace.kind === 'platform' ? 'bg-primary/15 text-primary' : 'bg-secondary text-secondary-foreground',
          )}
        >
          {current.icon}
        </span>
        <span className="flex min-w-0 flex-1 flex-col">
          <span className="truncate text-sm font-semibold leading-tight">{current.name}</span>
          <span className="truncate text-xs text-muted-foreground">{current.sub}</span>
        </span>
        <ChevronsUpDown className="size-4 shrink-0 text-muted-foreground" />
      </DropdownMenuTrigger>
      <DropdownMenuContent align="start" className="w-64">
        <DropdownMenuLabel>Workspace</DropdownMenuLabel>
        <DropdownMenuItem onClick={() => navigate('/platform/overview')}>
          <Globe />
          <span className="flex-1">Platform</span>
          {workspace.kind === 'platform' && <Check className="size-4 text-primary" />}
        </DropdownMenuItem>
        <DropdownMenuSeparator />
        <DropdownMenuLabel>Tenants</DropdownMenuLabel>
        {tenants.map((t) => (
          <DropdownMenuItem key={t.id} onClick={() => navigate(`/t/${t.slug}`)}>
            <Boxes />
            <span className="flex-1 truncate">{t.name}</span>
            {workspace.kind === 'tenant' && workspace.slug === t.slug && <Check className="size-4 text-primary" />}
          </DropdownMenuItem>
        ))}
        <DropdownMenuSeparator />
        <DropdownMenuItem onClick={onNewTenant}>
          <Plus />
          New tenant
        </DropdownMenuItem>
      </DropdownMenuContent>
    </DropdownMenu>
  )
}
