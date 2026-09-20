'use client'

import { Fragment, type ReactNode } from 'react'
import { Ellipsis } from 'lucide-react'
import { useNavigate } from 'react-router-dom'
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu'

export type ResourceAction = {
  label: string
  icon?: ReactNode
  href?: string
  onSelect?: () => void
  destructive?: boolean
  disabled?: boolean
}

export function ResourceActionMenu({ label, actions }: { label: string; actions: ResourceAction[] }) {
  const navigate = useNavigate()
  const firstDestructive = actions.findIndex((action) => action.destructive)

  return (
    <DropdownMenu>
      <DropdownMenuTrigger
        aria-label={label}
        title={label}
        className="inline-flex size-8 items-center justify-center rounded-md text-muted-foreground outline-none transition-colors hover:bg-muted hover:text-foreground focus-visible:ring-1 focus-visible:ring-ring"
      >
        <Ellipsis className="size-4" />
      </DropdownMenuTrigger>
      <DropdownMenuContent>
        {actions.map((action, index) => (
          <Fragment key={action.label}>
            {index === firstDestructive && index > 0 ? <DropdownMenuSeparator /> : null}
            <DropdownMenuItem
              variant={action.destructive ? 'destructive' : 'default'}
              disabled={action.disabled}
              onClick={() => {
                if (action.href) navigate(action.href)
                else action.onSelect?.()
              }}
            >
              {action.icon}
              {action.label}
            </DropdownMenuItem>
          </Fragment>
        ))}
      </DropdownMenuContent>
    </DropdownMenu>
  )
}
