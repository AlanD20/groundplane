'use client'

import { Link } from 'react-router-dom'
import {
  Activity,
  Boxes,
  Cpu,
  Database,
  KeyRound,
  Layers,
  LayoutDashboard,
  Server,
  ServerCog,
  Settings,
  ShieldCheck,
  Workflow,
} from 'lucide-react'
import { cn } from '@/lib/utils'

type NavItem = { label: string; href: string; icon: React.ReactNode }

function platformNav(): { section: string; items: NavItem[] }[] {
  return [
    {
      section: 'Platform',
      items: [
        { label: 'Overview', href: '/platform/overview', icon: <LayoutDashboard /> },
        { label: 'Backing services', href: '/platform/backing-services', icon: <Database /> },
        { label: 'Components', href: '/platform/components', icon: <Server /> },
        { label: 'Controller', href: '/platform/controller', icon: <ServerCog /> },
        { label: 'Agent', href: '/platform/agents', icon: <Workflow /> },
        { label: 'Activity', href: '/platform/activity', icon: <Activity /> },
        { label: 'Secret store', href: '/platform/secrets', icon: <ShieldCheck /> },
        { label: 'Host', href: '/platform/host', icon: <Cpu /> },
        { label: 'Settings', href: '/platform/settings', icon: <Settings /> },
      ],
    },
  ]
}

function tenantNav(slug: string): { section: string; items: NavItem[] }[] {
  return [
    {
      section: 'Tenant',
      items: [
        { label: 'Projects', href: `/t/${slug}`, icon: <Boxes /> },
        { label: 'Runners', href: `/t/${slug}/runners`, icon: <Cpu /> },
        { label: 'Activity', href: `/t/${slug}/activity`, icon: <Activity /> },
        { label: 'Settings', href: `/t/${slug}/settings`, icon: <Settings /> },
      ],
    },
  ]
}

// Inside a project (/t/<tenant>/<project>/…), the project's own resources
// sit in their own section above the tenant-level items.
function projectNav(slug: string, projectSlug: string): { section: string; items: NavItem[] }[] {
  return [
    {
      section: 'Project',
      items: [
        { label: 'Environments', href: `/t/${slug}/${projectSlug}`, icon: <Layers /> },
        { label: 'Secrets', href: `/t/${slug}/${projectSlug}/secrets`, icon: <KeyRound /> },
        { label: 'Settings', href: `/t/${slug}/${projectSlug}/settings`, icon: <Settings /> },
      ],
    },
    ...tenantNav(slug),
  ]
}

export function Sidebar({
  workspace,
  projectSlug,
  pathname,
}: {
  workspace: { kind: 'platform' | 'tenant'; slug: string }
  projectSlug?: string
  pathname: string
}) {
  const sections =
    workspace.kind === 'platform' ? platformNav() : projectSlug ? projectNav(workspace.slug, projectSlug) : tenantNav(workspace.slug)

  function isActive(href: string) {
    // Section roots (tenant page, project page) highlight only on the exact
    // route — /t/<tenant>/<project>/secrets must not highlight Environments.
    if (href === `/t/${workspace.slug}` || (projectSlug && href === `/t/${workspace.slug}/${projectSlug}`)) {
      return pathname === href
    }
    return pathname === href || pathname.startsWith(href + '/')
  }

  return (
    <nav className="flex flex-col gap-5 px-3 py-4">
      {sections.map((sec) => (
        <div key={sec.section} className="flex flex-col gap-1">
          <span className="px-2 pb-1 text-[11px] font-semibold uppercase tracking-wider text-muted-foreground/70">
            {sec.section}
          </span>
          {sec.items.map((item) => {
            const active = isActive(item.href)
            return (
              <Link
                key={item.href}
                to={item.href}
                className={cn(
                  'flex items-center gap-2.5 rounded-lg px-2 py-1.5 text-sm font-medium outline-none transition-colors',
                  '[&_svg]:size-4 [&_svg]:shrink-0 focus-visible:ring-2 focus-visible:ring-ring',
                  active
                    ? 'bg-sidebar-accent text-sidebar-accent-foreground [&_svg]:text-primary'
                    : 'text-muted-foreground hover:bg-sidebar-accent/60 hover:text-foreground [&_svg]:text-muted-foreground',
                )}
              >
                {item.icon}
                <span className="truncate">{item.label}</span>
              </Link>
            )
          })}
        </div>
      ))}
    </nav>
  )
}
