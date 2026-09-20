import type { ActivityEntry, PlatformInfra, Project, Runner, ReusableSecret, Tenant } from '@/lib/types'
import type { HostInfo } from '@/features/platform-host/host-model'

export type TaskNavigationContext = {
  tenants: Tenant[]
  tenantProjects: Project[]
  backingProjects: Project[]
  runners: Runner[]
  reusableSecrets: ReusableSecret[]
  platform: PlatformInfra
  host: HostInfo | null
}

export type TaskNavigationResolution = {
  href: string
  label: string
  fallback: boolean
}

export function resolveTaskOperationSurface(
  task: ActivityEntry,
  context: TaskNavigationContext,
): TaskNavigationResolution | null {
  const tenant = task.tenantId
    ? context.tenants.find((candidate) => candidate.id === task.tenantId)
    : undefined
  const target = task.target

  if (task.workspaceType === 'platform' && task.type === 'update' && target === 'controller') {
    return exact('/platform/host/controller', 'Open Controller')
  }

  // Immutable Environment ownership always selects the Environment Tasks tab.
  // The typed target only identifies the resource operated on inside it.
  if (task.environmentId) {
    const tenantProject = context.tenantProjects.find((candidate) => candidate.id === task.projectId)
    const environment = tenantProject?.environments?.find((candidate) => candidate.id === task.environmentId)
    const owningTenant = tenantProject?.tenantId
      ? context.tenants.find((candidate) => candidate.id === tenantProject.tenantId)
      : undefined
    if (tenantProject && environment && owningTenant) {
      return exact(
        `/t/${segment(owningTenant.slug)}/${segment(tenantProject.slug)}/${segment(environment.name)}?tab=tasks`,
        'Open Environment Tasks',
      )
    }
    const backingProject = context.backingProjects.find((candidate) => candidate.id === task.projectId)
    if (backingProject?.environments?.some((candidate) => candidate.id === task.environmentId)) {
      return exact(`/platform/backing-services/${segment(backingProject.id)}`, 'Open backing service')
    }
    return workspaceFallback(task, tenant)
  }

  const agent = context.platform.agents.find((candidate) => candidate.id === target)
  if (agent) return exact(`/platform/host/agents/${segment(agent.id)}`, 'Open Agent')

  const component = context.platform.components.find((candidate) => candidate.id === target)
  if (component) {
    return exact(`/platform/components/${segment(component.kind)}`, `Open ${component.name} Component`)
  }

  const secret = context.reusableSecrets.find((candidate) => candidate.id === target)
  if (secret?.scope === 'platform') {
    if (task.workspaceType !== 'platform' || task.tenantId !== undefined || task.projectId !== undefined || task.environmentId !== undefined) {
      return workspaceFallback(task, tenant)
    }
    return exact('/platform/secrets', 'Open Platform Secrets')
  }
  if (secret?.scope === 'project') {
    const project = context.tenantProjects.find((candidate) => candidate.id === secret.projectId)
    if (!project || task.workspaceType !== 'tenant' || task.tenantId !== project.tenantId || task.projectId !== secret.projectId || task.environmentId !== undefined) {
      return workspaceFallback(task, tenant)
    }
    const owningTenant = project?.tenantId
      ? context.tenants.find((candidate) => candidate.id === project.tenantId)
      : undefined
    if (project && owningTenant) {
      return exact(`/t/${segment(owningTenant.slug)}/${segment(project.slug)}/secrets`, 'Open Project Secrets')
    }
    return workspaceFallback(task, tenant)
  }

  const runner = context.runners.find((candidate) => candidate.id === target)
  if (runner) {
    const owningTenant = context.tenants.find((candidate) => candidate.id === runner.tenantId)
    return owningTenant
      ? exact(`/t/${segment(owningTenant.slug)}/runners`, 'Open Tenant Runners')
      : workspaceFallback(task, tenant)
  }

  const tenantTarget = context.tenants.find((candidate) => candidate.id === target)
  if (tenantTarget) return exact(`/t/${segment(tenantTarget.slug)}/settings`, 'Open Tenant settings')

  const projectTarget = context.tenantProjects.find((candidate) => candidate.id === target)
  if (projectTarget) {
    const owningTenant = projectTarget.tenantId
      ? context.tenants.find((candidate) => candidate.id === projectTarget.tenantId)
      : undefined
    return owningTenant
      ? exact(`/t/${segment(owningTenant.slug)}/${segment(projectTarget.slug)}/settings`, 'Open Project settings')
      : workspaceFallback(task, tenant)
  }

  const backingTarget = context.backingProjects.find((candidate) => candidate.id === target)
  if (backingTarget) {
    return exact(`/platform/backing-services/${segment(backingTarget.id)}`, 'Open backing service')
  }

  if (context.host && target === context.host.hostname) return exact('/platform/host', 'Open Host')

  // A recognized stable target that no longer has a live record is deliberately
  // not treated as an exact operation surface. The immutable owner decides the
  // fallback journal, and Inspect remains authoritative.
  if (stableTypedTarget(target)) return workspaceFallback(task, tenant)

  // Owner-only Tasks can still open the nearest live owner surface. This is not
  // inferred from operation_id, which is opaque reference identity.
  if (task.projectId) {
    const project = context.tenantProjects.find((candidate) => candidate.id === task.projectId)
    const owningTenant = project?.tenantId
      ? context.tenants.find((candidate) => candidate.id === project.tenantId)
      : tenant
    if (project && owningTenant) {
      return exact(`/t/${segment(owningTenant.slug)}/${segment(project.slug)}`, 'Open Project overview')
    }
    const backingProject = context.backingProjects.find((candidate) => candidate.id === task.projectId)
    if (backingProject) {
      return exact(`/platform/backing-services/${segment(backingProject.id)}`, 'Open backing service')
    }
  }
  if (tenant) return exact(`/t/${segment(tenant.slug)}`, 'Open Tenant overview')
  return workspaceFallback(task, tenant)
}

function stableTypedTarget(target: string): boolean {
  const prefix = target.split('_', 1)[0]?.toLowerCase()
  return !!prefix && [
    'agt', 'agent', 'cmp', 'component', 'env', 'prj', 'project', 'run', 'runner', 'sec', 'secret', 'ten', 'tenant',
  ].includes(prefix)
}

function workspaceFallback(
  task: ActivityEntry,
  tenant: Tenant | undefined,
): TaskNavigationResolution | null {
  if (task.workspaceType === 'platform') {
    return fallback('/platform/components', 'Open owning Platform workspace journal')
  }
  return tenant
    ? fallback(`/t/${segment(tenant.slug)}/activity`, 'Open owning Tenant workspace journal')
    : null
}

function exact(href: string, label: string): TaskNavigationResolution {
  return { href, label, fallback: false }
}

function fallback(href: string, label: string): TaskNavigationResolution {
  return { href, label, fallback: true }
}

function segment(value: string): string {
  return encodeURIComponent(value)
}
