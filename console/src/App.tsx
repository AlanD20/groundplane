import { lazy, Suspense } from 'react'
import { Navigate, Route, Routes } from 'react-router-dom'
import { AppFrame } from '@/components/shell/app-frame'

const PlatformOverview = lazy(() => import('@/routes/platform/overview/page'))
const BackingServices = lazy(() => import('@/routes/platform/backing-services/page'))
const BackingService = lazy(() => import('@/routes/platform/backing-services/[id]/page'))
const PlatformActivity = lazy(() => import('@/routes/platform/activity/page'))
const PlatformSecrets = lazy(() => import('@/routes/platform/secrets/page'))
const PlatformHost = lazy(() => import('@/routes/platform/host/page'))
const PlatformComponents = lazy(() => import('@/routes/platform/components/page'))
const PlatformComponent = lazy(() => import('@/routes/platform/components/[component]/page'))
const PlatformAgent = lazy(() => import('@/routes/platform/agents/[id]/page'))
const PlatformSettings = lazy(() => import('@/routes/platform/settings/page'))
const TenantOverview = lazy(() => import('@/routes/t/[tenant]/page'))
const TenantRunners = lazy(() => import('@/routes/t/[tenant]/runners/page'))
const TenantActivity = lazy(() => import('@/routes/t/[tenant]/activity/page'))
const TenantSettings = lazy(() => import('@/routes/t/[tenant]/settings/page'))
const ProjectOverview = lazy(() => import('@/routes/t/[tenant]/[project]/page'))
const ProjectSecrets = lazy(() => import('@/routes/t/[tenant]/[project]/secrets/page'))
const ProjectSettings = lazy(() => import('@/routes/t/[tenant]/[project]/settings/page'))
const Environment = lazy(() => import('@/routes/t/[tenant]/[project]/[env]/page'))
const ReleaseDetail = lazy(() => import('@/routes/t/[tenant]/[project]/[env]/releases/[id]/page'))
const ReleaseGroup = lazy(() => import('@/routes/t/[tenant]/[project]/[env]/release-groups/[id]/page'))

export default function App() {
  return (
    <AppFrame>
      <Suspense fallback={<div role="status" className="p-6 text-sm text-muted-foreground">Loading view…</div>}>
        <Routes>
          <Route path="/" element={<Navigate to="/platform/overview" replace />} />
          <Route path="/platform" element={<Navigate to="/platform/overview" replace />} />
          <Route path="/platform/overview" element={<PlatformOverview />} />
          <Route path="/platform/backing-services" element={<BackingServices />} />
          <Route path="/platform/backing-services/:id" element={<BackingService />} />
          <Route path="/platform/activity" element={<PlatformActivity />} />
          <Route path="/platform/secrets" element={<PlatformSecrets />} />
          <Route path="/platform/host" element={<PlatformHost />} />
          <Route path="/platform/components" element={<PlatformComponents />} />
          <Route path="/platform/components/:component" element={<PlatformComponent />} />
          <Route path="/platform/agents" element={<Navigate to="/platform/components" replace />} />
          <Route path="/platform/agents/:id" element={<PlatformAgent />} />
          <Route path="/platform/settings" element={<PlatformSettings />} />
          <Route path="/t/:tenant" element={<TenantOverview />} />
          <Route path="/t/:tenant/runners" element={<TenantRunners />} />
          <Route path="/t/:tenant/activity" element={<TenantActivity />} />
          <Route path="/t/:tenant/settings" element={<TenantSettings />} />
          <Route path="/t/:tenant/:project" element={<ProjectOverview />} />
          <Route path="/t/:tenant/:project/secrets" element={<ProjectSecrets />} />
          <Route path="/t/:tenant/:project/settings" element={<ProjectSettings />} />
          <Route path="/t/:tenant/:project/:env" element={<Environment />} />
          <Route path="/t/:tenant/:project/:env/releases/:id" element={<ReleaseDetail />} />
          <Route path="/t/:tenant/:project/:env/release-groups/:id" element={<ReleaseGroup />} />
          <Route path="*" element={<Navigate to="/platform/overview" replace />} />
        </Routes>
      </Suspense>
    </AppFrame>
  )
}
