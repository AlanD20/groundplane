'use client'

import { Navigate } from 'react-router-dom'
import { Workflow } from 'lucide-react'
import { PageHeader } from '@/components/common/page-header'
import { Card, CardContent } from '@/components/ui/card'
import { useStore } from '@/lib/store'

export default function PlatformAgentsPage() {
  const { platform, agentsLoading, agentError } = useStore()
  const agent = platform.agents[0]
  if (agent) return <Navigate to={`/platform/agents/${agent.id}`} replace />

  return (
    <div className="flex flex-col gap-6">
      <PageHeader
        title="Agent"
        description="Controller-managed local execution plane and runtime configuration."
        icon={<Workflow />}
      />
      <Card>
        <CardContent className="p-6 text-sm text-muted-foreground">
          {agentsLoading ? 'Loading Agent…' : (agentError ?? 'No Agent is enrolled on this host.')}
        </CardContent>
      </Card>
    </div>
  )
}
