'use client'

import { useState } from 'react'
import { Link } from 'react-router-dom'
import { ChevronRight, Plus, Workflow } from 'lucide-react'
import { EmptyState } from '@/components/common/empty-state'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { useRequiredParams } from '@/lib/router'
import type { Environment } from '@/lib/types'
import { ReleaseGroupFormDrawer } from './release-group-form-drawer'
import { releaseGroupPath, releaseGroupPolicyLabel, releaseGroupTag } from './release-group-projections'

export function ReleaseGroupsPanel({ env }: { env: Environment }) {
  const params = useRequiredParams('tenant', 'project', 'env')
  const [adding, setAdding] = useState(false)

  return (
    <Card>
      <CardHeader className="flex-row items-center justify-between gap-3">
        <div className="flex flex-col gap-1">
          <CardTitle className="flex items-center gap-2">
            <Workflow className="size-4 text-primary" /> Release groups
            <Badge variant="outline">{env.releaseGroups.length}</Badge>
          </CardTitle>
          <p className="text-xs text-muted-foreground">
            Explicit service sets deployed or rolled back in a fixed order under one task lock.
          </p>
        </div>
        <Button onClick={() => setAdding(true)} disabled={env.services.length < 2}>
          <Plus className="size-4" /> Add group
        </Button>
      </CardHeader>
      <CardContent>
        {env.services.length < 2 ? (
          <EmptyState
            icon={<Workflow />}
            title="At least two services are required"
            description="Add another service before defining a coordinated release group."
          />
        ) : env.releaseGroups.length === 0 ? (
          <EmptyState
            icon={<Workflow />}
            title="No release groups"
            description="Create an explicit ordered set when multiple services must release under one task lock."
            action={<Button onClick={() => setAdding(true)}><Plus className="size-4" /> Add group</Button>}
          />
        ) : (
          <div className="grid gap-3 lg:grid-cols-2">
            {env.releaseGroups.map((group) => (
              <Link
                key={group.id}
                to={releaseGroupPath(params.tenant, params.project, params.env, group.id)}
                className="group rounded-xl border border-border bg-surface/50 p-4 transition-colors hover:border-primary/40 hover:bg-muted/40"
              >
                <div className="flex items-start justify-between gap-3">
                  <div>
                    <p className="font-medium group-hover:text-primary">{group.name}</p>
                    <p className="mt-1 font-mono text-[11px] text-muted-foreground">{group.id}</p>
                  </div>
                  <ChevronRight className="size-4 text-muted-foreground transition-transform group-hover:translate-x-0.5" />
                </div>
                <div className="mt-4 flex flex-wrap items-center gap-1.5">
                  {group.order.map((service, index) => (
                    <span key={service} className="flex items-center gap-1.5">
                      <Badge variant="outline"><span className="text-muted-foreground">{index + 1}</span> {service}</Badge>
                      {index < group.order.length - 1 && <ChevronRight className="size-3 text-muted-foreground/50" />}
                    </span>
                  ))}
                </div>
                <div className="mt-4 flex flex-wrap gap-2 text-xs text-muted-foreground">
                  <Badge variant="primary">{releaseGroupTag(env, group)}</Badge>
                  <span>{releaseGroupPolicyLabel(group.onFailure)}</span>
                </div>
              </Link>
            ))}
          </div>
        )}
      </CardContent>
      <ReleaseGroupFormDrawer env={env} open={adding} onOpenChange={setAdding} />
    </Card>
  )
}
