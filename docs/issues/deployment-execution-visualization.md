# Deployment execution visualization

Maintainer design review. The operator guide owns [Task and work inspection](../features/tasks-and-logs.md#follow-an-operation).

## What operators need to see

A Service deployment can originate in an Environment-wide Blueprint Apply. The
originating Task can include sibling Services, Zones, and backing connections.
On a Service deployment, show the Service release and the originating Task as
distinct records. The operator should be able to tell whether the Task succeeded,
which operation needs attention, and where to find its recorded evidence.
[Task inspection](../features/tasks-and-logs.md#follow-an-operation)
[Release detail](../../console/src/features/release/release-detail-page.tsx)

## Patterns from other workflow tools

| Tool | Pattern | Application to Groundplane |
| --- | --- | --- |
| Azure Pipelines | The run summary shows stage and job status. Choosing a job opens its steps and logs. [Run details](https://learn.microsoft.com/en-us/azure/devops/pipelines/create-first-pipeline?view=azure-devops#view-pipeline-run-details) | Give each deployment a Task summary and let the operator request individual operation evidence. |
| GitHub Actions | The run graph shows progress. Choosing a job opens step logs and expands failures. [Run graph](https://docs.github.com/en/actions/how-tos/monitor-workflows/use-the-visualization-graph) [Run logs](https://docs.github.com/en/actions/how-tos/monitor-workflows/use-workflow-run-logs) | Put failures and attention states in the scan path. Keep successful detail collapsed until requested. |
| GitLab and Argo Workflows | GitLab can group jobs by stage or display links from explicit `needs` relationships. Argo models dependencies explicitly to represent parallel work. [GitLab job grouping](https://docs.gitlab.com/ci/pipelines/#group-jobs-by-stage-or-needs-configuration) [Argo workflow dependencies](https://argo-workflows.readthedocs.io/en/latest/walk-through/dag/) | Use a dependency view when every displayed edge has recorded authority. |

## What Groundplane can represent today

- Task status, target and recorded work counts support a run-level summary.
  A work page is a bounded selection from that Task. Label a partial list and
  keep a link to the full Task, including when the visible work page has only
  successful operations. [Task API](../../openapi.json)
- Work records support operation labels, target names, current phase, outcome,
  and some recorded blockers. Whole-Task prerequisites and individual blocker
  references represent different relationships. The work page lacks a complete
  graph of execution dependencies. [Task API](../../openapi.json)
  [Whole-Task prerequisites](../features/tasks-and-logs.md#run-after-whole-tasks)
- A timing view can use recorded attempt timestamps when available. Work
  creation and update timestamps record changes to the work record. Some
  Controller work settles without an executor attempt. A run-level elapsed time
  must use the Task's own timestamps. [Task API](../../openapi.json)
  [Recorded timing](../features/tasks-and-logs.md#follow-an-operation)

## Recommended deployment view

Show the originating Task's action, target, status, and work counts above an
in-flow list of visible operations. Each row should show its action, target, and
status once. Expanding it shows the existing recorded blocker, attempts, stages,
and diagnostics under that row. Use the same description and status model as
Task inspection. Keep a visible full-Task link for pagination and investigation.
Use keyboard-operable disclosure controls with visible expanded state.
[Disclosure accessibility pattern](https://www.w3.org/WAI/ARIA/apg/patterns/disclosure/)

A separate dependency view would be valuable when investigating parallel Blueprint
work. It needs a complete graph tied to the execution revision before it can show
authoritative critical paths. A timing view likewise needs recorded attempt spans
and clear treatment of waiting, retries, and unavailable evidence. The operation
list answers the immediate deployment questions with the data already in the
Task response.
