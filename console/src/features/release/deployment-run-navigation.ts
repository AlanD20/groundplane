export const deploymentRunParam = "deploymentRun";
export const deploymentReleaseParam = "deploymentRelease";

export function deploymentRunLink(
  search: URLSearchParams,
  taskId?: string,
  releaseId?: string,
) {
  const next = new URLSearchParams(search);
  next.delete("task");
  next.delete(deploymentRunParam);
  next.delete(deploymentReleaseParam);
  if (taskId) next.set(deploymentRunParam, taskId);
  if (releaseId) next.set(deploymentReleaseParam, releaseId);
  return `?${next}`;
}
