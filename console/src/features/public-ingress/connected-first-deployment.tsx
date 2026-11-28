import type { operations } from "@/lib/api.generated";
import { controllerRequest } from "@/lib/controller-json-request";
import { useStore } from "@/lib/store";
import type { Environment, Project } from "@/lib/types";
import { FirstDeploymentWizard } from "./first-deployment-wizard";

type ImageFetchAccepted =
  operations["image.fetch"]["responses"][202]["content"]["application/json"];

export function ConnectedFirstDeployment({
  project,
  environment,
}: {
  project: Project;
  environment?: Environment;
}) {
  const store = useStore();
  return (
    <FirstDeploymentWizard
      projects={[
        {
          id: project.id,
          label: project.name,
          environments: environment ? [environment] : project.environments,
          secretOptions: store.reusableSecrets
            .filter(
              (secret) =>
                secret.kind === "env" &&
                secret.sensitive &&
                (secret.scope === "platform" ||
                  secret.projectId === project.id),
            )
            .map((secret) => ({
              value: secret.id,
              label: `${secret.key} · ${secret.scope === "platform" ? "Platform" : "Project"}`,
            })),
        },
      ]}
      initialProjectId={project.id}
      initialEnvironmentId={environment?.id}
      environmentLocked={Boolean(environment)}
      blueprint={store}
      onCreateEnvironment={store.addEnvironment}
      onFetchImage={(image, idempotencyKey) =>
        controllerRequest<ImageFetchAccepted>("/images/fetch", 202, {
          method: "POST",
          body: { image },
          idempotencyKey,
        })
      }
    />
  );
}
