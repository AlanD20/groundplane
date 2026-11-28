import type { operations } from "@/lib/api.generated";
import { controllerRequest } from "@/lib/controller-json-request";
import { useStore } from "@/lib/store";
import type { Environment, Project } from "@/lib/types";
import { valkeyAuthenticationDetails } from "@/lib/valkey-authentication";
import type { FirstDeploymentBackingOption } from "./first-deployment-attachment-model";
import { FirstDeploymentRecoveryReview } from "./first-deployment-recovery-review";
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
  const backingOptions: FirstDeploymentBackingOption[] =
    store.backingProjects.flatMap((backing) => {
      const main = backing.environments?.find(({ name }) => name === "main");
      const service = main?.services[0];
      const adapter = store.adapters.find(
        ({ key }) => key === service?.adapter,
      );
      if (
        backing.deletionTaskId ||
        main?.provisioningState !== "ready" ||
        main.deletionTaskId ||
        service?.runtimeIntent !== "running" ||
        !adapter ||
        (adapter.custom && (service.hooks?.attach || service.hooks?.detach))
      ) {
        return [];
      }
      const valkey = valkeyAuthenticationDetails(service.authentication);
      if (adapter.key === "valkey" && !valkey) return [];
      const urlFactKey = adapter.envVars.find((key) => key.endsWith("_URL"));
      return [
        {
          value: service.id,
          label: `${backing.name} · ${adapter.label}`,
          backingProject: backing.slug,
          backingService: service.name,
          description: adapter.custom
            ? "Grants network access to the Backing Service."
            : valkey?.summary,
          urlFact:
            !adapter.custom && urlFactKey
              ? { key: urlFactKey, secret: service.authentication !== "none" }
              : undefined,
        },
      ];
    });
  return (
    <FirstDeploymentWizard
      projects={[
        {
          id: project.id,
          label: project.name,
          environments: environment ? [environment] : project.environments,
          backingOptions,
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
      renderRecoveryReview={(environmentId, draft, environmentPath) => (
        <FirstDeploymentRecoveryReview
          environmentId={environmentId}
          backupsHref={
            environmentPath
              ? `${environmentPath}?view=operations&panel=backups`
              : undefined
          }
          addsDataSources={
            draft.volumes.some((volume) => !volume.existing) ||
            Boolean(draft.attachments?.length)
          }
          runsMigration={Boolean(draft.migration)}
        />
      )}
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
