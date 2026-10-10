import { ServiceFormBody } from "@/components/common/service-form-body";
import {
  ResourceForm,
  ResourceFormHeader,
  ResourceFormTitle,
} from "@/components/common/resource-form";
import { useRequiredParams } from "@/lib/router";
import type { Environment } from "@/lib/types";

export function EnvironmentServiceForm({
  env,
  open,
  onOpenChange,
}: {
  env: Environment;
  open: boolean;
  onOpenChange: (open: boolean) => void;
}) {
  const params = useRequiredParams("tenant");
  return (
    <ResourceForm inline open={open} onOpenChange={onOpenChange}>
      <ResourceFormHeader>
        <ResourceFormTitle>Add Service · {env.name}</ResourceFormTitle>
      </ResourceFormHeader>
      <ServiceFormBody
        inline
        env={env}
        workspace={params.tenant}
        onClose={() => onOpenChange(false)}
      />
    </ResourceForm>
  );
}
