import { useEditorSession } from "@/components/common/editor-session";
import { ServiceFormBody } from "@/components/common/service-form-body";
import {
  ResourceForm,
  ResourceFormHeader,
  ResourceFormTitle,
} from "@/components/common/resource-form";
import { useRequiredParams } from "@/lib/router";
import type { Environment } from "@/lib/types";
import { useState } from "react";
import { FormDirty } from "@/components/common/form-actions";

export function EnvironmentServiceForm({
  env,
  open,
  onOpenChange,
}: {
  env: Environment;
  open: boolean;
  onOpenChange: (open: boolean) => void;
}) {
  const session = useEditorSession();
  const close = () => {
    session?.finish();
    onOpenChange(false);
  };
  const params = useRequiredParams("tenant");
  const [dirty, setDirty] = useState(false);
  return (
    <FormDirty.Provider value={dirty}>
      <ResourceForm inline open={open} onOpenChange={onOpenChange}>
        <ResourceFormHeader>
          <ResourceFormTitle>Add Service · {env.name}</ResourceFormTitle>
        </ResourceFormHeader>
        <ServiceFormBody
          inline
          env={env}
          workspace={params.tenant}
          onClose={close}
          onDiscard={(reason) => {
            setDirty(false);
            // The blocked navigation owns the next URL; closing via search
            // here would replace it with the current Services destination.
            if (reason !== "navigation") onOpenChange(false);
          }}
          onDirtyChange={setDirty}
          onSaved={() => {
            session?.finish();
            setDirty(false);
          }}
        />
      </ResourceForm>
    </FormDirty.Provider>
  );
}
