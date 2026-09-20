"use client";

import { useState } from "react";
import { useRequiredParams } from "@/lib/router";
import { Plus } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Drawer } from "@/components/ui/drawer";
import { ServiceFormBody } from "@/components/common/service-form-body";
import type { Environment } from "@/lib/types";

// ---- Forms ----

export function ServiceFormDialog({ env }: { env: Environment }) {
  const params = useRequiredParams("tenant");
  const [open, setOpen] = useState(false);
  return (
    <>
      <Button size="sm" onClick={() => setOpen(true)}>
        <Plus className="size-3.5" /> Service
      </Button>
      <Drawer open={open} onOpenChange={setOpen}>
        <ServiceFormBody
          env={env}
          workspace={params.tenant}
          onClose={() => setOpen(false)}
        />
      </Drawer>
    </>
  );
}
