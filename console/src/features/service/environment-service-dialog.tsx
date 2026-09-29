"use client";

import { ServiceFormBody } from "@/components/common/service-form-body";
import { Button } from "@/components/ui/button";
import { Drawer } from "@/components/ui/drawer";
import { useRequiredParams } from "@/lib/router";
import type { Environment } from "@/lib/types";
import { Plus } from "lucide-react";
import { useState } from "react";

// ---- Forms ----

export function ServiceFormDialog({ env }: { env: Environment }) {
  const params = useRequiredParams("tenant");
  const [open, setOpen] = useState(false);
  return (
    <>
      <Button size="sm" onClick={() => setOpen(true)}>
        <Plus className="size-3.5" /> Add Service
      </Button>
      <Drawer open={open} onOpenChange={setOpen}>
        {open && (
          <ServiceFormBody
            env={env}
            workspace={params.tenant}
            onClose={() => setOpen(false)}
          />
        )}
      </Drawer>
    </>
  );
}
