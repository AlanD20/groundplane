import { useId, useState } from "react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Dialog, DialogContent, DialogDescription, DialogFooter,
  DialogHeader, DialogTitle,
} from "@/components/ui/dialog";

export function BulkRevealConfirmation({
  open, onOpenChange, onConfirm, subject,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  onConfirm: () => void;
  subject: string;
}) {
  const id = useId();
  const [word, setWord] = useState("");
  function confirm() {
    if (word !== "reveal") return;
    setWord("");
    onOpenChange(false);
    onConfirm();
  }
  return (
    <Dialog open={open} onOpenChange={(next) => { setWord(""); onOpenChange(next); }}>
      <DialogContent data-form-shortcuts="ignore" onKeyDown={(event) => {
        if (event.key === "Enter" && event.target instanceof HTMLInputElement) {
          event.preventDefault();
          event.stopPropagation();
          confirm();
        }
      }}>
        <DialogHeader>
          <DialogTitle>Reveal existing secrets?</DialogTitle>
          <DialogDescription>Load {subject} into this editor.</DialogDescription>
        </DialogHeader>
        <Label htmlFor={id}>Type reveal to continue</Label>
        <Input id={id} autoFocus value={word} onChange={(event) => setWord(event.target.value)} />
        <DialogFooter>
          <Button type="button" variant="outline" onClick={() => { setWord(""); onOpenChange(false); }}>Cancel</Button>
          <Button type="button" disabled={word !== "reveal"} onClick={confirm}>Reveal secrets</Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
