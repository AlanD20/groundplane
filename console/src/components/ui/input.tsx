import { cn } from "@/lib/utils";
import { useId, useState } from "react";
import { Eye, EyeOff } from "lucide-react";
import { Button } from "./button";

function InputControl({
  className,
  type,
  ...props
}: React.ComponentProps<"input">) {
  return (
    <input
      type={type}
      data-slot="input"
      className={cn(
        "flex h-10 w-full min-w-0 rounded-lg border border-input bg-background px-3 py-1 text-sm outline-none transition-colors",
        "placeholder:text-muted-foreground focus-visible:border-ring focus-visible:ring-1 focus-visible:ring-ring/70",
        "disabled:pointer-events-none disabled:opacity-50 file:border-0 file:bg-transparent file:text-sm",
        className,
      )}
      {...props}
    />
  );
}

function SecretInput({
  id,
  className,
  disabled,
  ...props
}: Omit<React.ComponentProps<"input">, "type">) {
  const generatedId = useId();
  const inputId = id ?? generatedId;
  const [visible, setVisible] = useState(false);
  return (
    <div className="relative w-full min-w-0">
      <InputControl
        autoCapitalize="none"
        spellCheck={false}
        {...props}
        id={inputId}
        disabled={disabled}
        type={visible ? "text" : "password"}
        className={cn(className, "pr-24")}
      />
      <Button
        type="button"
        variant="ghost"
        size="sm"
        className="absolute right-1 top-1/2 -translate-y-1/2"
        aria-controls={inputId}
        aria-label={visible ? "Hide secret value" : "Show secret value"}
        disabled={disabled}
        onClick={() => setVisible((current) => !current)}
      >
        {visible ? <EyeOff aria-hidden="true" /> : <Eye aria-hidden="true" />}
        {visible ? "Hide" : "Show"}
      </Button>
    </div>
  );
}

function Input({ type, ...props }: React.ComponentProps<"input">) {
  return type === "password" ? (
    <SecretInput {...props} />
  ) : (
    <InputControl type={type} {...props} />
  );
}

export { Input };
