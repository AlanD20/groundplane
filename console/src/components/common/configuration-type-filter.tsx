import { Select } from "@/components/ui/select";

export function ConfigurationTypeFilter({
  value,
  onChange,
}: {
  value: string;
  onChange: (value: string) => void;
}) {
  return (
    <Select
      className="w-full shrink-0 sm:w-40"
      value={value}
      onValueChange={onChange}
      aria-label="Configuration type"
      options={[
        { value: "all", label: "All types" },
        { value: "variable", label: "Variables" },
        { value: "secret", label: "Secrets" },
        { value: "file", label: "Files" },
      ]}
    />
  );
}
