import { Autocomplete } from "@base-ui/react/autocomplete";
import { Input } from "./input";

export function AutocompleteInput({
  value,
  onValueChange,
  options,
  emptyText = "No matching suggestions. You can enter a name.",
  disabled,
  required,
  ...inputProps
}: Omit<React.ComponentProps<typeof Input>, "value" | "onChange"> & {
  value: string;
  onValueChange: (value: string) => void;
  options: string[];
  emptyText?: string;
}) {
  return (
    <Autocomplete.Root
      items={options}
      value={value}
      onValueChange={onValueChange}
      disabled={disabled}
      required={required}
      openOnInputClick
    >
      <Autocomplete.Input render={<Input {...inputProps} />} />
      <Autocomplete.Portal>
        <Autocomplete.Positioner sideOffset={6} className="z-50 outline-none">
          <Autocomplete.Popup className="w-[var(--anchor-width)] max-w-[calc(100vw-2rem)] overflow-hidden rounded-lg border border-border bg-popover p-1 text-popover-foreground shadow-xl outline-none transition-opacity duration-150 data-[ending-style]:opacity-0 data-[starting-style]:opacity-0">
            <Autocomplete.Empty className="p-3 text-sm text-muted-foreground">
              {emptyText}
            </Autocomplete.Empty>
            <Autocomplete.List className="max-h-64 overflow-y-auto overscroll-contain">
              {(option: string) => (
                <Autocomplete.Item
                  key={option}
                  value={option}
                  className="cursor-default rounded-md p-2 text-sm outline-none select-none break-all data-[highlighted]:bg-muted"
                >
                  {option}
                </Autocomplete.Item>
              )}
            </Autocomplete.List>
          </Autocomplete.Popup>
        </Autocomplete.Positioner>
      </Autocomplete.Portal>
    </Autocomplete.Root>
  );
}
