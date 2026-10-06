import { useMemo, useState } from "react";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Select } from "@/components/ui/select";
import { Badge } from "@/components/ui/badge";
import {
  backupScheduleControls,
  backupSchedulePreview,
  presetBackupSchedule,
  WEEKDAYS,
  type SchedulePreset,
} from "./backup-schedule";

const dayLabels = [
  "Sunday",
  "Monday",
  "Tuesday",
  "Wednesday",
  "Thursday",
  "Friday",
  "Saturday",
];
const dateFormat = new Intl.DateTimeFormat("en", {
  timeZone: "UTC",
  month: "short",
  day: "numeric",
  year: "numeric",
  hour: "2-digit",
  minute: "2-digit",
  hourCycle: "h23",
});

export function BackupScheduleEditor({
  value,
  onChange,
  disabled,
}: {
  value: string;
  onChange: (value: string) => void;
  disabled: boolean;
}) {
  const controls = backupScheduleControls(value);
  const [preset, setPreset] = useState<SchedulePreset>(
    value ? controls.preset : "daily",
  );
  const [time, setTime] = useState(controls.time);
  const [minute, setMinute] = useState(controls.minute);
  const [weekday, setWeekday] = useState(controls.weekday);
  const preview = useMemo(
    () => backupSchedulePreview(value, new Date()),
    [value],
  );
  const update = (
    mode: SchedulePreset,
    nextTime: string,
    nextMinute: string,
    nextWeekday: string,
  ) => {
    if (mode !== "custom")
      onChange(
        nextTime && nextMinute
          ? presetBackupSchedule(mode, nextTime, nextMinute, nextWeekday)
          : "",
      );
  };
  return (
    <section
      className="flex min-w-0 flex-col gap-3"
      aria-label="Backup schedule"
    >
      <div className="flex items-center justify-between">
        <Label htmlFor="bp-schedule">Frequency</Label>
        <Badge variant="outline">UTC</Badge>
      </div>
      <div className="grid min-w-0 gap-3 sm:grid-cols-2">
        <Select
          id="bp-schedule"
          aria-label="Frequency"
          value={preset}
          disabled={disabled}
          options={[
            { value: "hourly", label: "Hourly" },
            { value: "daily", label: "Daily" },
            { value: "weekly", label: "Weekly" },
            { value: "custom", label: "Custom cron" },
          ]}
          onValueChange={(next) => {
            if (
              next !== "hourly" &&
              next !== "daily" &&
              next !== "weekly" &&
              next !== "custom"
            )
              return;
            setPreset(next);
            update(next, time, minute, weekday);
          }}
        />
        {preset === "hourly" && (
          <div className="flex flex-col gap-1.5">
            <Label htmlFor="bp-minute">Minute of each hour</Label>
            <Input
              id="bp-minute"
              type="number"
              min={0}
              max={59}
              step={1}
              value={minute}
              disabled={disabled}
              onChange={(event) => {
                const next = event.target.value;
                setMinute(next);
                update(preset, time, next, weekday);
              }}
            />
          </div>
        )}
        {(preset === "daily" || preset === "weekly") && (
          <div className="flex flex-col gap-1.5">
            <Label htmlFor="bp-time">Time · UTC</Label>
            <Input
              id="bp-time"
              type="time"
              step={60}
              value={time}
              disabled={disabled}
              onChange={(event) => {
                const next = event.target.value;
                setTime(next);
                update(preset, next, minute, weekday);
              }}
            />
          </div>
        )}
        {preset === "weekly" && (
          <div className="flex flex-col gap-1.5 sm:col-span-2">
            <Label htmlFor="bp-weekday">Day</Label>
            <Select
              id="bp-weekday"
              aria-label="Day"
              value={weekday}
              disabled={disabled}
              options={WEEKDAYS.map((value, i) => ({
                value,
                label: dayLabels[i],
              }))}
              onValueChange={(next) => {
                setWeekday(next);
                update(preset, time, minute, next);
              }}
            />
          </div>
        )}
        {preset === "custom" && (
          <div className="flex flex-col gap-1.5 sm:col-span-2">
            <Label htmlFor="bp-cron">Cron expression</Label>
            <Input
              id="bp-cron"
              value={value}
              disabled={disabled}
              onChange={(event) => onChange(event.target.value)}
              placeholder="15 * * * *"
              className="font-mono"
              aria-describedby="bp-cron-help"
              maxLength={256}
            />
            <p id="bp-cron-help" className="text-xs text-muted-foreground">
              Minute, hour, day of month, month, weekday. No seconds field.
            </p>
          </div>
        )}
      </div>
      {value &&
        (preview.error ? (
          <p className="text-xs text-destructive" role="status">
            {preview.error}
          </p>
        ) : (
          <div className="rounded-lg border border-border bg-surface p-3 text-xs">
            <p className="font-medium">{preview.summary} · UTC</p>
            <p className="mt-2 text-muted-foreground">Next 3 scheduled runs</p>
            <ol className="mt-1 flex flex-col gap-1">
              {preview.nextRuns.map((date) => (
                <li key={date.toISOString()}>
                  <time dateTime={date.toISOString()}>
                    {dateFormat.format(date)}
                  </time>
                </li>
              ))}
            </ol>
            <p className="mt-2 break-all font-mono text-muted-foreground">
              {value}
            </p>
          </div>
        ))}
    </section>
  );
}
