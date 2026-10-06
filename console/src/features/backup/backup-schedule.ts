import cronstrue from "cronstrue";

const MONTHS = [
  "JAN",
  "FEB",
  "MAR",
  "APR",
  "MAY",
  "JUN",
  "JUL",
  "AUG",
  "SEP",
  "OCT",
  "NOV",
  "DEC",
];
export const WEEKDAYS = ["SUN", "MON", "TUE", "WED", "THU", "FRI", "SAT"];
export const DEFAULT_BACKUP_FREQUENCY = "15 3 * * *";
export type SchedulePreset = "hourly" | "daily" | "weekly" | "custom";

type Field = { values: number[]; wildcard: boolean };

function parseField(
  raw: string,
  min: number,
  max: number,
  names: string[] = [],
): Field {
  const values = new Set<number>();
  let wildcard = false;
  const number = (token: string) => {
    const named = names.indexOf(token.toUpperCase());
    const value =
      named >= 0 ? min + named : /^\d+$/.test(token) ? Number(token) : NaN;
    if (!Number.isSafeInteger(value) || value < min || value > max)
      throw new Error("Invalid cron field");
    return value;
  };
  for (const part of raw.split(",")) {
    const pieces = part.split("/");
    if (pieces.length > 2 || !pieces[0]) throw new Error("Invalid cron field");
    const step =
      pieces.length === 2 && /^\d+$/.test(pieces[1])
        ? Number(pieces[1])
        : pieces.length === 1
          ? 1
          : NaN;
    if (!Number.isSafeInteger(step) || step < 1)
      throw new Error("Invalid cron step");
    const range = pieces[0].split("-");
    const star = range[0] === "*";
    if (range.length > 2 || (star && range.length !== 1))
      throw new Error("Invalid cron range");
    const start = star ? min : number(range[0]);
    const end = star
      ? max
      : range.length === 2
        ? number(range[1])
        : pieces.length === 2
          ? max
          : start;
    if (end < start) throw new Error("Invalid cron range");
    for (let value = start; value <= end; value += step) values.add(value);
    wildcard ||= star && step === 1;
  }
  return { values: [...values].sort((a, b) => a - b), wildcard };
}

function parseSchedule(value: string) {
  const fields = value.trim().split(/\s+/);
  if (value.length > 256 || fields.length !== 5)
    throw new Error("Enter five cron fields: minute hour day month weekday.");
  const [minute, hour, dom, month, dow] = [
    parseField(fields[0], 0, 59),
    parseField(fields[1], 0, 23),
    parseField(fields[2], 1, 31),
    parseField(fields[3], 1, 12, MONTHS),
    parseField(fields[4], 0, 6, WEEKDAYS),
  ];
  const matchesDay = (day: Date) => {
    if (!month.values.includes(day.getUTCMonth() + 1)) return false;
    const dateMatches = dom.values.includes(day.getUTCDate());
    const weekdayMatches = dow.values.includes(day.getUTCDay());
    return dom.wildcard || dow.wildcard
      ? dateMatches && weekdayMatches
      : dateMatches || weekdayMatches;
  };
  // Use a leap year to reject impossible dates, matching Controller admission.
  const day = new Date(Date.UTC(2000, 0, 1));
  let possible = false;
  while (day.getUTCFullYear() === 2000 && !possible) {
    possible = matchesDay(day);
    day.setUTCDate(day.getUTCDate() + 1);
  }
  if (!possible)
    throw new Error("This schedule has no possible calendar date.");
  const next = (after: Date) => {
    const date = new Date(
      Date.UTC(after.getUTCFullYear(), after.getUTCMonth(), after.getUTCDate()),
    );
    for (let i = 0; i <= 8 * 366; i++, date.setUTCDate(date.getUTCDate() + 1)) {
      if (!matchesDay(date)) continue;
      for (const h of hour.values)
        for (const m of minute.values) {
          const candidate = new Date(date);
          candidate.setUTCHours(h, m, 0, 0);
          if (candidate > after) return candidate;
        }
    }
    throw new Error("No next run could be calculated.");
  };
  return { next };
}

export function backupScheduleError(value: string): string | null {
  try {
    parseSchedule(value);
    return null;
  } catch (error) {
    return error instanceof Error && error.message.includes("cron fields")
      ? error.message
      : "Enter a valid five-field UTC cron expression.";
  }
}

export function backupSchedulePreview(value: string, now: Date) {
  try {
    const schedule = parseSchedule(value);
    const nextRuns: Date[] = [];
    let after = now;
    for (let i = 0; i < 3; i++) {
      after = schedule.next(after);
      nextRuns.push(after);
    }
    return {
      summary: cronstrue.toString(value, {
        use24HourTimeFormat: true,
        logicalAndDayFields: false,
      }),
      nextRuns,
      error: null,
    };
  } catch {
    return {
      summary: "",
      nextRuns: [],
      error: backupScheduleError(value) ?? "No next run could be calculated.",
    };
  }
}

export function backupScheduleControls(value: string) {
  const fields = value.trim().split(/\s+/);
  const [minute, hour, day, month, weekday] = fields;
  const time =
    /^\d+$/.test(hour) && /^\d+$/.test(minute)
      ? `${hour.padStart(2, "0")}:${minute.padStart(2, "0")}`
      : "03:15";
  const minuteValue = /^\d+$/.test(minute) ? minute : "15";
  const weekdayValue = WEEKDAYS.includes(weekday?.toUpperCase())
    ? weekday.toUpperCase()
    : /^[0-6]$/.test(weekday)
      ? WEEKDAYS[Number(weekday)]
      : "SUN";
  let preset: SchedulePreset = "custom";
  if (
    fields.length === 5 &&
    !backupScheduleError(value) &&
    day === "*" &&
    month === "*" &&
    /^\d+$/.test(minute)
  ) {
    if (hour === "*" && weekday === "*") preset = "hourly";
    else if (/^\d+$/.test(hour) && weekday === "*") preset = "daily";
    else if (
      /^\d+$/.test(hour) &&
      (WEEKDAYS.includes(weekday.toUpperCase()) || /^[0-6]$/.test(weekday))
    )
      preset = "weekly";
  }
  return { preset, time, minute: minuteValue, weekday: weekdayValue };
}

export function backupScheduleDescription(value: string) {
  try {
    return cronstrue.toString(value, {
      use24HourTimeFormat: true,
      logicalAndDayFields: false,
    });
  } catch {
    return value;
  }
}

export function presetBackupSchedule(
  preset: Exclude<SchedulePreset, "custom">,
  time: string,
  minute: string,
  weekday: string,
) {
  if (preset === "hourly") return `${minute} * * * *`;
  const [h, m] = time.split(":");
  return `${Number(m)} ${Number(h)} * * ${preset === "weekly" ? weekday : "*"}`;
}
