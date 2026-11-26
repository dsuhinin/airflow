/*!
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
import dayjs, { type Dayjs, type ManipulateType } from "dayjs";
import advancedFormat from "dayjs/plugin/advancedFormat";
import tz from "dayjs/plugin/timezone";
import utc from "dayjs/plugin/utc";

import type { CalendarTimeRangeResponse } from "openapi/requests/types.gen";

import { SearchParamsKeys } from "src/constants/searchParams";
import { DATE_FORMAT } from "src/utils/datetimeUtils";
import { isOneOf } from "src/utils/isOneOf";

import {
  FAILURE_COLOR_INTENSITIES,
  PLANNED_COLOR,
  RUNNING_COLOR,
  TOTAL_COLOR_INTENSITIES,
} from "../Calendar/calendarUtils";
import type {
  BarColors,
  BarState,
  BucketUnit,
  CalendarDateField,
  CalendarPeriod,
  CalendarStateFilter,
  CalendarView,
  StateBucket,
  ThemeColor,
  TimeBucket,
} from "./types";

dayjs.extend(advancedFormat);
dayjs.extend(utc);
dayjs.extend(tz);

export const CALENDAR_VIEWS = [
  "day",
  "week",
  "month",
  "year",
  "range",
] as const satisfies ReadonlyArray<CalendarView>;
export const CALENDAR_DATE_FIELDS = [
  "start_date",
  "end_date",
  "logical_date",
  "run_after",
] as const satisfies ReadonlyArray<CalendarDateField>;
const CALENDAR_STATE_FILTERS = [
  "all",
  "success",
  "failed",
] as const satisfies ReadonlyArray<CalendarStateFilter>;

export const isCalendarView = isOneOf(CALENDAR_VIEWS);
export const isCalendarDateField = isOneOf(CALENDAR_DATE_FIELDS);
export const isCalendarStateFilter = isOneOf(CALENDAR_STATE_FILTERS);

export const BAR_STATE_ORDER: ReadonlyArray<BarState> = ["success", "failed", "running", "planned"];

export const HOURS = Array.from({ length: 24 }, (_, hour) => String(hour).padStart(2, "0"));
export const MONTHS = Array.from({ length: 12 }, (_, month) => month);

/** Periods up to a week are shown hour by hour, longer ones day by day. */
export const MAX_HOURLY_DAYS = 7;

const PERIOD_UNITS: Record<Exclude<CalendarView, "range">, ManipulateType> = {
  day: "day",
  month: "month",
  week: "week",
  year: "year",
};

export const getPeriod = (view: CalendarView, date: Dayjs, range?: CalendarPeriod): CalendarPeriod => {
  if (view === "range") {
    return (
      range ?? { end: date.endOf("day"), start: date.subtract(MAX_HOURLY_DAYS - 1, "day").startOf("day") }
    );
  }

  const unit = PERIOD_UNITS[view];

  // The month grid is padded to full weeks, and the padding days show their runs too.
  return view === "month"
    ? { end: date.endOf(unit).endOf("week"), start: date.startOf(unit).startOf("week") }
    : { end: date.endOf(unit), start: date.startOf(unit) };
};

export const shiftDate = (view: Exclude<CalendarView, "range">, date: Dayjs, direction: -1 | 1): Dayjs =>
  date.add(direction, PERIOD_UNITS[view]);

export const shiftPeriod = (period: CalendarPeriod, direction: -1 | 1): CalendarPeriod => {
  const days = period.end.diff(period.start, "day") + 1;

  return {
    end: period.end.add(direction * days, "day"),
    start: period.start.add(direction * days, "day"),
  };
};

export const formatPeriodTitle = (view: CalendarView, date: Dayjs, period: CalendarPeriod): string => {
  switch (view) {
    case "day":
      return date.format("dddd, MMM Do YYYY");
    case "month":
      return date.format("MMMM YYYY");
    case "range":
      return `${period.start.format("MMM D, YYYY")} – ${period.end.format("MMM D, YYYY")}`;
    case "week":
      return `${period.start.format("MMM Do")} – ${period.end.format("MMM Do YYYY")}`;
    case "year":
      return date.format("YYYY");
    default:
      return "";
  }
};

export const getDaysInPeriod = (period: CalendarPeriod): Array<Dayjs> => {
  const days: Array<Dayjs> = [];
  let day = period.start.startOf("day");

  while (!day.isAfter(period.end, "day")) {
    days.push(day);
    day = day.add(1, "day");
  }

  return days;
};

export const getHourKey = (dayKey: string, hour: string): string => `${dayKey}T${hour}`;

export const getBucketKey = (date: Dayjs, unit: BucketUnit): string =>
  unit === "day" ? date.format(DATE_FORMAT) : getHourKey(date.format(DATE_FORMAT), date.format("HH"));

export const filterRunsByState = (
  runs: Array<CalendarTimeRangeResponse>,
  filter: CalendarStateFilter,
): Array<CalendarTimeRangeResponse> => (filter === "all" ? runs : runs.filter((run) => run.state === filter));

// Queued runs are not scheduled yet, so the design shows them together with planned ones.
const toBarState = (state: CalendarTimeRangeResponse["state"]): BarState =>
  state === "queued" ? "planned" : state;

const summarizeStates = (runs: Array<CalendarTimeRangeResponse>): Array<StateBucket> => {
  const byState = new Map<BarState, StateBucket>();

  runs.forEach((run) => {
    const state = toBarState(run.state);
    const existing = byState.get(state);

    byState.set(state, {
      count: (existing?.count ?? 0) + run.count,
      hasBackfill: (existing?.hasBackfill ?? false) || Boolean(run.is_backfill),
      state,
    });
  });

  return BAR_STATE_ORDER.flatMap((state) => {
    const bucket = byState.get(state);

    return bucket && bucket.count > 0 ? [bucket] : [];
  });
};

/** Groups runs by day or hour in the selected timezone, keyed with `getBucketKey`. */
export const bucketRuns = (
  runs: Array<CalendarTimeRangeResponse>,
  timezone: string,
  unit: BucketUnit,
): Map<string, TimeBucket> => {
  const grouped = new Map<string, Array<CalendarTimeRangeResponse>>();

  runs.forEach((run) => {
    const key = getBucketKey(dayjs(run.date).tz(timezone), unit);

    grouped.set(key, [...(grouped.get(key) ?? []), run]);
  });

  const buckets = new Map<string, TimeBucket>();

  grouped.forEach((bucketRunsList, key) => {
    const states = summarizeStates(bucketRunsList);

    buckets.set(key, {
      date: dayjs.tz(key, timezone),
      states,
      total: states.reduce((sum, bucket) => sum + bucket.count, 0),
    });
  });

  return buckets;
};

export const getMaxStateCount = (buckets: Map<string, TimeBucket>): number =>
  Math.max(0, ...[...buckets.values()].flatMap((bucket) => bucket.states.map((state) => state.count)));

export const getTotalRuns = (buckets: Map<string, TimeBucket>): number =>
  [...buckets.values()].reduce((sum, bucket) => sum + bucket.total, 0);

const MAX_INTENSITY = TOTAL_COLOR_INTENSITIES.length - 1;

export const getIntensityLevel = (count: number, max: number): number =>
  count <= 0 || max <= 0 ? 0 : Math.min(MAX_INTENSITY, Math.max(1, Math.ceil((count / max) * MAX_INTENSITY)));

const INVERTED_TEXT = { _dark: "gray.900", _light: "white" };

// Light bars keep the page text color, the two darkest levels need an inverted one.
const INTENSITY_TEXT: Array<ThemeColor> = ["fg", "fg", "white", INVERTED_TEXT, INVERTED_TEXT];

const BAR_STYLES: Record<BarState, { bg: Array<ThemeColor>; fg: Array<ThemeColor> }> = {
  failed: { bg: FAILURE_COLOR_INTENSITIES, fg: INTENSITY_TEXT },
  planned: { bg: [PLANNED_COLOR], fg: ["white"] },
  running: { bg: [RUNNING_COLOR], fg: ["fg"] },
  success: { bg: TOTAL_COLOR_INTENSITIES, fg: INTENSITY_TEXT },
};

export const getBarColors = (state: BarState, level: number): BarColors => {
  const { bg, fg } = BAR_STYLES[state];
  const index = Math.min(level, bg.length - 1);

  return { bg: bg[index] ?? PLANNED_COLOR, fg: fg[index] ?? "fg" };
};

export const getRunsLink = (dagId: string, start: Dayjs, state: BarState): string => {
  const params = new URLSearchParams({
    [SearchParamsKeys.SORT]: "start_date",
    [SearchParamsKeys.START_DATE_GTE]: start.toISOString(),
  });

  if (state !== "planned") {
    params.set(SearchParamsKeys.STATE, state);
  }

  return `/dags/${dagId}/runs?${params.toString()}`;
};
