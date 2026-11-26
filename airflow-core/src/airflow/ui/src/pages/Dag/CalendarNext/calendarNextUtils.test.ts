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
import dayjs from "dayjs";
import tz from "dayjs/plugin/timezone";
import utc from "dayjs/plugin/utc";
import { describe, expect, it } from "vitest";

import type { CalendarTimeRangeResponse } from "openapi/requests/types.gen";

import {
  bucketRuns,
  filterRunsByState,
  formatPeriodTitle,
  getBarColors,
  getBucketKey,
  getDaysInPeriod,
  getIntensityLevel,
  getMaxStateCount,
  getPeriod,
  getRunsLink,
  getTotalRuns,
  shiftDate,
  shiftPeriod,
} from "./calendarNextUtils";
import { buildSampleRuns } from "./sampleData";

dayjs.extend(utc);
dayjs.extend(tz);

const date = dayjs.utc("2026-09-16T10:00:00Z");

const runs: Array<CalendarTimeRangeResponse> = [
  { count: 3, date: "2026-09-16T10:00:00Z", state: "success" },
  { count: 1, date: "2026-09-16T10:30:00Z", is_backfill: true, state: "success" },
  { count: 2, date: "2026-09-16T11:00:00Z", state: "failed" },
  { count: 4, date: "2026-09-16T11:00:00Z", state: "queued" },
  { count: 5, date: "2026-09-17T01:00:00Z", state: "planned" },
];

describe("getPeriod", () => {
  it.each([
    ["day", "2026-09-16", "2026-09-16"],
    ["week", "2026-09-13", "2026-09-19"],
    ["month", "2026-08-30", "2026-10-03"],
    ["year", "2026-01-01", "2026-12-31"],
  ] as const)("covers the %s around the date", (view, start, end) => {
    const period = getPeriod(view, date);

    expect(period.start.format("YYYY-MM-DD")).toBe(start);
    expect(period.end.format("YYYY-MM-DD")).toBe(end);
  });

  it("falls back to the last seven days when no custom range is given", () => {
    const period = getPeriod("range", date);

    expect(period.start.format("YYYY-MM-DD")).toBe("2026-09-10");
    expect(period.end.format("YYYY-MM-DD")).toBe("2026-09-16");
  });

  it("returns the custom range as is", () => {
    const range = { end: date.add(2, "day"), start: date };

    expect(getPeriod("range", date, range)).toBe(range);
  });
});

describe("navigation", () => {
  it.each([
    ["day", "2026-09-17"],
    ["week", "2026-09-23"],
    ["month", "2026-10-16"],
    ["year", "2027-09-16"],
  ] as const)("shifts a %s forward", (view, expected) => {
    expect(shiftDate(view, date, 1).format("YYYY-MM-DD")).toBe(expected);
  });

  it("shifts a custom range by its own length", () => {
    const period = {
      end: dayjs.utc("2026-09-16").endOf("day"),
      start: dayjs.utc("2026-09-14").startOf("day"),
    };
    const next = shiftPeriod(period, -1);

    expect(next.start.format("YYYY-MM-DD")).toBe("2026-09-11");
    expect(next.end.format("YYYY-MM-DD")).toBe("2026-09-13");
  });
});

describe("formatPeriodTitle", () => {
  const period = { end: dayjs.utc("2026-09-19").endOf("day"), start: dayjs.utc("2026-09-13").startOf("day") };

  it.each([
    ["day", "Wednesday, Sep 16th 2026"],
    ["week", "Sep 13th – Sep 19th 2026"],
    ["month", "September 2026"],
    ["year", "2026"],
    ["range", "Sep 13, 2026 – Sep 19, 2026"],
  ] as const)("formats the %s title", (view, expected) => {
    expect(formatPeriodTitle(view, date, period)).toBe(expected);
  });
});

describe("bucketRuns", () => {
  it("keys buckets by day or hour", () => {
    expect(getBucketKey(date, "day")).toBe("2026-09-16");
    expect(getBucketKey(date, "hour")).toBe("2026-09-16T10");
  });

  it("groups runs by hour and folds queued runs into planned", () => {
    const buckets = bucketRuns(runs, "UTC", "hour");

    expect([...buckets.keys()]).toEqual(["2026-09-16T10", "2026-09-16T11", "2026-09-17T01"]);
    expect(buckets.get("2026-09-16T10")?.states).toEqual([{ count: 4, hasBackfill: true, state: "success" }]);
    expect(buckets.get("2026-09-16T11")?.states).toEqual([
      { count: 2, hasBackfill: false, state: "failed" },
      { count: 4, hasBackfill: false, state: "planned" },
    ]);
    expect(getTotalRuns(buckets)).toBe(15);
    expect(getMaxStateCount(buckets)).toBe(5);
  });

  it("groups runs by day in the selected timezone", () => {
    const buckets = bucketRuns(runs, "Asia/Tokyo", "day");

    expect([...buckets.keys()]).toEqual(["2026-09-16", "2026-09-17"]);
    expect(buckets.get("2026-09-16")?.total).toBe(10);
    expect(buckets.get("2026-09-17")?.date.format("YYYY-MM-DD")).toBe("2026-09-17");
  });

  it("keeps only the requested state", () => {
    expect(filterRunsByState(runs, "failed")).toHaveLength(1);
    expect(filterRunsByState(runs, "all")).toBe(runs);
  });
});

describe("bar styling", () => {
  it.each([
    [0, 10, 0],
    [1, 10, 1],
    [5, 10, 2],
    [7, 10, 3],
    [10, 10, 4],
    [3, 0, 0],
  ])("maps %i of %i to level %i", (count, max, level) => {
    expect(getIntensityLevel(count, max)).toBe(level);
  });

  it("uses the heatmap palettes for success and failed bars", () => {
    expect(getBarColors("success", 1)).toEqual({
      bg: { _dark: "green.900", _light: "green.200" },
      fg: "fg",
    });
    expect(getBarColors("failed", 4)).toEqual({
      bg: { _dark: "red.300", _light: "red.800" },
      fg: { _dark: "gray.900", _light: "white" },
    });
    expect(getBarColors("planned", 2).fg).toBe("white");
  });
});

describe("getRunsLink", () => {
  it("filters the runs list by start date and state", () => {
    expect(getRunsLink("my_dag", date, "failed")).toBe(
      "/dags/my_dag/runs?sort=start_date&start_date_gte=2026-09-16T10%3A00%3A00.000Z&state=failed",
    );
  });

  it("leaves out the state for planned runs", () => {
    expect(getRunsLink("my_dag", date, "planned")).not.toContain("state=");
  });
});

describe("sample data", () => {
  it("lists every day of the period", () => {
    const period = {
      end: dayjs.utc("2026-09-18").endOf("day"),
      start: dayjs.utc("2026-09-16").startOf("day"),
    };

    expect(getDaysInPeriod(period).map((day) => day.date())).toEqual([16, 17, 18]);
  });

  it("is deterministic and stays inside the period", () => {
    const period = getPeriod("week", date);
    const first = buildSampleRuns(period, "hour");

    expect(first).toEqual(buildSampleRuns(period, "hour"));
    expect(buildSampleRuns(getPeriod("year", date), "day").length).toBeGreaterThan(0);
    expect(first.length).toBeGreaterThan(0);
    first.forEach((run) => {
      expect(dayjs(run.date).isBefore(period.start)).toBe(false);
      expect(dayjs(run.date).isAfter(period.end)).toBe(false);
    });
  });
});
