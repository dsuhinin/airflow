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
import dayjs, { type Dayjs } from "dayjs";

import type { CalendarTimeRangeResponse } from "openapi/requests/types.gen";

import { DATE_FORMAT } from "src/utils/datetimeUtils";

import type { BucketUnit, CalendarPeriod } from "./types";

// Stage 1 placeholder: the layout is wired to this generator instead of the calendar API.
// The output is deterministic per slot so a period looks the same on every render, and the
// unit mirrors the API's granularity so each view only produces what it displays.

const hashString = (value: string): number => {
  let hash = 5381;

  for (const char of value) {
    hash = (hash * 33 + char.charCodeAt(0)) % 2_147_483_647;
  }

  return hash;
};

const pickCount = (seed: number): number =>
  seed % 97 === 0 ? 900 + (seed % 700) : seed % 11 === 0 ? 20 + (seed % 90) : 1 + (seed % 4);

// A day slot stands for 24 hours, so its states show up far more often than an hour's.
const FREQUENCY: Record<BucketUnit, Record<CalendarTimeRangeResponse["state"], number>> = {
  day: { failed: 4, planned: 2, queued: 13, running: 9, success: 2 },
  hour: { failed: 19, planned: 11, queued: 61, running: 37, success: 7 },
};

const buildSlot = (slot: Dayjs, now: Dayjs, unit: BucketUnit): Array<CalendarTimeRangeResponse> => {
  if (hashString(slot.format(DATE_FORMAT)) % 3 === 0) {
    return [];
  }

  const date = slot.toISOString();
  const seed = hashString(date);
  const every = FREQUENCY[unit];

  if (slot.isAfter(now)) {
    return seed % every.planned === 0 ? [{ count: 1 + (seed % 3), date, state: "planned" }] : [];
  }

  const runs: Array<CalendarTimeRangeResponse> = [];

  if (seed % every.success === 0) {
    runs.push({ count: pickCount(seed), date, is_backfill: seed % 17 === 0, state: "success" });
  }
  if (seed % every.failed === 0) {
    runs.push({ count: 1 + (seed % 5), date, is_backfill: seed % 23 === 0, state: "failed" });
  }
  if (seed % every.running === 0) {
    runs.push({ count: 1 + (seed % 3), date, state: "running" });
  }
  if (seed % every.queued === 0) {
    runs.push({ count: 1 + (seed % 2), date, state: "queued" });
  }

  return runs;
};

export const buildSampleRuns = (
  period: CalendarPeriod,
  unit: BucketUnit,
): Array<CalendarTimeRangeResponse> => {
  const now = dayjs();
  const runs: Array<CalendarTimeRangeResponse> = [];
  let slot = period.start.startOf(unit);

  while (!slot.isAfter(period.end)) {
    runs.push(...buildSlot(slot, now, unit));
    slot = slot.add(1, unit);
  }

  return runs;
};
