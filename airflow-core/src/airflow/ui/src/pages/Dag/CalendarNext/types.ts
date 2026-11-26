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
import type { Dayjs } from "dayjs";

export type CalendarView = "day" | "month" | "range" | "week" | "year";

export type CalendarDateField = "end_date" | "logical_date" | "run_after" | "start_date";

export type CalendarStateFilter = "all" | "failed" | "success";

export type BucketUnit = "day" | "hour";

export type BarState = "failed" | "planned" | "running" | "success";

export type StateBucket = {
  readonly count: number;
  readonly hasBackfill: boolean;
  readonly state: BarState;
};

export type TimeBucket = {
  readonly date: Dayjs;
  readonly states: Array<StateBucket>;
  readonly total: number;
};

export type CalendarPeriod = {
  readonly end: Dayjs;
  readonly start: Dayjs;
};

export type ThemeColor = string | { _dark: string; _light: string };

export type BarColors = {
  readonly bg: ThemeColor;
  readonly fg: ThemeColor;
};

/** Translated state names and the locale for counters, resolved once per view instead of per bar. */
export type BarLabels = {
  readonly language: string;
  readonly states: Record<BarState, string>;
};
