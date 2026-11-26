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
import { Box, VStack } from "@chakra-ui/react";
import dayjs, { type Dayjs } from "dayjs";
import { useTranslation } from "react-i18next";
import { useParams, useSearchParams } from "react-router-dom";

import { SearchParamsKeys } from "src/constants/searchParams";
import { useTimezone } from "src/context/timezone";
import { DATE_FORMAT } from "src/utils/datetimeUtils";

import { CalendarNextLegend } from "./CalendarNextLegend";
import { CalendarToolbar } from "./CalendarToolbar";
import { DayView } from "./DayView";
import { MonthView } from "./MonthView";
import { WeekView } from "./WeekView";
import { YearView } from "./YearView";
import {
  MAX_HOURLY_DAYS,
  bucketRuns,
  filterRunsByState,
  getDaysInPeriod,
  getMaxStateCount,
  getPeriod,
  getTotalRuns,
  isCalendarDateField,
  isCalendarStateFilter,
  isCalendarView,
  shiftDate,
  shiftPeriod,
} from "./calendarNextUtils";
import { SECTION_GAP } from "./layout";
import { buildSampleRuns } from "./sampleData";
import type { BarLabels, CalendarPeriod, CalendarView } from "./types";

const parseDate = (value: string | null, timezone: string): Dayjs | undefined =>
  value !== null && dayjs(value).isValid() ? dayjs(value).tz(timezone, true) : undefined;

export const CalendarNext = () => {
  const { dagId = "" } = useParams();
  const { i18n, t: translate } = useTranslation("common");
  const [searchParams, setSearchParams] = useSearchParams();
  const { selectedTimezone } = useTimezone();

  const viewParam = searchParams.get(SearchParamsKeys.CALENDAR_VIEW);
  const view: CalendarView = isCalendarView(viewParam) ? viewParam : "month";

  const dateFieldParam = searchParams.get(SearchParamsKeys.CALENDAR_DATE_FIELD);
  const dateField = isCalendarDateField(dateFieldParam) ? dateFieldParam : "start_date";

  const stateParam = searchParams.get(SearchParamsKeys.CALENDAR_STATE);
  const stateFilter = isCalendarStateFilter(stateParam) ? stateParam : "all";

  const selectedDate =
    parseDate(searchParams.get(SearchParamsKeys.CALENDAR_DATE), selectedTimezone) ??
    dayjs().tz(selectedTimezone);

  const rangeStart = parseDate(searchParams.get(SearchParamsKeys.CALENDAR_RANGE_START), selectedTimezone);
  const rangeEnd = parseDate(searchParams.get(SearchParamsKeys.CALENDAR_RANGE_END), selectedTimezone);
  const range =
    rangeStart && rangeEnd && !rangeEnd.isBefore(rangeStart)
      ? { end: rangeEnd.endOf("day"), start: rangeStart.startOf("day") }
      : undefined;

  const period = getPeriod(view, selectedDate, range);
  const days = getDaysInPeriod(period);
  const unit = days.length <= MAX_HOURLY_DAYS ? "hour" : "day";

  const runs = filterRunsByState(buildSampleRuns(period, unit), stateFilter);
  const buckets = bucketRuns(runs, selectedTimezone, unit);
  const maxCount = getMaxStateCount(buckets);
  const totalRuns = getTotalRuns(buckets);

  const labels: BarLabels = {
    language: i18n.language,
    states: {
      failed: translate("states.failed"),
      planned: translate("states.planned"),
      running: translate("states.running"),
      success: translate("states.success"),
    },
  };

  const updateParams = (updates: Record<string, string>) => {
    setSearchParams((previous) => {
      const next = new URLSearchParams(previous);

      Object.entries(updates).forEach(([key, value]) => next.set(key, value));

      return next;
    });
  };

  const setDate = (date: Dayjs) =>
    updateParams({ [SearchParamsKeys.CALENDAR_DATE]: date.format(DATE_FORMAT) });

  const setRange = (next: CalendarPeriod) =>
    updateParams({
      [SearchParamsKeys.CALENDAR_RANGE_END]: next.end.format(DATE_FORMAT),
      [SearchParamsKeys.CALENDAR_RANGE_START]: next.start.format(DATE_FORMAT),
    });

  const navigate = (direction: -1 | 1) => {
    if (view === "range") {
      setRange(shiftPeriod(period, direction));
    } else {
      setDate(shiftDate(view, selectedDate, direction));
    }
  };

  const openDay = (date: Dayjs) =>
    updateParams({
      [SearchParamsKeys.CALENDAR_DATE]: date.format(DATE_FORMAT),
      [SearchParamsKeys.CALENDAR_VIEW]: "day",
    });

  const renderView = () => {
    switch (view) {
      case "day":
        return (
          <DayView buckets={buckets} dagId={dagId} date={selectedDate} labels={labels} maxCount={maxCount} />
        );
      case "month":
        return (
          <MonthView
            buckets={buckets}
            dagId={dagId}
            days={days}
            labels={labels}
            maxCount={maxCount}
            month={selectedDate}
            onSelectDay={openDay}
          />
        );
      case "range":
        return unit === "hour" ? (
          <WeekView
            buckets={buckets}
            dagId={dagId}
            days={days}
            labels={labels}
            maxCount={maxCount}
            showDates
          />
        ) : (
          <MonthView
            buckets={buckets}
            dagId={dagId}
            days={days}
            labels={labels}
            maxCount={maxCount}
            onSelectDay={openDay}
          />
        );
      case "week":
        return <WeekView buckets={buckets} dagId={dagId} days={days} labels={labels} maxCount={maxCount} />;
      case "year":
        return (
          <YearView
            buckets={buckets}
            dagId={dagId}
            labels={labels}
            maxCount={maxCount}
            onSelectDay={openDay}
            year={selectedDate}
          />
        );
      default:
        return undefined;
    }
  };

  return (
    <VStack align="stretch" data-testid="dag-calendar-next-root" gap={SECTION_GAP} px="2px" py="21px">
      <CalendarToolbar
        date={selectedDate}
        dateField={dateField}
        onDateChange={setDate}
        onDateFieldChange={(field) => updateParams({ [SearchParamsKeys.CALENDAR_DATE_FIELD]: field })}
        onNavigate={navigate}
        onRangeChange={setRange}
        onStateFilterChange={(filter) => updateParams({ [SearchParamsKeys.CALENDAR_STATE]: filter })}
        onViewChange={(next) => updateParams({ [SearchParamsKeys.CALENDAR_VIEW]: next })}
        period={period}
        stateFilter={stateFilter}
        totalRuns={totalRuns}
        view={view}
      />
      <Box>{renderView()}</Box>
      <CalendarNextLegend />
    </VStack>
  );
};
