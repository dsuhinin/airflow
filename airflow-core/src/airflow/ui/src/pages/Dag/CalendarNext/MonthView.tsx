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
import { Grid } from "@chakra-ui/react";
import dayjs, { type Dayjs } from "dayjs";

import { ColumnHeader } from "./ColumnHeader";
import { DayCell } from "./DayCell";
import { getBucketKey } from "./calendarNextUtils";
import { COLUMN_GAP, ROW_GAP } from "./layout";
import type { BarLabels, TimeBucket } from "./types";

type Props = {
  readonly buckets: Map<string, TimeBucket>;
  readonly dagId: string;
  readonly days: Array<Dayjs>;
  readonly labels: BarLabels;
  readonly maxCount: number;
  readonly month?: Dayjs;
  readonly onSelectDay: (date: Dayjs) => void;
};

export const MonthView = ({ buckets, dagId, days, labels, maxCount, month, onSelectDay }: Props) => {
  const today = dayjs();

  return (
    <Grid
      columnGap={COLUMN_GAP}
      data-testid="calendar-month-grid"
      rowGap={ROW_GAP}
      templateColumns="repeat(7, minmax(0, 1fr))"
    >
      {days.slice(0, 7).map((day) => (
        <ColumnHeader key={day.day()}>{day.format("dddd")}</ColumnHeader>
      ))}
      {days.map((day) => {
        const dateKey = getBucketKey(day, "day");

        return (
          <DayCell
            bucket={buckets.get(dateKey)}
            dagId={dagId}
            date={day}
            dateKey={dateKey}
            isCurrentMonth={month === undefined || day.isSame(month, "month")}
            isToday={day.isSame(today, "day")}
            key={dateKey}
            labels={labels}
            maxCount={maxCount}
            onSelectDay={onSelectDay}
          />
        );
      })}
    </Grid>
  );
};
