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
import { Box, Grid } from "@chakra-ui/react";
import type { Dayjs } from "dayjs";

import { ColumnHeader } from "./ColumnHeader";
import { HourColumn } from "./HourColumn";
import { getBucketKey } from "./calendarNextUtils";
import { COLUMN_GAP, ROW_GAP } from "./layout";
import type { BarLabels, TimeBucket } from "./types";

type Props = {
  readonly buckets: Map<string, TimeBucket>;
  readonly dagId: string;
  readonly days: Array<Dayjs>;
  readonly labels: BarLabels;
  readonly maxCount: number;
  readonly showDates?: boolean;
};

const MIN_COLUMN_WIDTH = 150;

export const WeekView = ({ buckets, dagId, days, labels, maxCount, showDates = false }: Props) => (
  <Box overflowX="auto">
    <Grid
      columnGap={COLUMN_GAP}
      data-testid="calendar-week-grid"
      minWidth={`${days.length * MIN_COLUMN_WIDTH}px`}
      rowGap={ROW_GAP}
      templateColumns={`repeat(${days.length}, minmax(0, 1fr))`}
    >
      {days.map((day) => (
        <ColumnHeader key={getBucketKey(day, "day")}>
          {showDates ? day.format("ddd D") : day.format("dddd")}
        </ColumnHeader>
      ))}
      {days.map((day) => (
        <HourColumn
          buckets={buckets}
          dagId={dagId}
          day={day}
          key={getBucketKey(day, "day")}
          labels={labels}
          maxCount={maxCount}
        />
      ))}
    </Grid>
  </Box>
);
