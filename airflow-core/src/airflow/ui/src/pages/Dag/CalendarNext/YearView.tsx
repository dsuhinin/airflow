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
import { Box, Button, Grid, Text, VStack } from "@chakra-ui/react";
import type { Dayjs } from "dayjs";

import { formatCompactNumber } from "src/utils/formatNumber";

import { ColumnHeader } from "./ColumnHeader";
import { StateBars } from "./StateBars";
import { MONTHS, getBucketKey } from "./calendarNextUtils";
import { CAPTION_FONT_SIZE, CELL_RADIUS, HOUR_LABEL_FONT_SIZE, YEAR_LAYOUT } from "./layout";
import type { BarLabels, TimeBucket } from "./types";

type Props = {
  readonly buckets: Map<string, TimeBucket>;
  readonly dagId: string;
  readonly labels: BarLabels;
  readonly maxCount: number;
  readonly onSelectDay: (date: Dayjs) => void;
  readonly year: Dayjs;
};

const DAYS = Array.from({ length: 31 }, (_, index) => index + 1);

export const YearView = ({ buckets, dagId, labels, maxCount, onSelectDay, year }: Props) => (
  <Grid
    data-testid="calendar-year-grid"
    gap={YEAR_LAYOUT.columnGap}
    templateColumns={`repeat(auto-fit, minmax(${YEAR_LAYOUT.columnMinWidth}, 1fr))`}
  >
    {MONTHS.map((month) => {
      const monthStart = year.month(month).startOf("month");
      const daysInMonth = monthStart.daysInMonth();

      return (
        <VStack align="stretch" gap={YEAR_LAYOUT.rowGap} key={month} minWidth={0}>
          <ColumnHeader gap={YEAR_LAYOUT.headerGap}>{monthStart.format("MMM")}</ColumnHeader>
          {DAYS.map((dayNumber) => {
            if (dayNumber > daysInMonth) {
              return <Box height={YEAR_LAYOUT.rowHeight} key={dayNumber} />;
            }

            const day = monthStart.date(dayNumber);
            const dateKey = getBucketKey(day, "day");
            const bucket = buckets.get(dateKey);

            return (
              <Box
                bg="bg.muted"
                borderRadius={CELL_RADIUS}
                data-testid="calendar-year-row"
                height={YEAR_LAYOUT.rowHeight}
                key={dayNumber}
                position="relative"
              >
                <Button
                  aria-label={dateKey}
                  color="fg.subtle"
                  fontSize={HOUR_LABEL_FONT_SIZE}
                  fontWeight="normal"
                  height={YEAR_LAYOUT.rowHeight}
                  left={YEAR_LAYOUT.dayLabelLeft}
                  minWidth={0}
                  onClick={() => onSelectDay(day)}
                  position="absolute"
                  px={0}
                  top={0}
                  variant="plain"
                >
                  {day.format("DD")}
                </Button>
                {bucket ? (
                  <>
                    <Box
                      left={YEAR_LAYOUT.barLeft}
                      position="absolute"
                      top={YEAR_LAYOUT.barTop}
                      width={YEAR_LAYOUT.barWidth}
                    >
                      <StateBars
                        barHeight={YEAR_LAYOUT.barHeight}
                        bucket={bucket}
                        dagId={dagId}
                        labels={labels}
                        maxCount={maxCount}
                        variant="unlabelled"
                      />
                    </Box>
                    <Text
                      fontSize={CAPTION_FONT_SIZE}
                      lineHeight={YEAR_LAYOUT.rowHeight}
                      position="absolute"
                      right={YEAR_LAYOUT.countRight}
                      top={0}
                    >
                      {formatCompactNumber(bucket.total, labels.language)}
                    </Text>
                  </>
                ) : undefined}
              </Box>
            );
          })}
        </VStack>
      );
    })}
  </Grid>
);
