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
import { Box, HStack, Text } from "@chakra-ui/react";
import type { Dayjs } from "dayjs";

import { formatCompactNumber } from "src/utils/formatNumber";

import { StateBars } from "./StateBars";
import { HOURS, getBucketKey, getHourKey } from "./calendarNextUtils";
import { CAPTION_FONT_SIZE, CELL_RADIUS, HOUR_LABEL_FONT_SIZE, HOUR_LAYOUT } from "./layout";
import type { BarLabels, TimeBucket } from "./types";

type Props = {
  readonly buckets: Map<string, TimeBucket>;
  readonly dagId: string;
  readonly day: Dayjs;
  readonly labels: BarLabels;
  readonly maxCount: number;
  readonly wide?: boolean;
};

export const HourColumn = ({ buckets, dagId, day, labels, maxCount, wide = false }: Props) => {
  const dayKey = getBucketKey(day, "day");
  const layout = wide ? HOUR_LAYOUT.day : HOUR_LAYOUT.week;

  return (
    <Box
      bg="bg.muted"
      borderRadius={CELL_RADIUS}
      data-testid="calendar-hour-column"
      minWidth={0}
      pb={layout.bottom}
      pt={layout.top}
    >
      {HOURS.map((hour) => {
        const bucket = buckets.get(getHourKey(dayKey, hour));

        return (
          <Box height={layout.rowHeight} key={hour} position="relative">
            <Text
              color="fg.subtle"
              fontSize={HOUR_LABEL_FONT_SIZE}
              left={layout.labelLeft}
              lineHeight="11px"
              position="absolute"
              top={0}
            >
              {hour}:00
            </Text>
            {hour === "00" ? undefined : (
              <Box
                borderColor="border.emphasized"
                borderTopStyle="dashed"
                borderTopWidth="1px"
                left={layout.lineLeft}
                position="absolute"
                right={layout.right}
                top="6px"
              />
            )}
            {bucket ? (
              <Box left={layout.barsLeft} position="absolute" right={layout.right} top={layout.barsTop}>
                {wide ? (
                  <StateBars
                    barHeight={layout.barHeight}
                    bucket={bucket}
                    dagId={dagId}
                    horizontal
                    labels={labels}
                    maxCount={maxCount}
                    variant="labelled"
                  />
                ) : (
                  <HStack align="flex-start" gap="4px">
                    <Text
                      fontSize={CAPTION_FONT_SIZE}
                      lineHeight={layout.barHeight}
                      minWidth={HOUR_LAYOUT.week.countWidth}
                      textAlign="end"
                    >
                      {formatCompactNumber(bucket.total, labels.language)}
                    </Text>
                    <Box flex="1" minWidth={0}>
                      <StateBars
                        barHeight={layout.barHeight}
                        bucket={bucket}
                        dagId={dagId}
                        labels={labels}
                        maxCount={maxCount}
                        variant="compact"
                      />
                    </Box>
                  </HStack>
                )}
              </Box>
            ) : undefined}
          </Box>
        );
      })}
    </Box>
  );
};
