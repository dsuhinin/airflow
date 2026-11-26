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
import { Box, Button, Flex, Text } from "@chakra-ui/react";
import type { Dayjs } from "dayjs";

import { StateBars } from "./StateBars";
import {
  CAPTION_FONT_SIZE,
  CAPTION_LINE_HEIGHT,
  CELL_RADIUS,
  MONTH_CELL_HEIGHT,
  MONTH_CELL_PADDING,
} from "./layout";
import type { BarLabels, TimeBucket } from "./types";

type Props = {
  readonly bucket?: TimeBucket;
  readonly dagId: string;
  readonly date: Dayjs;
  readonly dateKey: string;
  /** Reserved for the event caption from the design; nothing feeds it yet. */
  readonly eventLabel?: string;
  readonly isCurrentMonth: boolean;
  readonly isToday: boolean;
  readonly labels: BarLabels;
  readonly maxCount: number;
  readonly onSelectDay: (date: Dayjs) => void;
};

export const DayCell = ({
  bucket,
  dagId,
  date,
  dateKey,
  eventLabel,
  isCurrentMonth,
  isToday,
  labels,
  maxCount,
  onSelectDay,
}: Props) => (
  <Flex
    bg="bg.muted"
    borderRadius={CELL_RADIUS}
    data-testid="calendar-day-cell"
    direction="column"
    minHeight={MONTH_CELL_HEIGHT}
    minWidth={0}
    p={MONTH_CELL_PADDING}
  >
    <Button
      alignSelf="center"
      aria-label={dateKey}
      color={isToday ? "fg" : isCurrentMonth ? "fg.muted" : "fg.subtle"}
      fontSize={CAPTION_FONT_SIZE}
      fontWeight={isToday ? "bold" : "normal"}
      height={CAPTION_LINE_HEIGHT}
      minWidth={0}
      mt="-2px"
      onClick={() => onSelectDay(date)}
      px={1}
      variant="plain"
    >
      {date.format("DD")}
    </Button>
    {eventLabel === undefined ? undefined : (
      <Text color="fg.muted" fontSize={CAPTION_FONT_SIZE} mt="2px" truncate>
        {eventLabel}
      </Text>
    )}
    <Box flex="1" />
    {bucket ? (
      <StateBars bucket={bucket} dagId={dagId} labels={labels} maxCount={maxCount} variant="labelled" />
    ) : undefined}
  </Flex>
);
