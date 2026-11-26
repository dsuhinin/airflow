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
import { Grid, VStack } from "@chakra-ui/react";

import { formatCompactNumber } from "src/utils/formatNumber";

import { RunBar } from "./RunBar";
import { getBarColors, getIntensityLevel, getRunsLink } from "./calendarNextUtils";
import { BAR_GAP, BAR_HEIGHT, HOUR_LAYOUT } from "./layout";
import type { BarLabels, BarState, TimeBucket } from "./types";

type Props = {
  readonly barHeight?: string;
  readonly bucket: TimeBucket;
  readonly dagId: string;
  readonly horizontal?: boolean;
  readonly labels: BarLabels;
  readonly maxCount: number;
  /**
   * labelled: "12 Success" per state, stacked (month) or side by side (day).
   * compact: state name only, several states collapse into thin stripes (week).
   * unlabelled: color only, several states collapse into thin stripes (year).
   */
  readonly variant: "compact" | "labelled" | "unlabelled";
};

// Two stripes of 7px or three of 4px, each with a 2px gap, fill a 16px slot.
const getStripeHeight = (count: number) => (count > 2 ? "4px" : "7px");

export const StateBars = ({
  barHeight = BAR_HEIGHT,
  bucket,
  dagId,
  horizontal = false,
  labels,
  maxCount,
  variant,
}: Props) => {
  const stripe = variant !== "labelled" && bucket.states.length > 1;

  const getLabel = (count: number, state: BarState) => {
    if (variant === "labelled") {
      return `${formatCompactNumber(count, labels.language)} ${labels.states[state]}`;
    }

    return variant === "compact" && !stripe ? labels.states[state] : undefined;
  };

  const bars = bucket.states.map(({ count, hasBackfill, state }) => (
    <RunBar
      colors={getBarColors(state, getIntensityLevel(count, maxCount))}
      hasBackfill={hasBackfill}
      height={stripe ? getStripeHeight(bucket.states.length) : barHeight}
      key={state}
      label={getLabel(count, state)}
      state={state}
      stripe={stripe}
      to={getRunsLink(dagId, bucket.date, state)}
    />
  ));

  if (horizontal) {
    return (
      <Grid
        gap={HOUR_LAYOUT.day.barGap}
        templateColumns={`repeat(auto-fill, minmax(${HOUR_LAYOUT.day.barMinWidth}, 1fr))`}
        width="100%"
      >
        {bars}
      </Grid>
    );
  }

  return (
    <VStack align="stretch" gap={BAR_GAP} width="100%">
      {bars}
    </VStack>
  );
};
