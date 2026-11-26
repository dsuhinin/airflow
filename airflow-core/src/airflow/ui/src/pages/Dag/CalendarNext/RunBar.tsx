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
import { Box, Text } from "@chakra-ui/react";

import { RouterLink } from "src/system-components";

import { RunTypeIcon } from "src/components/RunTypeIcon";

import { BAR_PADDING_X, CAPTION_FONT_SIZE, CELL_RADIUS } from "./layout";
import type { BarColors, BarState } from "./types";

type Props = {
  readonly colors: BarColors;
  readonly hasBackfill?: boolean;
  readonly height: string;
  readonly label?: string;
  readonly state: BarState;
  readonly stripe?: boolean;
  readonly to: string;
};

export const RunBar = ({ colors, hasBackfill = false, height, label, state, stripe = false, to }: Props) => (
  <RouterLink
    _hover={{ opacity: 0.85, textDecoration: "none" }}
    alignItems="center"
    bg={colors.bg}
    borderRadius={stripe ? "2px" : CELL_RADIUS}
    color={colors.fg}
    data-state={state}
    data-testid="calendar-run-bar"
    display="flex"
    gap={1}
    height={height}
    justifyContent={label === undefined ? "center" : "space-between"}
    px={stripe ? 0 : BAR_PADDING_X}
    to={to}
    width="100%"
  >
    {label === undefined ? undefined : (
      <Text fontSize={CAPTION_FONT_SIZE} lineHeight="1" truncate>
        {label}
      </Text>
    )}
    {hasBackfill && !stripe ? (
      <Box data-testid="calendar-backfill-indicator" flexShrink={0} fontSize="14px" lineHeight={1}>
        <RunTypeIcon runType="backfill" />
      </Box>
    ) : undefined}
  </RouterLink>
);
