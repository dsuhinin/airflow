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
import { Box, Flex, HStack, Text } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";

import { RunTypeIcon } from "src/components/RunTypeIcon";

import { LegendIcon } from "../Calendar/LegendIcon";
import { TOTAL_COLOR_INTENSITIES } from "../Calendar/calendarUtils";
import { BAR_STATE_ORDER, getBarColors } from "./calendarNextUtils";

const LEGEND_LEVEL = 2;

export const CalendarNextLegend = () => {
  const { t: translate } = useTranslation(["dag", "common"]);

  return (
    <Flex align="center" data-testid="calendar-legend" gap={4} justify="space-between" wrap="wrap">
      <HStack gap={4} wrap="wrap">
        {BAR_STATE_ORDER.map((state) => (
          <HStack gap={2} key={state}>
            <LegendIcon color={getBarColors(state, LEGEND_LEVEL).bg} />
            <Text color="fg.muted" fontSize="xs">
              {translate(`common:states.${state}`)}
            </Text>
          </HStack>
        ))}
        <HStack gap={2}>
          <LegendIcon
            color={{
              primary: getBarColors("success", LEGEND_LEVEL).bg,
              secondary: getBarColors("planned", LEGEND_LEVEL).bg,
            }}
          />
          <Text color="fg.muted" fontSize="xs">
            {translate("dag:calendar.legend.mixed")}
          </Text>
        </HStack>
        <HStack gap={2}>
          <Box fontSize="14px" lineHeight={1}>
            <RunTypeIcon runType="backfill" />
          </Box>
          <Text color="fg.muted" fontSize="xs">
            {translate("dag:calendar.backfill")}
          </Text>
        </HStack>
      </HStack>
      <HStack gap={2}>
        <Text color="fg.muted" fontSize="xs">
          {translate("dag:calendar.legend.less")}
        </Text>
        <HStack gap={0.5}>
          {TOTAL_COLOR_INTENSITIES.slice(1).map((color) => (
            <LegendIcon color={color} key={color._light} />
          ))}
        </HStack>
        <Text color="fg.muted" fontSize="xs">
          {translate("dag:calendar.legend.more")}
        </Text>
      </HStack>
    </Flex>
  );
};
