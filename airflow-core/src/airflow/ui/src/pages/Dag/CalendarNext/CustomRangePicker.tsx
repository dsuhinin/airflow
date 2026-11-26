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
import { VStack } from "@chakra-ui/react";
import dayjs from "dayjs";
import { useTranslation } from "react-i18next";

import { DateRangeCalendar } from "src/components/FilterBar/filters/DateRangeCalendar";
import { DateRangeInputs } from "src/components/FilterBar/filters/DateRangeInputs";
import type { DateRangeValue } from "src/components/FilterBar/types";
import { isValidDateValue } from "src/components/FilterBar/utils";

import { useDateRangeFilter } from "src/hooks/useDateRangeFilter";

import type { CalendarPeriod } from "./types";

type Props = {
  readonly onChange: (period: CalendarPeriod) => void;
  readonly period: CalendarPeriod;
};

export const CustomRangePicker = ({ onChange, period }: Props) => {
  const { t: translate } = useTranslation(["common", "components"]);

  const value: DateRangeValue = {
    endDate: period.end.toISOString(),
    startDate: period.start.toISOString(),
  };

  const handleChange = (next: DateRangeValue) => {
    if (isValidDateValue(next.startDate) && isValidDateValue(next.endDate)) {
      const start = dayjs(next.startDate);
      const end = dayjs(next.endDate);

      if (start.isValid() && end.isValid() && !end.isBefore(start)) {
        onChange({ end, start });
      }
    }
  };

  const {
    commitEditingState,
    editingState,
    endDateValue,
    getFieldError,
    handleDateClick,
    handleInputChange,
    setEditingState,
    startDateValue,
  } = useDateRangeFilter({ onChange: handleChange, translate, value });

  return (
    <VStack gap={2} w="full">
      <DateRangeInputs
        editingState={editingState}
        endDateValue={endDateValue}
        getFieldError={getFieldError}
        handleInputChange={handleInputChange}
        onChange={handleChange}
        onCommit={commitEditingState}
        setEditingState={setEditingState}
        startDateValue={startDateValue}
        translate={translate}
        value={value}
      />
      <DateRangeCalendar
        currentMonth={editingState.currentMonth}
        onDateClick={handleDateClick}
        onMonthChange={(month) => setEditingState((prev) => ({ ...prev, currentMonth: month }))}
        value={value}
      />
    </VStack>
  );
};
