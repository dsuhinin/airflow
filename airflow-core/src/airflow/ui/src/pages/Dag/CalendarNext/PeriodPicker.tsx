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
import { useState } from "react";

import { Button, Grid, HStack, Text, VStack } from "@chakra-ui/react";
import type { Dayjs } from "dayjs";
import { useTranslation } from "react-i18next";
import { MdChevronLeft, MdChevronRight } from "react-icons/md";

import { DateRangeCalendar } from "src/components/FilterBar/filters/DateRangeCalendar";

import { MONTHS } from "./calendarNextUtils";
import type { CalendarPeriod, CalendarView } from "./types";

type PickerItem = {
  readonly isSelected: boolean;
  readonly key: number;
  readonly label: string;
  readonly onSelect: () => void;
};

type PickerGridProps = {
  readonly items: Array<PickerItem>;
  readonly nextLabel: string;
  readonly onNext: () => void;
  readonly onPrevious: () => void;
  readonly previousLabel: string;
  readonly title: string;
};

const PickerGrid = ({ items, nextLabel, onNext, onPrevious, previousLabel, title }: PickerGridProps) => (
  <VStack gap={2} w="full">
    <HStack justify="space-between" w="full">
      <Button aria-label={previousLabel} onClick={onPrevious} size="xs" variant="ghost">
        <MdChevronLeft />
      </Button>
      <Text fontSize="sm" fontWeight="medium">
        {title}
      </Text>
      <Button aria-label={nextLabel} onClick={onNext} size="xs" variant="ghost">
        <MdChevronRight />
      </Button>
    </HStack>
    <Grid gap={1} templateColumns="repeat(3, 1fr)" w="full">
      {items.map(({ isSelected, key, label, onSelect }) => (
        <Button
          fontWeight={isSelected ? "bold" : "normal"}
          key={key}
          onClick={onSelect}
          size="sm"
          variant={isSelected ? "subtle" : "ghost"}
        >
          {label}
        </Button>
      ))}
    </Grid>
  </VStack>
);

type Props = {
  readonly date: Dayjs;
  readonly onChange: (date: Dayjs) => void;
  readonly period: CalendarPeriod;
  readonly view: Exclude<CalendarView, "range">;
};

const YEARS_PER_PAGE = 12;

export const PeriodPicker = ({ date, onChange, period, view }: Props) => {
  const { t: translate } = useTranslation("dag");
  const [cursor, setCursor] = useState(date);

  if (view === "day" || view === "week") {
    return (
      <DateRangeCalendar
        currentMonth={cursor}
        onDateClick={onChange}
        onMonthChange={setCursor}
        value={{
          endDate: view === "week" ? period.end.toISOString() : undefined,
          startDate: period.start.toISOString(),
        }}
      />
    );
  }

  if (view === "month") {
    return (
      <PickerGrid
        items={MONTHS.map((month) => {
          const candidate = cursor.month(month).startOf("month");

          return {
            isSelected: candidate.isSame(date, "month"),
            key: month,
            label: candidate.format("MMM"),
            onSelect: () => onChange(candidate),
          };
        })}
        nextLabel={translate("calendar.navigation.nextYear")}
        onNext={() => setCursor(cursor.add(1, "year"))}
        onPrevious={() => setCursor(cursor.subtract(1, "year"))}
        previousLabel={translate("calendar.navigation.previousYear")}
        title={cursor.format("YYYY")}
      />
    );
  }

  const firstYear = cursor.year() - YEARS_PER_PAGE + 1;

  return (
    <PickerGrid
      items={Array.from({ length: YEARS_PER_PAGE }, (_, index) => firstYear + index).map((year) => ({
        isSelected: year === date.year(),
        key: year,
        label: String(year),
        onSelect: () => onChange(date.year(year).startOf("year")),
      }))}
      nextLabel={translate("calendar.navigation.nextYears")}
      onNext={() => setCursor(cursor.add(YEARS_PER_PAGE, "year"))}
      onPrevious={() => setCursor(cursor.subtract(YEARS_PER_PAGE, "year"))}
      previousLabel={translate("calendar.navigation.previousYears")}
      title={`${firstYear} – ${cursor.year()}`}
    />
  );
};
