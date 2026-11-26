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

import { Button, Flex, HStack, SegmentGroup, Separator, Text } from "@chakra-ui/react";
import { createListCollection, type ListCollection } from "@chakra-ui/react/collection";
import type { Dayjs } from "dayjs";
import { useTranslation } from "react-i18next";
import { FiChevronLeft, FiChevronRight } from "react-icons/fi";

import { IconButton, Popover, Select } from "src/system-components";

import { formatCompactNumber } from "src/utils/formatNumber";

import { CustomRangePicker } from "./CustomRangePicker";
import { PeriodPicker } from "./PeriodPicker";
import {
  CALENDAR_DATE_FIELDS,
  CALENDAR_VIEWS,
  formatPeriodTitle,
  isCalendarStateFilter,
} from "./calendarNextUtils";
import { CAPTION_FONT_SIZE, CELL_RADIUS, CONTROL_HEIGHT } from "./layout";
import type { CalendarDateField, CalendarPeriod, CalendarStateFilter, CalendarView } from "./types";

type ToolbarSelectProps<T extends string> = {
  readonly collection: ListCollection<{ label: string; value: T }>;
  readonly onChange: (value: T) => void;
  readonly placeholder: string;
  readonly rounded?: boolean;
  readonly testId: string;
  readonly value: T;
  readonly width: string;
};

const ToolbarSelect = <T extends string>({
  collection,
  onChange,
  placeholder,
  rounded = false,
  testId,
  value,
  width,
}: ToolbarSelectProps<T>) => (
  <Select.Root
    collection={collection}
    data-testid={testId}
    onValueChange={({ value: [next] }) => {
      const option = collection.items.find((item) => item.value === next);

      if (option) {
        onChange(option.value);
      }
    }}
    size="sm"
    value={[value]}
    width={width}
  >
    <Select.Trigger
      triggerProps={
        rounded
          ? {
              bg: "bg.emphasized",
              border: "none",
              borderRadius: "full",
              cursor: "pointer",
              height: CONTROL_HEIGHT,
            }
          : { cursor: "pointer", height: CONTROL_HEIGHT }
      }
    >
      <Select.ValueText placeholder={placeholder} />
    </Select.Trigger>
    <Select.Content>
      {collection.items.map((option) => (
        <Select.Item item={option} key={option.value}>
          {option.label}
        </Select.Item>
      ))}
    </Select.Content>
  </Select.Root>
);

type Props = {
  readonly date: Dayjs;
  readonly dateField: CalendarDateField;
  readonly onDateChange: (date: Dayjs) => void;
  readonly onDateFieldChange: (field: CalendarDateField) => void;
  readonly onNavigate: (direction: -1 | 1) => void;
  readonly onRangeChange: (period: CalendarPeriod) => void;
  readonly onStateFilterChange: (filter: CalendarStateFilter) => void;
  readonly onViewChange: (view: CalendarView) => void;
  readonly period: CalendarPeriod;
  readonly stateFilter: CalendarStateFilter;
  readonly totalRuns: number;
  readonly view: CalendarView;
};

const VIEW_LABEL_KEYS: Record<CalendarView, string> = {
  day: "dag:calendar.views.day",
  month: "dag:calendar.views.month",
  range: "dag:calendar.views.customRange",
  week: "dag:calendar.views.week",
  year: "dag:calendar.views.year",
};

const DATE_FIELD_LABEL_KEYS: Record<CalendarDateField, string> = {
  end_date: "common:endDate",
  logical_date: "common:logicalDate",
  run_after: "common:dagRun.runAfter",
  start_date: "common:startDate",
};

export const CalendarToolbar = ({
  date,
  dateField,
  onDateChange,
  onDateFieldChange,
  onNavigate,
  onRangeChange,
  onStateFilterChange,
  onViewChange,
  period,
  stateFilter,
  totalRuns,
  view,
}: Props) => {
  const { i18n, t: translate } = useTranslation(["dag", "common"]);
  const [isPickerOpen, setIsPickerOpen] = useState(false);

  const viewOptions = createListCollection({
    items: CALENDAR_VIEWS.map((value) => ({ label: translate(VIEW_LABEL_KEYS[value]), value })),
  });

  const dateFieldOptions = createListCollection({
    items: CALENDAR_DATE_FIELDS.map((value) => ({ label: translate(DATE_FIELD_LABEL_KEYS[value]), value })),
  });

  const stateFilterItems = [
    { label: translate("dag:calendar.allStates"), value: "all" },
    { label: translate("common:states.success"), value: "success" },
    { label: translate("common:states.failed"), value: "failed" },
  ];

  const handleViewChange = (next: CalendarView) => {
    onViewChange(next);
    setIsPickerOpen(next === "range");
  };

  const handleDatePick = (next: Dayjs) => {
    onDateChange(next);
    setIsPickerOpen(false);
  };

  const handleRangePick = (next: CalendarPeriod) => {
    onRangeChange(next);
    setIsPickerOpen(false);
  };

  return (
    <Flex align="center" data-testid="calendar-toolbar" gap={3} justify="space-between" wrap="wrap">
      <HStack gap="12px" wrap="wrap">
        <ToolbarSelect
          collection={viewOptions}
          onChange={handleViewChange}
          placeholder={translate("dag:calendar.view")}
          rounded
          testId="calendar-view-select"
          value={view}
          width="117px"
        />

        <Separator height={CONTROL_HEIGHT} orientation="vertical" />

        <HStack gap={1}>
          <IconButton
            aria-label={translate("dag:calendar.navigation.previous")}
            onClick={() => onNavigate(-1)}
            size="sm"
          >
            <FiChevronLeft />
          </IconButton>
          <Popover.Root
            lazyMount
            onOpenChange={({ open }) => setIsPickerOpen(open)}
            open={isPickerOpen}
            positioning={{ placement: "bottom" }}
            unmountOnExit
          >
            <Popover.Trigger asChild>
              <Button
                aria-label={translate("dag:calendar.navigation.selectPeriod")}
                data-testid="calendar-current-period"
                fontSize="md"
                fontWeight="normal"
                height={CONTROL_HEIGHT}
                minWidth="160px"
                variant="ghost"
              >
                {formatPeriodTitle(view, date, period)}
              </Button>
            </Popover.Trigger>
            <Popover.Content p={3} width="320px">
              {view === "range" ? (
                <CustomRangePicker onChange={handleRangePick} period={period} />
              ) : (
                <PeriodPicker date={date} onChange={handleDatePick} period={period} view={view} />
              )}
            </Popover.Content>
          </Popover.Root>
          <IconButton
            aria-label={translate("dag:calendar.navigation.next")}
            onClick={() => onNavigate(1)}
            size="sm"
          >
            <FiChevronRight />
          </IconButton>
        </HStack>

        <Separator height={CONTROL_HEIGHT} orientation="vertical" />

        <Text
          color="fg.muted"
          data-testid="calendar-runs-count"
          fontSize={CAPTION_FONT_SIZE}
          whiteSpace="nowrap"
        >
          {translate("dag:calendar.runs", {
            count: totalRuns,
            value: formatCompactNumber(totalRuns, i18n.language),
          })}
        </Text>
      </HStack>

      <HStack gap="12px" wrap="wrap">
        <ToolbarSelect
          collection={dateFieldOptions}
          onChange={onDateFieldChange}
          placeholder={translate("dag:calendar.dateField")}
          testId="calendar-date-field-select"
          value={dateField}
          width="140px"
        />

        <SegmentGroup.Root
          bg="bg.emphasized"
          borderRadius={CELL_RADIUS}
          data-testid="calendar-state-filter"
          height={CONTROL_HEIGHT}
          onValueChange={({ value }) => {
            if (isCalendarStateFilter(value)) {
              onStateFilterChange(value);
            }
          }}
          p="5px"
          size="sm"
          value={stateFilter}
        >
          <SegmentGroup.Indicator bg="bg.panel" borderRadius={CELL_RADIUS} />
          <SegmentGroup.Items
            cursor="pointer"
            fontSize="sm"
            height="25px"
            items={stateFilterItems}
            px="10px"
            py={0}
          />
        </SegmentGroup.Root>
      </HStack>
    </Flex>
  );
};
