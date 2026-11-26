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
import { lazy, Suspense } from "react";

import { Skeleton } from "@chakra-ui/react";
import { useSearchParams } from "react-router-dom";

import { SearchParamsKeys } from "src/constants/searchParams";

import { Calendar } from "./Calendar/Calendar";

const NEXT_CALENDAR_LAYOUT = "next";

const CalendarNext = lazy(() =>
  import("./CalendarNext").then((module) => ({ default: module.CalendarNext })),
);

// The redesigned calendar stays behind `?calendar-layout=next` until it replaces the current one.
export const CalendarTab = () => {
  const [searchParams] = useSearchParams();

  if (searchParams.get(SearchParamsKeys.CALENDAR_LAYOUT) !== NEXT_CALENDAR_LAYOUT) {
    return <Calendar />;
  }

  return (
    <Suspense fallback={<Skeleton height="400px" m={4} />}>
      <CalendarNext />
    </Suspense>
  );
};
