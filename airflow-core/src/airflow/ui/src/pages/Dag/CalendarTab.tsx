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

import { useCalendarLayout } from "src/hooks/useUserSettings";

import { Calendar } from "./Calendar/Calendar";

const CalendarNext = lazy(() =>
  import("./CalendarNext").then((module) => ({ default: module.CalendarNext })),
);

// The previous calendar stays reachable through the Settings page switch until the redesign replaces it.
export const CalendarTab = () => {
  const [layout] = useCalendarLayout();

  if (layout === "current") {
    return <Calendar />;
  }

  return (
    <Suspense fallback={<Skeleton height="400px" m={4} />}>
      <CalendarNext />
    </Suspense>
  );
};
