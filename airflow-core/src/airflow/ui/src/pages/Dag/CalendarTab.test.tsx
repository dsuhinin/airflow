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
import "@testing-library/jest-dom/vitest";
import { render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { CALENDAR_LAYOUT_KEY } from "src/constants/localStorage";
import { BaseWrapper } from "src/utils/Wrapper";

import { CalendarTab } from "./CalendarTab";

vi.mock("./Calendar/Calendar", () => ({ Calendar: () => <div data-testid="current-calendar" /> }));
vi.mock("./CalendarNext", () => ({ CalendarNext: () => <div data-testid="next-calendar" /> }));

afterEach(() => {
  localStorage.clear();
});

describe("CalendarTab", () => {
  it("renders the redesigned calendar by default", async () => {
    render(<CalendarTab />, { wrapper: BaseWrapper });

    expect(await screen.findByTestId("next-calendar")).toBeInTheDocument();
    expect(screen.queryByTestId("current-calendar")).not.toBeInTheDocument();
  });

  it("renders the current calendar when the setting picks it", () => {
    localStorage.setItem(CALENDAR_LAYOUT_KEY, JSON.stringify("current"));

    render(<CalendarTab />, { wrapper: BaseWrapper });

    expect(screen.getByTestId("current-calendar")).toBeInTheDocument();
    expect(screen.queryByTestId("next-calendar")).not.toBeInTheDocument();
  });

  it("falls back to the redesigned calendar for unknown stored values", async () => {
    localStorage.setItem(CALENDAR_LAYOUT_KEY, JSON.stringify("other"));

    render(<CalendarTab />, { wrapper: BaseWrapper });

    expect(await screen.findByTestId("next-calendar")).toBeInTheDocument();
  });
});
