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
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { MemoryRouter, Route, Routes, useLocation } from "react-router-dom";
import { describe, expect, it, vi } from "vitest";

import { TimezoneProvider } from "src/context/timezone";
import { BaseWrapper } from "src/utils/Wrapper";

import { CalendarNext } from "./CalendarNext";

// Return translation keys as-is so the tests can look elements up by key.
vi.mock("react-i18next", () => ({
  useTranslation: () => ({
    i18n: { language: "en" },
    // eslint-disable-next-line id-length
    t: (key: string) => key,
  }),
}));

const LocationDisplay = () => <span data-testid="search">{useLocation().search}</span>;

const renderCalendar = (search = "") =>
  render(
    <BaseWrapper>
      <MemoryRouter initialEntries={[`/dags/example_dag/calendar${search}`]}>
        <TimezoneProvider>
          <Routes>
            <Route
              element={
                <>
                  <CalendarNext />
                  <LocationDisplay />
                </>
              }
              path="/dags/:dagId/calendar"
            />
          </Routes>
        </TimezoneProvider>
      </MemoryRouter>
    </BaseWrapper>,
  );

const DATE = "&calendar-date=2026-09-16";

describe("CalendarNext", () => {
  it("renders the month grid by default with sample bars linking to the runs tab", () => {
    renderCalendar(`?calendar-layout=next${DATE}`);

    expect(screen.getByTestId("dag-calendar-next-root")).toBeInTheDocument();
    expect(screen.getByTestId("calendar-month-grid")).toBeInTheDocument();
    expect(screen.getAllByTestId("calendar-day-cell")).toHaveLength(35);
    expect(screen.getByTestId("calendar-current-period")).toHaveTextContent("September 2026");

    const [bar] = screen.getAllByTestId("calendar-run-bar");

    expect(bar?.closest("a")?.getAttribute("href")).toContain("/dags/example_dag/runs?");
    expect(screen.getByTestId("calendar-legend")).toBeInTheDocument();
  });

  it.each([
    ["week", "calendar-week-grid"],
    ["day", "calendar-day-timeline"],
    ["year", "calendar-year-grid"],
    ["range", "calendar-week-grid"],
  ])("renders the %s view from the URL", (view, testId) => {
    renderCalendar(`?calendar-view=${view}${DATE}`);

    expect(screen.getByTestId(testId)).toBeInTheDocument();
  });

  it("shows seven hour columns for a week", () => {
    renderCalendar(`?calendar-view=week${DATE}`);

    expect(screen.getAllByTestId("calendar-hour-column")).toHaveLength(7);
    expect(screen.getByTestId("calendar-current-period")).toHaveTextContent("Sep 13th – Sep 19th 2026");
  });

  it("moves to the next period through the URL", () => {
    renderCalendar(`?calendar-view=month${DATE}`);

    fireEvent.click(screen.getByLabelText("dag:calendar.navigation.next"));

    expect(screen.getByTestId("search")).toHaveTextContent("calendar-date=2026-10-16");
  });

  it("shifts a custom range by its own length", () => {
    renderCalendar("?calendar-view=range&calendar-range-start=2026-09-14&calendar-range-end=2026-09-16");

    fireEvent.click(screen.getByLabelText("dag:calendar.navigation.previous"));

    expect(screen.getByTestId("search")).toHaveTextContent("calendar-range-start=2026-09-11");
    expect(screen.getByTestId("search")).toHaveTextContent("calendar-range-end=2026-09-13");
  });

  it("opens the day view when a day number is clicked", () => {
    renderCalendar(`?calendar-view=month${DATE}`);

    fireEvent.click(screen.getByLabelText("2026-09-03"));

    const search = screen.getByTestId("search");

    expect(search).toHaveTextContent("calendar-view=day");
    expect(search).toHaveTextContent("calendar-date=2026-09-03");
  });

  it("writes the state filter to the URL", async () => {
    renderCalendar(`?calendar-view=month${DATE}`);

    fireEvent.click(screen.getByRole("radio", { name: "common:states.failed" }));

    await waitFor(() => expect(screen.getByTestId("search")).toHaveTextContent("calendar-state=failed"));
  });

  it("falls back to the month view for an unknown view", () => {
    renderCalendar(`?calendar-view=bogus${DATE}`);

    expect(screen.getByTestId("calendar-month-grid")).toBeInTheDocument();
  });
});
