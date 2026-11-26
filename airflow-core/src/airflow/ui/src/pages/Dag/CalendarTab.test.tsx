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
import { MemoryRouter } from "react-router-dom";
import { describe, expect, it, vi } from "vitest";

import { BaseWrapper } from "src/utils/Wrapper";

import { CalendarTab } from "./CalendarTab";

vi.mock("./Calendar/Calendar", () => ({ Calendar: () => <div data-testid="current-calendar" /> }));
vi.mock("./CalendarNext", () => ({ CalendarNext: () => <div data-testid="next-calendar" /> }));

const renderTab = (search: string) =>
  render(
    <BaseWrapper>
      <MemoryRouter initialEntries={[`/dags/example_dag/calendar${search}`]}>
        <CalendarTab />
      </MemoryRouter>
    </BaseWrapper>,
  );

describe("CalendarTab", () => {
  it("renders the current calendar by default", () => {
    renderTab("");

    expect(screen.getByTestId("current-calendar")).toBeInTheDocument();
    expect(screen.queryByTestId("next-calendar")).not.toBeInTheDocument();
  });

  it("renders the redesigned calendar behind the layout parameter", async () => {
    renderTab("?calendar-layout=next");

    expect(await screen.findByTestId("next-calendar")).toBeInTheDocument();
    expect(screen.queryByTestId("current-calendar")).not.toBeInTheDocument();
  });

  it("ignores other layout values", () => {
    renderTab("?calendar-layout=other");

    expect(screen.getByTestId("current-calendar")).toBeInTheDocument();
  });
});
