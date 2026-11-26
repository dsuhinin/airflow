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

// Measurements taken from the Figma frames (1728px wide), kept in px so the views match them exactly.

export const CONTROL_HEIGHT = "35px";
export const SECTION_GAP = "31px";

export const CAPTION_FONT_SIZE = "11px";
export const CAPTION_LINE_HEIGHT = "13px";
export const HOUR_LABEL_FONT_SIZE = "9px";

export const CELL_RADIUS = "4px";
export const COLUMN_GAP = "11px";
export const ROW_GAP = "10px";
export const HEADER_GAP = "11px";

export const BAR_HEIGHT = "18px";
export const BAR_PADDING_X = "7px";
export const BAR_GAP = "2px";

export const MONTH_CELL_HEIGHT = "130px";
export const MONTH_CELL_PADDING = "10px";

export const HOUR_LAYOUT = {
  day: {
    barGap: "10px",
    barHeight: "18px",
    barMinWidth: "244px",
    barsLeft: "57px",
    barsTop: "12px",
    bottom: "4px",
    labelLeft: "12px",
    lineLeft: "52px",
    right: "16px",
    rowHeight: "30px",
    top: "12px",
  },
  week: {
    barHeight: "20px",
    barsLeft: "45px",
    barsTop: "9px",
    bottom: "17px",
    countWidth: "19px",
    labelLeft: "11px",
    lineLeft: "45px",
    right: "12px",
    rowHeight: "28px",
    top: "12px",
  },
} as const;

export const YEAR_LAYOUT = {
  barHeight: "16px",
  barLeft: "22px",
  barTop: "2px",
  barWidth: "50px",
  columnGap: "7px",
  columnMinWidth: "100px",
  countRight: "10px",
  dayLabelLeft: "5px",
  headerGap: "12px",
  rowGap: "3px",
  rowHeight: "20px",
} as const;
