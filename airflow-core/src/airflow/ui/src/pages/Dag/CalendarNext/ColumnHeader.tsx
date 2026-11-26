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
import type { PropsWithChildren } from "react";

import { Text } from "@chakra-ui/react";

import { CAPTION_FONT_SIZE, CAPTION_LINE_HEIGHT, HEADER_GAP, ROW_GAP } from "./layout";

type Props = PropsWithChildren<{ readonly gap?: string }>;

// Grids space their rows by ROW_GAP, so the header only adds what is missing up to HEADER_GAP.
const DEFAULT_GAP = `calc(${HEADER_GAP} - ${ROW_GAP})`;

export const ColumnHeader = ({ children, gap = DEFAULT_GAP }: Props) => (
  <Text
    fontSize={CAPTION_FONT_SIZE}
    lineHeight={CAPTION_LINE_HEIGHT}
    mb={gap}
    textAlign="center"
    textTransform="uppercase"
    truncate
  >
    {children}
  </Text>
);
