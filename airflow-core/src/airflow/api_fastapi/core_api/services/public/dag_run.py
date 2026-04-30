# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from __future__ import annotations

import asyncio
import itertools
import json
import operator
import statistics
from collections import Counter
from typing import TYPE_CHECKING, Any

import attrs
from sqlalchemy import select

from airflow.api_fastapi.core_api.datamodels.dag_run import DurationStats
from airflow.models.dagrun import DagRun
from airflow.models.xcom import XCOM_RETURN_KEY, XComModel
from airflow.utils.session import create_session_async
from airflow.utils.state import State

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Iterator


def compute_duration_stats(durations: list[float]) -> DurationStats | None:
    """
    Compute duration statistics from a list of completed DAG run durations (in seconds).

    Returns None when the list is empty (no completed runs exist yet).
    Mode is computed on second-rounded values to avoid float precision noise; returns None
    when every run has a unique duration.
    Percentiles use linear interpolation between adjacent sorted values.
    """
    if not durations:
        return None

    sorted_d = sorted(durations)

    counts = Counter(round(d) for d in sorted_d)
    max_count = max(counts.values())
    mode_val: float | None = (
        float(min(k for k, v in counts.items() if v == max_count)) if max_count > 1 else None
    )

    def _percentile(p: float) -> float:
        idx = (len(sorted_d) - 1) * p / 100
        lo = int(idx)
        hi = min(lo + 1, len(sorted_d) - 1)
        return sorted_d[lo] + (sorted_d[hi] - sorted_d[lo]) * (idx - lo)

    return DurationStats(
        mean=statistics.mean(sorted_d),
        mode=mode_val,
        p50=_percentile(50),
        p90=_percentile(90),
        p95=_percentile(95),
        p99=_percentile(99),
    )


@attrs.define
class DagRunWaiter:
    """Wait for the specified dag run to finish, and collect info from it."""

    dag_id: str
    run_id: str
    interval: float
    result_task_ids: list[str] | None

    async def _get_dag_run(self) -> DagRun:
        async with create_session_async() as session:
            return await session.scalar(select(DagRun).filter_by(dag_id=self.dag_id, run_id=self.run_id))

    async def _serialize_xcoms(self) -> dict[str, Any]:
        xcom_query = XComModel.get_many(
            run_id=self.run_id,
            key=XCOM_RETURN_KEY,
            task_ids=self.result_task_ids,
            dag_ids=self.dag_id,
        )
        async with create_session_async() as session:
            xcom_results = (
                await session.scalars(xcom_query.order_by(XComModel.task_id, XComModel.map_index))
            ).all()

        def _group_xcoms(g: Iterator[XComModel | tuple[XComModel]]) -> Any:
            entries = [row[0] if isinstance(row, tuple) else row for row in g]
            if len(entries) == 1 and entries[0].map_index < 0:  # Unpack non-mapped task xcom.
                return entries[0].value
            return [entry.value for entry in entries]  # Task is mapped; return all xcoms in a list.

        return {
            task_id: _group_xcoms(g)
            for task_id, g in itertools.groupby(xcom_results, key=operator.attrgetter("task_id"))
        }

    async def _serialize_response(self, dag_run: DagRun) -> str:
        resp = {"state": dag_run.state}
        if dag_run.state not in State.finished_dr_states:
            return json.dumps(resp)
        if self.result_task_ids:
            resp["results"] = await self._serialize_xcoms()
        return json.dumps(resp)

    async def wait(self) -> AsyncGenerator[str, None]:
        yield await self._serialize_response(dag_run := await self._get_dag_run())
        yield "\n"
        while dag_run.state not in State.finished_dr_states:
            await asyncio.sleep(self.interval)
            yield await self._serialize_response(dag_run := await self._get_dag_run())
            yield "\n"
