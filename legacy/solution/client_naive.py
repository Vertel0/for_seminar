# -*- coding: utf-8 -*-
"""Эталон, слот 1: наивный последовательный клиент. Один POST на измерение, ждём ответ."""

import time
import httpx


async def run(measurements, *, base_url="http://127.0.0.1:8000", client=None):
    own = client is None
    if own:
        client = httpx.AsyncClient(base_url=base_url, timeout=10.0)
    latencies = []
    ok = err = 0
    try:
        for m in measurements:
            t0 = time.perf_counter()
            try:
                r = await client.post("/telemetry", json=m)
                if r.status_code == 200:
                    ok += 1
                else:
                    err += 1   # наивный клиент НЕ ретраит — отказ = потеря
            except httpx.HTTPError:
                err += 1
            latencies.append(time.perf_counter() - t0)
    finally:
        if own:
            await client.aclose()
    return {"latencies": latencies, "ok": ok, "err": err, "sent": len(measurements)}
