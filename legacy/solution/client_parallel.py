# -*- coding: utf-8 -*-
"""
Эталон, слот 2: параллелизм (asyncio + семафор на K) + батчинг (/telemetry/batch).

Throughput растёт с K до «колена» (упёрлись в intake). Батч ещё ↑throughput и ↓число
запросов, но ↑latency батча и риск потерять весь батч при ошибке.
"""

import time
import asyncio
import httpx


def _chunks(seq, size):
    for i in range(0, len(seq), size):
        yield seq[i:i + size]


async def run(measurements, *, base_url="http://127.0.0.1:8000", client=None,
              concurrency=20, batch_size=1):
    own = client is None
    if own:
        client = httpx.AsyncClient(base_url=base_url, timeout=10.0)
    sem = asyncio.Semaphore(concurrency)
    latencies = []
    counters = {"ok": 0, "err": 0}

    async def send_batch(batch):
        async with sem:
            t0 = time.perf_counter()
            try:
                if batch_size == 1:
                    r = await client.post("/telemetry", json=batch[0])
                else:
                    r = await client.post("/telemetry/batch", json=batch)
                if r.status_code == 200:
                    counters["ok"] += len(batch)
                else:
                    counters["err"] += len(batch)
            except httpx.HTTPError:
                counters["err"] += len(batch)
            latencies.append(time.perf_counter() - t0)

    try:
        batches = list(_chunks(measurements, max(1, batch_size)))
        await asyncio.gather(*(send_batch(b) for b in batches))
    finally:
        if own:
            await client.aclose()
    return {"latencies": latencies, "ok": counters["ok"], "err": counters["err"],
            "sent": len(measurements), "requests": len(list(_chunks(measurements, max(1, batch_size))))}
