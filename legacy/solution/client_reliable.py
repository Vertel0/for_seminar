# -*- coding: utf-8 -*-
"""
Эталон, слот 3: надёжный клиент. 0 потерь, 0 дублей под вредностью и падением intake.

Собрано из четырёх кирпичей лекции:
  - ТАЙМАУТ на каждый запрос (без него зависший intake вешает весь клиент);
  - РЕТРАЙ с экспоненциальным backoff + jitter на 5xx/таймаут;
  - ИДЕМПОТЕНТНОСТЬ: idempotency_key стабилен на измерение (проставлен в station_gen),
    поэтому повтор не задваивает запись — сервер дедупит;
  - ЛОКАЛЬНАЯ ОЧЕРЕДЬ: неотправленные копятся в pending и до-шлются раундами,
    пока intake не поднимется (или не истечёт общий дедлайн).
"""

import time
import asyncio
import random
import httpx


async def _send_once(client, m, request_timeout):
    try:
        r = await client.post("/telemetry", json=m, timeout=request_timeout)
        return r.status_code < 500          # 2xx/4xx — принято; 5xx/504 — повторим
    except httpx.HTTPError:                  # таймаут, обрыв соединения — повторим
        return False


async def run(measurements, *, base_url="http://127.0.0.1:8000", client=None,
              concurrency=20, request_timeout=1.0, overall_deadline_s=120.0):
    own = client is None
    if own:
        client = httpx.AsyncClient(base_url=base_url)
    sem = asyncio.Semaphore(concurrency)
    rnd = random.Random(0)
    pending = list(measurements)             # локальная очередь на отправку
    attempts = 0
    start = time.monotonic()
    round_i = 0
    try:
        while pending and (time.monotonic() - start) < overall_deadline_s:
            failed = []

            async def attempt(m):
                nonlocal attempts
                async with sem:
                    ok = await _send_once(client, m, request_timeout)
                    attempts += 1
                    if not ok:
                        failed.append(m)

            await asyncio.gather(*(attempt(m) for m in pending))
            pending = failed
            round_i += 1
            if pending:
                # exp backoff + jitter между раундами (даём intake подняться)
                backoff = min(0.05 * (2 ** round_i), 2.0) + rnd.random() * 0.05
                await asyncio.sleep(backoff)
    finally:
        if own:
            await client.aclose()
    return {"sent": len(measurements), "unsent": len(pending),
            "attempts": attempts, "rounds": round_i}
