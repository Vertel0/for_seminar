# -*- coding: utf-8 -*-
"""
run_demo.py — self-contained демонстрация (для семинариста), без поднятия сервера руками.

Гоняет intake в процессе (ASGI-транспорт) и показывает контраст:
  наивный клиент под вредностью теряет данные;
  надёжный клиент под той же вредностью + падением intake на ~1с — 0 потерь, 0 дублей.

Запуск:  python3 run_demo.py
"""

import os
import sys
import asyncio

import httpx

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, os.path.join(ROOT, "lab"))
sys.path.insert(0, HERE)

from intake_service import app                      # noqa: E402
from station_gen import generate, attach_idempotency_keys  # noqa: E402
from measure import report, Stopwatch               # noqa: E402
import client_naive                                 # noqa: E402
import client_reliable                              # noqa: E402


def _client():
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                             base_url="http://intake", timeout=2.0)


async def main():
    ms = attach_idempotency_keys(generate(n_stations=10, per_station=50))
    print("Измерений в потоке:", len(ms), "\n")

    async with _client() as c:
        # 1) наивный под вредностью -> потери
        await c.post("/admin/reset")
        await c.post("/admin/config", json={"fail_rate": 0.2})
        with Stopwatch() as sw:
            res = await client_naive.run(ms, client=c)
        s1 = (await c.get("/stats")).json()
        report("НАИВНЫЙ под вредностью (fail_rate=0.2)", res["latencies"],
               sw.elapsed, res["sent"], len(ms), s1)
        print("  -> потеряно измерений:", len(ms) - s1["accepted_unique"], "\n")

        # 2) надёжный под вредностью + ack-loss (запись есть, ответ потерян) + падение на 1с
        await c.post("/admin/reset")
        await c.post("/admin/config", json={"fail_rate": 0.2, "ack_loss_rate": 0.15})
        await c.post("/admin/down", json={"seconds": 1.0})
        with Stopwatch() as sw:
            res = await client_reliable.run(ms, client=c, request_timeout=0.5)
        s2 = (await c.get("/stats")).json()
        print("--- НАДЁЖНЫЙ под вредностью + падение intake на 1с ---")
        print("  раундов очереди: %d, всего попыток: %d" % (res["rounds"], res["attempts"]))
        print("  принято уникальных: %d / %d" % (s2["accepted_unique"], len(ms)))
        print("  ИТОГ: потерь = %d, задвоений = %d (повторов задедуплено: %d)"
              % (len(ms) - s2["accepted_unique"],
                 max(0, s2["accepted_unique"] - len(ms)),
                 s2["duplicates"]))


if __name__ == "__main__":
    asyncio.run(main())
