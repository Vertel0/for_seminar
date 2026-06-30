"""
Тесты эталонных клиентов. intake гоняется в процессе (ASGI-транспорт) — без uvicorn и сети.
Главный тест — надёжный клиент даёт 0 потерь под вредностью и падением intake.
"""

import asyncio
import httpx

from intake_service import app
from station_gen import generate, attach_idempotency_keys
import client_naive
import client_parallel
import client_reliable


def _client():
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                             base_url="http://intake", timeout=2.0)


def test_naive_delivers_all_when_healthy():
    async def scn():
        async with _client() as c:
            await c.post("/admin/reset")
            ms = attach_idempotency_keys(generate(5, 10, seed=1))
            res = await client_naive.run(ms, client=c)
            stats = (await c.get("/stats")).json()
            assert res["err"] == 0
            assert stats["accepted_unique"] == len(ms)
    asyncio.run(scn())


def test_parallel_batch_delivers_all_when_healthy():
    async def scn():
        async with _client() as c:
            await c.post("/admin/reset")
            ms = attach_idempotency_keys(generate(5, 20, seed=2))
            res = await client_parallel.run(ms, client=c, concurrency=10, batch_size=5)
            stats = (await c.get("/stats")).json()
            assert res["err"] == 0
            assert stats["accepted_unique"] == len(ms)
            assert res["requests"] == (5 * 20) // 5      # батчинг сократил число запросов
    asyncio.run(scn())


def test_reliable_zero_loss_under_failure_and_outage():
    async def scn():
        async with _client() as c:
            await c.post("/admin/reset")
            await c.post("/admin/config", json={"fail_rate": 0.3})
            await c.post("/admin/down", json={"seconds": 0.7})   # intake лежит 0.7с
            ms = attach_idempotency_keys(generate(6, 20, seed=3))
            res = await client_reliable.run(ms, client=c, request_timeout=0.5, concurrency=15)
            stats = (await c.get("/stats")).json()
            # 0 потерь и 0 задвоений: уникальных принято ровно столько, сколько слали
            assert res["unsent"] == 0
            assert stats["accepted_unique"] == len(ms)
            # ретраи реально были (иначе тест ничего не проверяет)
            assert stats["failed_injected"] > 0
    asyncio.run(scn())


def test_idempotency_prevents_double_write_under_ack_loss():
    """ack-loss: запись прошла, ответ потерян, клиент ретраит. Стабильный ключ спасает
    от задвоения — уникальных ровно столько, сколько слали, но дедуп СРАБОТАЛ (duplicates>0)."""
    async def scn():
        async with _client() as c:
            await c.post("/admin/reset")
            await c.post("/admin/config", json={"ack_loss_rate": 0.4})
            ms = attach_idempotency_keys(generate(5, 20, seed=7))
            res = await client_reliable.run(ms, client=c, request_timeout=0.5)
            stats = (await c.get("/stats")).json()
            assert res["unsent"] == 0
            assert stats["accepted_unique"] == len(ms)   # не задвоилось
            assert stats["duplicates"] > 0               # дедуп реально сработал
    asyncio.run(scn())
