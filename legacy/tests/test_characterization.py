"""
Характеризующие тесты (characterization / golden master).

Они НЕ проверяют «как правильно». Они фиксируют то, как код ведёт себя ПРЯМО СЕЙЧАС,
чтобы при рефакторинге монолита ничего не уехало незаметно. Это ваша сетка безопасности:
рефакторите под зелёным — поведение не изменилось.

Значения ниже сняты с реального прогона legacy-кода на фикстурах (golden values).
"""

import forecast_processor
from forecast_processor import ForecastProcessor


def run():
    proc = ForecastProcessor()
    summary = proc.process()
    return proc, summary


def test_summary_counts_pinned():
    _, s = run()
    assert s["forecasts_fetched"] == 11
    assert s["forecasts_valid"] == 8     # из 11 отвалились: дубль, чужая станция, выход за диапазон
    assert s["matched"] == 8


def test_overall_mae_pinned():
    _, s = run()
    assert s["overall_mae_temp"] == 1.4125


def test_worst_stations_pinned():
    _, s = run()
    assert s["worst_stations"] == [
        ("NSK001", 1.9),
        ("SPB001", 1.75),
        ("MSK002", 1.05),
        ("MSK001", 0.95),
    ]


def test_side_effects_pinned():
    _, s = run()
    assert s["stored"] == 8
    assert s["alerts_sent"] == 2
    assert len(forecast_processor.SENT_EMAILS) == 2
    assert len(forecast_processor.QUEUE_MESSAGES) == 2
    assert len(forecast_processor.WEBHOOK_CALLS) == 2
    assert len(forecast_processor.MEMORY_STORE) == 8
    cur = forecast_processor._DB.execute("SELECT COUNT(*) FROM forecast_errors")
    assert cur.fetchone()[0] == 8


def test_dedup_keeps_first_source():
    # MSK001 на 12:00 есть в primary (24.0) и в third (24.3). Должен победить primary.
    proc, _ = run()
    rec = [e for e in proc.errors
           if e["station"] == "MSK001" and e["ts"].endswith("12:00:00")][0]
    assert rec["fc_temp"] == 24.0
    assert rec["source"] == "primary"


def test_unit_conversion_fahrenheit():
    # SPB001 06:00 пришёл из backup в °F: 60.8°F == 16.0°C
    proc, _ = run()
    rec = [e for e in proc.errors
           if e["station"] == "SPB001" and e["ts"].endswith("06:00:00")][0]
    assert rec["fc_temp"] == 16.0


def test_per_station_aggregate_pinned():
    _, s = run()
    by = s["by_station"]
    assert by["NSK001"]["mae_temp"] == 1.9
    assert by["NSK001"]["n"] == 2
    assert by["MSK001"]["mae_temp"] == 0.95
