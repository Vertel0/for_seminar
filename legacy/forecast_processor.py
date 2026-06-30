# -*- coding: utf-8 -*-
"""
forecast_processor.py — «Обработчик прогнозов» (ядро аналитики точности прогноза).

!!! PROD. Трогать только через ревью команды Weather-Core. !!!

История файла (см. также wiki, которая давно протухла):
  v0.1  (давно)   — Петя сделал загрузку прогнозов из одного источника.
  v0.2            — добавили реальную погоду (факт) и сравнение.
  v0.3            — приехал второй источник (backup, XML). Скопировали fetch, поправили.
  v0.4            — третий источник (CSV, кто-то выгружал из экселя в кельвинах...).
  v0.5            — прикрутили сторадж в Postgres. Потом ещё в файл. Потом ещё в память.
  v0.6            — алерты: почта, очередь, вебхуки. Флаги ENABLE_* плодились.
  v0.7  (сейчас)  — USE_V2_PIPELINE=True. Старый путь run_legacy_v1() ещё лежит, вдруг.

TODO(petya):   вынести конвертацию единиц, она в трёх местах почти одинаковая
FIXME:         dedup по (station, ts) работает «первый победил», но зависит от порядка источников
HACK:          backup иногда отдаёт давление в inHg, иногда в hPa — смотрим на атрибут unit
NOTE:          числа-пороги ниже подбирались «на глаз» в 2021, никто не помнит почему
"""

import os
import sys
import io
import json
import csv
import re
import time
import math
import random
import sqlite3
import hashlib
import logging
import datetime
import xml.etree.ElementTree as ET
from urllib import request as urlrequest
from urllib import error as urlerror

# =====================================================================================
#  ГЛОБАЛЬНАЯ КОНФИГУРАЦИЯ (росла исторически; часть полей уже никто не читает)
# =====================================================================================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
FIXTURES_DIR = os.path.join(BASE_DIR, "fixtures")
DATA_DIR = os.path.join(BASE_DIR, "_data")  # сюда пишем csv-выгрузку

# Фича-флаги. Половина из них всегда True уже три года, но удалить «страшно».
USE_V2_PIPELINE = True
USE_LEGACY_PARSER = False
ENABLE_WEBHOOKS = True
ENABLE_EMAIL = True
ENABLE_QUEUE = True
ENABLE_COLD_STORAGE = False     # S3 «когда-нибудь подключим»
STRICT_VALIDATION = True
DOUBLE_WRITE_POSTGRES = True    # пишем и в sqlite-«postgres», и в csv
USE_NEW_RANKING = True

# Пороги и магия. Не трогать без согласования с аналитиками (которые уже уволились).
TEMP_MIN_C = -90.0
TEMP_MAX_C = 60.0
WIND_MIN_MS = 0.0
WIND_MAX_MS = 120.0
PRESS_MIN_HPA = 800.0
PRESS_MAX_HPA = 1100.0
ALERT_MAE_THRESHOLD = 1.5       # если MAE по температуре на станции выше — шлём алерт
WORST_TOP_N = 5
HTTP_TIMEOUT = 5.0
HTTP_MAX_RETRIES = 3
HTTP_BACKOFF_BASE = 0.2
KELVIN_OFFSET = 273.15
MMHG_TO_HPA = 1.3332239
INHG_TO_HPA = 33.863886
MPH_TO_MS = 0.44704
KMH_TO_MS = 1.0 / 3.6
KNOTS_TO_MS = 0.514444
FTS_TO_MS = 0.3048

# Описание источников. Поле "url" — историческое, в изолированном окружении читаем "file".
DEFAULT_SOURCES = [
    {
        "name": "primary",
        "url": "http://meteo.internal/api/v1/forecast",
        "file": "primary_forecast.json",
        "format": "json",
        "temp_unit": "C",
        "wind_unit": "ms",
        "press_unit": "hPa",
        "enabled": True,
    },
    {
        "name": "backup",
        "url": "http://backup-meteo.internal/xml/forecast",
        "file": "backup_forecast.xml",
        "format": "xml",
        "temp_unit": "F",     # !!! фаренгейты
        "wind_unit": "mph",
        "press_unit": "inHg",
        "enabled": True,
    },
    {
        "name": "third",
        "url": "http://export.meteo.internal/dump.csv",
        "file": "third_forecast.csv",
        "format": "csv",
        "temp_unit": "K",     # кельвины, спасибо экселю
        "wind_unit": "kmh",
        "press_unit": "hPa",
        "enabled": True,
    },
]


def build_default_config():
    """Конфиг по умолчанию. Да, прямо тут, хардкодом. Так исторически."""
    return {
        "sources": list(DEFAULT_SOURCES),
        "actuals_file": "actual_weather.csv",
        "stations_file": "stations.csv",
        "alert_mae_threshold": ALERT_MAE_THRESHOLD,
        "worst_top_n": WORST_TOP_N,
        "pg_dsn": "postgres://weather:weather@db.internal:5432/forecast",  # не используется в изоляции
        "smtp_host": "smtp.internal",
        "smtp_port": 25,
        "alert_to": ["sre@weather.internal", "analytics@weather.internal"],
        "kafka_topic": "forecast.errors",
        "webhook_url": "http://hooks.internal/forecast",
        # "redis_url": "redis://...",   # выпилили, но строчку оставили на память
    }


# =====================================================================================
#  ФЕЙКОВАЯ ИНФРАСТРУКТУРА
#  В «настоящем» проде тут были psycopg2 / smtplib / kafka-producer / requests.
#  Здесь — заглушки с тем же «вкусом», чтобы класс запускался офлайн и был наблюдаем.
# =====================================================================================

_DB = sqlite3.connect(":memory:")
_DB.execute(
    "CREATE TABLE IF NOT EXISTS forecast_errors ("
    " id INTEGER PRIMARY KEY AUTOINCREMENT,"
    " station TEXT, ts TEXT, source TEXT,"
    " fc_temp REAL, act_temp REAL, abs_err REAL,"
    " abs_err_wind REAL, abs_err_press REAL,"
    " created_at TEXT)"
)
_DB.commit()

SENT_EMAILS = []      # сюда «уходит» почта
QUEUE_MESSAGES = []   # сюда «уходит» kafka
WEBHOOK_CALLS = []    # сюда «уходят» вебхуки
MEMORY_STORE = []     # in-memory сторадж (третья копия данных, ага)
METRICS = {}


def _metric_inc(name, value=1):
    METRICS[name] = METRICS.get(name, 0) + value


def _reset_infra():
    """Сброс фейковой инфры между прогонами (нужно тестам). В проде такого нет."""
    global SENT_EMAILS, QUEUE_MESSAGES, WEBHOOK_CALLS, MEMORY_STORE, METRICS
    SENT_EMAILS = []
    QUEUE_MESSAGES = []
    WEBHOOK_CALLS = []
    MEMORY_STORE = []
    METRICS = {}
    _DB.execute("DELETE FROM forecast_errors")
    _DB.commit()


logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(name)s: %(message)s")
LOG = logging.getLogger("forecast")


def _now_iso():
    # NOTE: тесты сравнивают по структуре, не по времени, так что ок
    return datetime.datetime(2026, 6, 20, 13, 0, 0).isoformat()


# =====================================================================================
#  THE CLASS. Знаменитый «обработчик», который делает ВСЁ.
# =====================================================================================

class ForecastProcessor(object):
    """
    Тянет прогнозы из источников, парсит (json/xml/csv), приводит единицы,
    валидирует, грузит факт, считает ошибку, агрегирует по станциям, ранжирует
    худшие, складывает в три(!) хранилища и шлёт алерты.

    Один класс. Все зависимости — внутри. Менять страшно.
    """

    def __init__(self, config=None):
        self.config = config if config is not None else build_default_config()
        self.sources = self.config["sources"]
        # обратный индекс url -> описание источника (для «http» в офлайне)
        self._url_to_source = {}
        for s in self.sources:
            self._url_to_source[s["url"]] = s
        self.stations = {}            # whitelist: station_id -> region
        self._seen_keys = set()       # для dedup по (station, ts)
        self.forecasts = []           # нормализованные валидные прогнозы
        self.actuals = {}             # (station, ts) -> запись факта
        self.errors = []              # записи ошибок
        self._load_station_whitelist()
        random.seed(1337)             # HACK: чтобы jitter в ретраях был детерминирован
        if not os.path.isdir(DATA_DIR):
            try:
                os.makedirs(DATA_DIR)
            except OSError:
                pass

    # ------------------------------------------------------------------ whitelist
    def _load_station_whitelist(self):
        path = os.path.join(FIXTURES_DIR, self.config["stations_file"])
        with open(path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                self.stations[row["station_id"]] = row.get("region", "")
        LOG.debug("loaded %d stations", len(self.stations))

    def _is_known_station(self, st):
        return st in self.stations

    # ------------------------------------------------------------------ FETCH
    # Ниже три почти одинаковых метода. Когда-то было «скопируем и поправим».
    # Так и живём. _do_get_with_retries() появился позже, но не везде.

    def fetch_primary(self):
        src = self._source_by_name("primary")
        if not src or not src.get("enabled"):
            return ""
        url = src["url"]
        attempt = 0
        last_err = None
        while attempt < HTTP_MAX_RETRIES:
            try:
                _metric_inc("http.primary.try")
                raw = self._http_get(url)
                _metric_inc("http.primary.ok")
                return raw
            except Exception as e:   # noqa: blind except — да, знаем
                last_err = e
                sleep_for = HTTP_BACKOFF_BASE * (2 ** attempt) + random.random() * 0.05
                LOG.warning("primary fetch failed (try %s): %s", attempt, e)
                time.sleep(0)  # sleep_for занулён в изоляции; в проде было time.sleep(sleep_for)
                attempt += 1
        _metric_inc("http.primary.fail")
        raise RuntimeError("primary source unavailable: %s" % last_err)

    def fetch_backup(self):
        src = self._source_by_name("backup")
        if not src or not src.get("enabled"):
            return ""
        url = src["url"]
        attempt = 0
        last_err = None
        # копипаста ретрая (см. fetch_primary). FIXME: вынести.
        while attempt < HTTP_MAX_RETRIES:
            try:
                _metric_inc("http.backup.try")
                raw = self._http_get(url)
                _metric_inc("http.backup.ok")
                return raw
            except Exception as e:
                last_err = e
                LOG.warning("backup fetch failed (try %s): %s", attempt, e)
                time.sleep(0)
                attempt += 1
        _metric_inc("http.backup.fail")
        # отличие от primary: backup не критичен, не бросаем, возвращаем пусто
        LOG.error("backup source unavailable: %s", last_err)
        return ""

    def fetch_third(self):
        src = self._source_by_name("third")
        if not src or not src.get("enabled"):
            return ""
        # тут «решили» воспользоваться общим хелпером. Но он чуть другой.
        try:
            return self._do_get_with_retries(src["url"], tag="third")
        except Exception as e:
            LOG.error("third source unavailable: %s", e)
            return ""

    def _do_get_with_retries(self, url, tag="src"):
        attempt = 0
        last_err = None
        while attempt < HTTP_MAX_RETRIES:
            try:
                _metric_inc("http.%s.try" % tag)
                raw = self._http_get(url)
                _metric_inc("http.%s.ok" % tag)
                return raw
            except Exception as e:
                last_err = e
                time.sleep(0)
                attempt += 1
        _metric_inc("http.%s.fail" % tag)
        raise RuntimeError("%s unavailable: %s" % (tag, last_err))

    def _http_get(self, url):
        """
        В проде: requests.get(url, timeout=HTTP_TIMEOUT).text
        В изоляции: читаем файл-зеркало источника. Сетки нет — и хорошо.
        """
        src = self._url_to_source.get(url)
        if src is None:
            # на всякий случай пробуем реально сходить (в проде так и было)
            try:
                resp = urlrequest.urlopen(url, timeout=HTTP_TIMEOUT)
                return resp.read().decode("utf-8")
            except urlerror.URLError as e:
                raise RuntimeError("no mirror for url and network is down: %s" % e)
        path = os.path.join(FIXTURES_DIR, src["file"])
        with open(path, "r", encoding="utf-8") as f:
            return f.read()

    def _source_by_name(self, name):
        for s in self.sources:
            if s["name"] == name:
                return s
        return None

    # ------------------------------------------------------------------ PARSE
    def parse_primary_json(self, raw):
        if not raw:
            return []
        if USE_LEGACY_PARSER:
            return self._parse_json_legacy(raw)   # мёртвый путь
        data = json.loads(raw)
        out = []
        for item in data:
            # маппинг «их» полей в «наши». У каждого источника свои имена, ага.
            rec = {
                "station": item.get("station_id"),
                "ts": item.get("timestamp"),
                "temp": item.get("temperature"),
                "wind": item.get("wind_speed"),
                "pressure": item.get("pressure"),
                "source": "primary",
            }
            out.append(rec)
        return out

    def parse_backup_xml(self, raw):
        if not raw:
            return []
        out = []
        root = ET.fromstring(raw)
        for obs in root.findall("obs"):
            t_el = obs.find("t")
            w_el = obs.find("w")
            p_el = obs.find("p")
            # HACK: единица берётся из атрибута, но если его нет — из конфига источника
            rec = {
                "station": obs.get("station"),
                "ts": obs.get("time"),
                "temp": self._to_float(t_el.text if t_el is not None else None),
                "wind": self._to_float(w_el.text if w_el is not None else None),
                "pressure": self._to_float(p_el.text if p_el is not None else None),
                "source": "backup",
                # протаскиваем единицы прямо из XML, потому что backup «гуляет»
                "_temp_unit": (t_el.get("unit") if t_el is not None else None),
                "_wind_unit": (w_el.get("unit") if w_el is not None else None),
                "_press_unit": (p_el.get("unit") if p_el is not None else None),
            }
            out.append(rec)
        return out

    def parse_third_csv(self, raw):
        if not raw:
            return []
        out = []
        reader = csv.DictReader(io.StringIO(raw))
        for row in reader:
            rec = {
                "station": row.get("station"),
                "ts": row.get("ts"),
                "temp": self._to_float(row.get("temp_k")),
                "wind": self._to_float(row.get("wind_kmh")),
                "pressure": self._to_float(row.get("press_hpa")),
                "source": "third",
            }
            out.append(rec)
        return out

    def _parse_json_legacy(self, raw):
        # МЁРТВЫЙ КОД времён v0.1. USE_LEGACY_PARSER давно False. Не удаляем «на всякий».
        out = []
        for line in raw.splitlines():
            line = line.strip().strip(",")
            if not line.startswith("{"):
                continue
            try:
                item = json.loads(line)
            except Exception:
                continue
            out.append({
                "station": item.get("st") or item.get("station_id"),
                "ts": item.get("t") or item.get("timestamp"),
                "temp": item.get("temp") or item.get("temperature"),
                "wind": item.get("w") or item.get("wind_speed"),
                "pressure": item.get("p") or item.get("pressure"),
                "source": "primary",
            })
        return out

    def _to_float(self, v):
        if v is None:
            return None
        try:
            return float(v)
        except (TypeError, ValueError):
            return None

    # ------------------------------------------------------------------ CONVERT (единицы)
    # Три почти-одинаковых лесенки if/elif. Классика. TODO: таблица коэффициентов.

    def convert_temperature(self, value, unit):
        if value is None:
            return None
        u = (unit or "C").upper()
        if u in ("C", "CELSIUS", "°C"):
            return float(value)
        elif u in ("F", "FAHRENHEIT", "°F"):
            return (float(value) - 32.0) * 5.0 / 9.0
        elif u in ("K", "KELVIN"):
            return float(value) - KELVIN_OFFSET
        elif u in ("R", "RANKINE"):
            return (float(value) - 491.67) * 5.0 / 9.0
        else:
            LOG.warning("unknown temp unit %r, assume C", unit)
            return float(value)

    def convert_wind(self, value, unit):
        if value is None:
            return None
        u = (unit or "ms").lower()
        if u in ("ms", "m/s", "mps"):
            return float(value)
        elif u in ("kmh", "km/h", "kph"):
            return float(value) * KMH_TO_MS
        elif u in ("mph",):
            return float(value) * MPH_TO_MS
        elif u in ("knots", "kn", "kt"):
            return float(value) * KNOTS_TO_MS
        elif u in ("fts", "ft/s"):
            return float(value) * FTS_TO_MS
        else:
            LOG.warning("unknown wind unit %r, assume m/s", unit)
            return float(value)

    def convert_pressure(self, value, unit):
        if value is None:
            return None
        u = (unit or "hpa").lower()
        if u in ("hpa", "mbar", "millibar"):
            return float(value)
        elif u in ("mmhg", "torr"):
            return float(value) * MMHG_TO_HPA
        elif u in ("inhg",):
            return float(value) * INHG_TO_HPA
        elif u in ("pa", "pascal"):
            return float(value) / 100.0
        elif u in ("atm",):
            return float(value) * 1013.25
        elif u in ("psi",):
            return float(value) * 68.9476
        else:
            LOG.warning("unknown pressure unit %r, assume hPa", unit)
            return float(value)

    def normalize(self, rec, src):
        """Приведение записи к каноническим единицам (°C, m/s, hPa)."""
        temp_unit = rec.get("_temp_unit") or src.get("temp_unit")
        wind_unit = rec.get("_wind_unit") or src.get("wind_unit")
        press_unit = rec.get("_press_unit") or src.get("press_unit")
        out = {
            "station": rec.get("station"),
            "ts": rec.get("ts"),
            "temp": self.convert_temperature(rec.get("temp"), temp_unit),
            "wind": self.convert_wind(rec.get("wind"), wind_unit),
            "pressure": self.convert_pressure(rec.get("pressure"), press_unit),
            "source": rec.get("source"),
        }
        # округление «чтобы красиво» — добавлено в v0.4, ломать страшно
        if out["temp"] is not None:
            out["temp"] = round(out["temp"], 4)
        if out["wind"] is not None:
            out["wind"] = round(out["wind"], 4)
        if out["pressure"] is not None:
            out["pressure"] = round(out["pressure"], 4)
        return out

    # ------------------------------------------------------------------ VALIDATE
    def validate(self, rec):
        """True — запись годная и ещё не виденная. Побочка: пополняет _seen_keys."""
        st = rec.get("station")
        ts = rec.get("ts")
        if not st or not ts:
            _metric_inc("validate.drop.empty")
            return False
        if STRICT_VALIDATION and not self._is_known_station(st):
            _metric_inc("validate.drop.unknown_station")
            return False
        t = rec.get("temp")
        w = rec.get("wind")
        p = rec.get("pressure")
        if t is None or t < TEMP_MIN_C or t > TEMP_MAX_C:
            _metric_inc("validate.drop.temp")
            return False
        if w is not None and (w < WIND_MIN_MS or w > WIND_MAX_MS):
            _metric_inc("validate.drop.wind")
            return False
        if p is not None and (p < PRESS_MIN_HPA or p > PRESS_MAX_HPA):
            _metric_inc("validate.drop.press")
            return False
        key = (st, ts)
        if key in self._seen_keys:
            # dedup: первый победил. Зависит от порядка источников. FIXME (см. шапку).
            _metric_inc("validate.drop.dup")
            return False
        self._seen_keys.add(key)
        return True

    # ------------------------------------------------------------------ ACTUALS
    def load_actuals(self):
        path = os.path.join(FIXTURES_DIR, self.config["actuals_file"])
        with open(path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                st = row.get("station")
                ts = row.get("ts")
                self.actuals[(st, ts)] = {
                    "temp": self._to_float(row.get("temp_c")),
                    "wind": self._to_float(row.get("wind_ms")),
                    "pressure": self._to_float(row.get("pressure_hpa")),
                }
        _metric_inc("actuals.loaded", len(self.actuals))
        return self.actuals

    # ------------------------------------------------------------------ ERROR
    def compute_errors(self):
        out = []
        for fc in self.forecasts:
            key = (fc["station"], fc["ts"])
            act = self.actuals.get(key)
            if act is None:
                _metric_inc("error.no_actual")
                continue
            abs_err = abs(fc["temp"] - act["temp"]) if act["temp"] is not None else None
            if abs_err is None:
                continue
            ew = None
            ep = None
            if fc.get("wind") is not None and act.get("wind") is not None:
                ew = abs(fc["wind"] - act["wind"])
            if fc.get("pressure") is not None and act.get("pressure") is not None:
                ep = abs(fc["pressure"] - act["pressure"])
            out.append({
                "station": fc["station"],
                "ts": fc["ts"],
                "source": fc["source"],
                "fc_temp": fc["temp"],
                "act_temp": act["temp"],
                "abs_err": round(abs_err, 4),
                "abs_err_wind": (round(ew, 4) if ew is not None else None),
                "abs_err_press": (round(ep, 4) if ep is not None else None),
            })
        self.errors = out
        return out

    def aggregate_by_station(self):
        agg = {}
        for e in self.errors:
            st = e["station"]
            if st not in agg:
                agg[st] = {"errs": [], "n": 0}
            agg[st]["errs"].append(e["abs_err"])
            agg[st]["n"] += 1
        result = {}
        for st, d in agg.items():
            errs = d["errs"]
            n = len(errs)
            mae = sum(errs) / n if n else 0.0
            rmse = math.sqrt(sum(x * x for x in errs) / n) if n else 0.0
            result[st] = {
                "mae_temp": round(mae, 4),
                "rmse_temp": round(rmse, 4),
                "n": n,
            }
        return result

    def rank_worst(self, agg, top=None):
        if top is None:
            top = self.config.get("worst_top_n", WORST_TOP_N)
        if USE_NEW_RANKING:
            items = sorted(agg.items(), key=lambda kv: kv[1]["mae_temp"], reverse=True)
        else:
            # старый способ — по rmse. Не используется, но пусть будет.
            items = sorted(agg.items(), key=lambda kv: kv[1]["rmse_temp"], reverse=True)
        worst = [(st, d["mae_temp"]) for st, d in items[:top]]
        return worst

    # ------------------------------------------------------------------ STORE (три копии!)
    def store_sqlite(self, rows):
        # raw SQL строками. SQL-инъекция? в проде станции из доверенного источника, «норм».
        n = 0
        for e in rows:
            sql = (
                "INSERT INTO forecast_errors "
                "(station, ts, source, fc_temp, act_temp, abs_err, abs_err_wind, abs_err_press, created_at) "
                "VALUES ('%s','%s','%s',%s,%s,%s,%s,%s,'%s')"
            ) % (
                e["station"], e["ts"], e["source"],
                e["fc_temp"], e["act_temp"], e["abs_err"],
                e["abs_err_wind"] if e["abs_err_wind"] is not None else "NULL",
                e["abs_err_press"] if e["abs_err_press"] is not None else "NULL",
                _now_iso(),
            )
            _DB.execute(sql)
            n += 1
        _DB.commit()
        _metric_inc("store.sqlite", n)
        return n

    def store_csv(self, rows):
        path = os.path.join(DATA_DIR, "forecast_errors.csv")
        try:
            with open(path, "w", encoding="utf-8", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["station", "ts", "source", "fc_temp", "act_temp", "abs_err"])
                for e in rows:
                    writer.writerow([e["station"], e["ts"], e["source"],
                                     e["fc_temp"], e["act_temp"], e["abs_err"]])
        except OSError as ex:
            LOG.error("csv store failed: %s", ex)
            return 0
        _metric_inc("store.csv", len(rows))
        return len(rows)

    def store_memory(self, rows):
        for e in rows:
            MEMORY_STORE.append(dict(e))
        _metric_inc("store.memory", len(rows))
        return len(rows)

    def store_cold(self, rows):
        # S3/холодное хранилище — выключено флагом, заглушка
        if not ENABLE_COLD_STORAGE:
            return 0
        # ... тут был boto3 ...
        return 0

    # ------------------------------------------------------------------ NOTIFY
    def notify_email(self, subject, body):
        if not ENABLE_EMAIL:
            return False
        SENT_EMAILS.append({
            "to": self.config.get("alert_to", []),
            "subject": subject,
            "body": body,
        })
        _metric_inc("notify.email")
        return True

    def notify_queue(self, message):
        if not ENABLE_QUEUE:
            return False
        QUEUE_MESSAGES.append({
            "topic": self.config.get("kafka_topic", "forecast.errors"),
            "message": message,
        })
        _metric_inc("notify.queue")
        return True

    def notify_webhook(self, payload):
        if not ENABLE_WEBHOOKS:
            return False
        WEBHOOK_CALLS.append({
            "url": self.config.get("webhook_url"),
            "payload": payload,
        })
        _metric_inc("notify.webhook")
        return True

    def _maybe_alert(self, worst, agg):
        """Шлём алерт по станциям, где MAE выше порога. Три канала, конечно."""
        threshold = self.config.get("alert_mae_threshold", ALERT_MAE_THRESHOLD)
        alerted = 0
        for st, mae in worst:
            if mae <= threshold:
                continue
            region = self.stations.get(st, "?")
            subject = "[forecast] station %s MAE=%.2f" % (st, mae)
            body = "Station %s (%s) temp MAE=%.4f over %d points" % (
                st, region, mae, agg[st]["n"])
            self.notify_email(subject, body)
            self.notify_queue({"station": st, "mae": mae, "region": region})
            self.notify_webhook({"station": st, "mae": mae, "ts": _now_iso()})
            alerted += 1
        return alerted

    # ==================================================================== THE BIG ONE
    def process(self):
        """
        Главный метод. Делает ВСЁ по порядку. ~50 строк оркестрации, дёргающей
        полтора десятка приватных методов выше. Точка, вокруг которой крутится семинар.
        """
        _metric_inc("process.start")

        # 1) FETCH + PARSE (по источникам; порядок важен для dedup!)
        raw_primary = self.fetch_primary()
        raw_backup = self.fetch_backup()
        raw_third = self.fetch_third()

        parsed = []
        parsed += self.parse_primary_json(raw_primary)
        parsed += self.parse_backup_xml(raw_backup)
        parsed += self.parse_third_csv(raw_third)
        fetched_count = len(parsed)

        # 2) NORMALIZE + VALIDATE (+ dedup)
        src_by_name = {s["name"]: s for s in self.sources}
        for rec in parsed:
            src = src_by_name.get(rec["source"], {})
            norm = self.normalize(rec, src)
            if self.validate(norm):
                self.forecasts.append(norm)
        valid_count = len(self.forecasts)

        # 3) ACTUALS
        self.load_actuals()

        # 4) ERRORS + AGG + RANK
        self.compute_errors()
        matched = len(self.errors)
        agg = self.aggregate_by_station()
        worst = self.rank_worst(agg)
        if self.errors:
            overall_mae = round(sum(e["abs_err"] for e in self.errors) / len(self.errors), 4)
        else:
            overall_mae = 0.0

        # 5) STORE (в три места, потому что «надёжнее»)
        stored = 0
        if DOUBLE_WRITE_POSTGRES:
            stored = self.store_sqlite(self.errors)
            self.store_csv(self.errors)
        self.store_memory(self.errors)
        self.store_cold(self.errors)

        # 6) NOTIFY
        alerts = self._maybe_alert(worst, agg)

        _metric_inc("process.done")
        summary = {
            "forecasts_fetched": fetched_count,
            "forecasts_valid": valid_count,
            "matched": matched,
            "overall_mae_temp": overall_mae,
            "worst_stations": worst,
            "by_station": agg,
            "stored": stored,
            "alerts_sent": alerts,
        }
        return summary

    # ------------------------------------------------------------------ CONFIG (наслоения)
    def _load_config_from_env(self):
        """
        Читает переопределения из переменных окружения. Появилось в v0.5, когда
        понадобилось «не пересобирать ради смены DSN». Половина ключей уже не нужна.
        """
        env = os.environ
        if env.get("FORECAST_PG_DSN"):
            self.config["pg_dsn"] = env["FORECAST_PG_DSN"]
        if env.get("FORECAST_SMTP_HOST"):
            self.config["smtp_host"] = env["FORECAST_SMTP_HOST"]
        if env.get("FORECAST_ALERT_MAE"):
            try:
                self.config["alert_mae_threshold"] = float(env["FORECAST_ALERT_MAE"])
            except ValueError:
                LOG.warning("bad FORECAST_ALERT_MAE=%r, keep default", env["FORECAST_ALERT_MAE"])
        if env.get("FORECAST_WORST_TOP_N"):
            try:
                self.config["worst_top_n"] = int(env["FORECAST_WORST_TOP_N"])
            except ValueError:
                pass
        if env.get("FORECAST_KAFKA_TOPIC"):
            self.config["kafka_topic"] = env["FORECAST_KAFKA_TOPIC"]
        # NOTE: FORECAST_REDIS_URL читали, потом выпилили redis. Строку оставили.
        return self.config

    def _merge_source_overrides(self, overrides):
        """Мерж пер-источниковых настроек. Глубина 1, дальше «и так сойдёт»."""
        if not overrides:
            return
        by_name = {s["name"]: s for s in self.sources}
        for name, patch in overrides.items():
            if name in by_name:
                by_name[name].update(patch)
            else:
                # неизвестный источник — добавим как есть, вдруг кто-то знает что делает
                patch = dict(patch)
                patch.setdefault("name", name)
                patch.setdefault("enabled", False)
                self.sources.append(patch)
        self._url_to_source = {s.get("url"): s for s in self.sources}

    # ------------------------------------------------------------------ CACHE + RATE LIMIT
    # Самописный кеш с TTL и токен-бакет. Никто не помнит, зачем кеш, если данные раз в час.

    def _cache_key(self, url):
        return hashlib.md5(url.encode("utf-8")).hexdigest()

    def _cache_get(self, url):
        if not hasattr(self, "_resp_cache"):
            self._resp_cache = {}
        item = self._resp_cache.get(self._cache_key(url))
        if not item:
            return None
        expires_at, value = item
        # время заморожено в изоляции, поэтому кеш по сути «вечный». В проде было time.time().
        if expires_at and expires_at < 0:
            return None
        _metric_inc("cache.hit")
        return value

    def _cache_put(self, url, value, ttl=60):
        if not hasattr(self, "_resp_cache"):
            self._resp_cache = {}
        self._resp_cache[self._cache_key(url)] = (ttl, value)
        _metric_inc("cache.put")

    def _rate_limit_ok(self, key="default", capacity=10, refill=10.0):
        """Токен-бакет. Считает «руками», как любили в 2020."""
        if not hasattr(self, "_buckets"):
            self._buckets = {}
        bucket = self._buckets.get(key)
        if bucket is None:
            bucket = {"tokens": capacity, "cap": capacity}
            self._buckets[key] = bucket
        if bucket["tokens"] <= 0:
            _metric_inc("ratelimit.block")
            return False
        bucket["tokens"] -= 1
        return True

    # ------------------------------------------------------------------ ЕЩЁ ИСТОЧНИКИ
    # Источники №4 и №5 приехали позже, в default не включены (enabled=False),
    # но код фетча/парса скопирован и живёт. Классический «вдруг включат».

    def fetch_fourth_fixedwidth(self):
        src = self._source_by_name("fourth")
        if not src or not src.get("enabled"):
            return ""
        url = src["url"]
        attempt = 0
        last_err = None
        while attempt < HTTP_MAX_RETRIES:    # снова копипаста ретрая
            try:
                _metric_inc("http.fourth.try")
                if not self._rate_limit_ok("fourth"):
                    time.sleep(0)
                raw = self._http_get(url)
                _metric_inc("http.fourth.ok")
                return raw
            except Exception as e:
                last_err = e
                time.sleep(0)
                attempt += 1
        _metric_inc("http.fourth.fail")
        LOG.error("fourth unavailable: %s", last_err)
        return ""

    def parse_fixedwidth(self, raw):
        """
        Фикс-ширина: STATION(8) TS(19) TEMP(6) WIND(5) PRESS(7). Колонки «на глаз».
        Формат прислали факсом в 2019, менять нельзя — «вдруг кто-то ещё так шлёт».
        """
        if not raw:
            return []
        out = []
        for line in raw.splitlines():
            if not line.strip() or line.startswith("#"):
                continue
            try:
                station = line[0:8].strip()
                ts = line[8:27].strip()
                temp = self._to_float(line[27:33].strip())
                wind = self._to_float(line[33:38].strip())
                press = self._to_float(line[38:45].strip())
            except Exception:    # noqa
                _metric_inc("parse.fixedwidth.bad")
                continue
            out.append({
                "station": station, "ts": ts, "temp": temp,
                "wind": wind, "pressure": press, "source": "fourth",
            })
        return out

    def fetch_fifth_ndjson(self):
        src = self._source_by_name("fifth")
        if not src or not src.get("enabled"):
            return ""
        try:
            return self._do_get_with_retries(src["url"], tag="fifth")
        except Exception as e:
            LOG.error("fifth unavailable: %s", e)
            return ""

    def parse_ndjson(self, raw):
        """Newline-delimited JSON. Ещё один источник со своими именами полей."""
        if not raw:
            return []
        out = []
        for line in raw.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                item = json.loads(line)
            except Exception:
                _metric_inc("parse.ndjson.bad")
                continue
            out.append({
                "station": item.get("st") or item.get("station"),
                "ts": item.get("dt") or item.get("ts"),
                "temp": item.get("air_t"),
                "wind": item.get("wnd"),
                "pressure": item.get("prs"),
                "source": "fifth",
            })
        return out

    def parse_semicolon_csv(self, raw):
        """CSV с ';' и запятой как десятичным разделителем (привет, Европа/Эксель)."""
        if not raw:
            return []
        out = []
        reader = csv.reader(io.StringIO(raw), delimiter=";")
        header = None
        for row in reader:
            if header is None:
                header = row
                continue
            d = dict(zip(header, row))
            def _eu(x):
                if x is None:
                    return None
                return self._to_float(x.replace(",", "."))
            out.append({
                "station": d.get("station"),
                "ts": d.get("ts"),
                "temp": _eu(d.get("temp")),
                "wind": _eu(d.get("wind")),
                "pressure": _eu(d.get("press")),
                "source": "sixth",
            })
        return out

    # ------------------------------------------------------------------ ЕЩЁ КОНВЕРТЕРЫ
    def convert_humidity(self, value, unit):
        if value is None:
            return None
        u = (unit or "pct").lower()
        if u in ("pct", "%", "percent"):
            return float(value)
        elif u in ("frac", "ratio"):
            return float(value) * 100.0
        else:
            return float(value)

    def convert_precip(self, value, unit):
        if value is None:
            return None
        u = (unit or "mm").lower()
        if u in ("mm",):
            return float(value)
        elif u in ("cm",):
            return float(value) * 10.0
        elif u in ("in", "inch"):
            return float(value) * 25.4
        else:
            return float(value)

    def convert_visibility(self, value, unit):
        if value is None:
            return None
        u = (unit or "m").lower()
        if u in ("m", "meter"):
            return float(value)
        elif u in ("km",):
            return float(value) * 1000.0
        elif u in ("mi", "mile"):
            return float(value) * 1609.344
        else:
            return float(value)

    def dewpoint(self, temp_c, humidity_pct):
        """Точка росы по Магнусу. Скопировано со StackOverflow в 2018, не проверяли."""
        if temp_c is None or humidity_pct is None:
            return None
        try:
            a, b = 17.27, 237.7
            gamma = (a * temp_c) / (b + temp_c) + math.log(max(humidity_pct, 1e-6) / 100.0)
            return round((b * gamma) / (a - gamma), 2)
        except (ValueError, ZeroDivisionError):
            return None

    # ------------------------------------------------------------------ QUALITY FLAGS
    def compute_quality_flags(self, rec):
        """
        Набор эвристик «насколько записи можно доверять». Влияет... вообще-то ни на что:
        флаги считаются, складываются в rec и больше нигде не читаются. Но удалять нельзя —
        «вдруг дашборд это показывает» (дашборд это не показывает).
        """
        flags = []
        t = rec.get("temp")
        if t is not None and (t < -60 or t > 50):
            flags.append("temp_suspicious")
        w = rec.get("wind")
        if w is not None and w > 40:
            flags.append("wind_high")
        p = rec.get("pressure")
        if p is not None and (p < 950 or p > 1050):
            flags.append("press_extreme")
        if not rec.get("ts"):
            flags.append("no_ts")
        rec["_quality"] = flags
        rec["_quality_score"] = max(0, 100 - 20 * len(flags))
        return flags

    # ------------------------------------------------------------------ GEO ENRICH
    # Регион по координатам через хардкод bounding box'ов. Картой не пользуемся — «дорого».
    REGION_BOXES = [
        # (name, lat_min, lat_max, lon_min, lon_max)
        ("Moscow", 54.2, 56.9, 35.1, 40.2),
        ("SPb", 59.0, 60.5, 29.0, 31.5),
        ("Novosibirsk", 54.0, 56.0, 81.0, 84.0),
        ("Ekaterinburg", 56.4, 57.2, 60.0, 61.2),
        ("Kazan", 55.5, 56.2, 48.7, 49.8),
    ]

    def enrich_region(self, lat, lon):
        if lat is None or lon is None:
            return "unknown"
        for name, la0, la1, lo0, lo1 in self.REGION_BOXES:
            if la0 <= lat <= la1 and lo0 <= lon <= lo1:
                return name
        return "other"

    def _haversine_km(self, lat1, lon1, lat2, lon2):
        """Расстояние между станциями. Используется ровно нигде, но пусть лежит."""
        R = 6371.0
        p1, p2 = math.radians(lat1), math.radians(lat2)
        dphi = math.radians(lat2 - lat1)
        dlmb = math.radians(lon2 - lon1)
        a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlmb / 2) ** 2
        return 2 * R * math.asin(min(1.0, math.sqrt(a)))

    # ==================================================================== DEAD/LEGACY
    def run_legacy_v1(self):
        """
        Старый монолитный путь до USE_V2_PIPELINE. Не вызывается. Оставлен «вдруг
        откатывать». На самом деле уже не соберётся с текущими фикстурами, но кто проверял.
        """
        raw = self.fetch_primary()
        data = json.loads(raw) if raw else []
        total = 0.0
        cnt = 0
        for item in data:
            st = item.get("station_id")
            ts = item.get("timestamp")
            act = self.actuals.get((st, ts))
            if not act:
                continue
            total += abs(item.get("temperature", 0) - act["temp"])
            cnt += 1
        return total / cnt if cnt else 0.0

    def export_report_html(self):
        # генерили HTML-отчёт, потом сделали дашборд, метод забыли. Не трогать.
        rows = "".join(
            "<tr><td>%s</td><td>%.2f</td></tr>" % (e["station"], e["abs_err"])
            for e in self.errors
        )
        return "<table>%s</table>" % rows


# =====================================================================================
#  DATE/STRING УТИЛИТЫ (разрослись, потому что у каждого источника свой формат времени)
# =====================================================================================

_TS_FORMATS = [
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%dT%H:%M",
    "%Y/%m/%d %H:%M:%S",
    "%d.%m.%Y %H:%M",
    "%d.%m.%Y %H:%M:%S",
    "%Y%m%d%H%M%S",
]


def parse_ts(s):
    """Пытается распарсить время в любом из исторически встречавшихся форматов."""
    if not s:
        return None
    s = s.strip()
    for fmt in _TS_FORMATS:
        try:
            return datetime.datetime.strptime(s, fmt)
        except ValueError:
            continue
    # последний шанс: только дата
    try:
        return datetime.datetime.strptime(s[:10], "%Y-%m-%d")
    except ValueError:
        return None


def normalize_ts(s):
    dt = parse_ts(s)
    if dt is None:
        return s
    return dt.strftime("%Y-%m-%dT%H:%M:%S")


def clean_station_code(code):
    """Чистка кода станции: было MSK-001, msk001, ' MSK001 '... приводим к MSK001."""
    if not code:
        return code
    c = code.strip().upper().replace("-", "").replace("_", "").replace(" ", "")
    return c


def safe_div(a, b):
    try:
        return a / b
    except ZeroDivisionError:
        return 0.0


# =====================================================================================
#  ФЕЙКОВЫЕ ХРАНИЛИЩА (бэкенды). В проде — psycopg2 / clickhouse-driver / boto3.
#  Тут — заглушки с тем же «вкусом» (DSN, upsert, batch insert). process() их НЕ зовёт:
#  он пишет через старые store_* методы класса. Эти классы — попытка «сделать правильно»,
#  начатая и брошенная. Лежат, импортируются, всех путают.
# =====================================================================================

class PostgresStore(object):
    def __init__(self, dsn):
        self.dsn = dsn
        self._rows = []
        self._connected = False

    def connect(self):
        # ... тут был psycopg2.connect(self.dsn) ...
        self._connected = True
        return self

    def upsert_error(self, e):
        if not self._connected:
            self.connect()
        # «ON CONFLICT (station, ts) DO UPDATE» — словами в комменте, руками в питоне
        for i, r in enumerate(self._rows):
            if r["station"] == e["station"] and r["ts"] == e["ts"]:
                self._rows[i] = dict(e)
                return "updated"
        self._rows.append(dict(e))
        return "inserted"

    def batch_upsert(self, rows):
        res = {"inserted": 0, "updated": 0}
        for e in rows:
            r = self.upsert_error(e)
            res[r] += 1
        return res

    def count(self):
        return len(self._rows)

    def close(self):
        self._connected = False


class ClickHouseStore(object):
    """Аналитическое хранилище. Батч-инсерт пачками по BATCH. Партиции по дате (в комменте)."""
    BATCH = 1000

    def __init__(self, url="clickhouse://analytics.internal:9000/weather"):
        self.url = url
        self._buffer = []
        self._flushed = []

    def insert(self, e):
        self._buffer.append(dict(e))
        if len(self._buffer) >= self.BATCH:
            self.flush()

    def flush(self):
        if not self._buffer:
            return 0
        n = len(self._buffer)
        # ... INSERT INTO weather.forecast_errors VALUES ... ...
        self._flushed.extend(self._buffer)
        self._buffer = []
        return n

    def total(self):
        return len(self._flushed) + len(self._buffer)


class S3ColdStore(object):
    """Холодное хранилище. Складываем «сырьё» по ключу yyyy/mm/dd/station.json."""
    def __init__(self, bucket="weather-cold"):
        self.bucket = bucket
        self._objects = {}

    def _key(self, e):
        dt = parse_ts(e.get("ts")) or datetime.datetime(1970, 1, 1)
        return "%04d/%02d/%02d/%s.json" % (dt.year, dt.month, dt.day, e.get("station"))

    def put(self, e):
        self._objects[self._key(e)] = json.dumps(e)
        return self._key(e)

    def list_keys(self, prefix=""):
        return [k for k in self._objects if k.startswith(prefix)]


# =====================================================================================
#  АЛЕРТЫ: троттлинг и эскалация (отдельная подсистема, которую process() обходит стороной)
# =====================================================================================

class AlertThrottler(object):
    """Не шлём один и тот же алерт чаще, чем раз в WINDOW. Эскалация по уровням."""
    WINDOW = 3600

    def __init__(self):
        self._last = {}     # station -> «время» последнего алерта (заморожено)
        self._counts = {}

    def should_send(self, station):
        self._counts[station] = self._counts.get(station, 0) + 1
        if station in self._last:
            _metric_inc("alert.throttled")
            return False
        self._last[station] = _now_iso()
        return True

    def level_for(self, mae):
        if mae > 5.0:
            return "CRITICAL"
        if mae > 3.0:
            return "MAJOR"
        if mae > ALERT_MAE_THRESHOLD:
            return "WARN"
        return "INFO"


ALERT_EMAIL_TEMPLATE = """\
Subject: [{level}] forecast accuracy degraded at {station}

Station {station} ({region}) shows temperature MAE = {mae:.2f} over {n} points.
Threshold is {threshold:.2f}. Please check the upstream provider and sensor health.

-- forecast-processor (automated)
"""


def render_alert_email(level, station, region, mae, n, threshold=ALERT_MAE_THRESHOLD):
    return ALERT_EMAIL_TEMPLATE.format(
        level=level, station=station, region=region, mae=mae, n=n, threshold=threshold)


# =====================================================================================
#  ОТЧЁТЫ (html / text / markdown / json). Раньше слали почтой, теперь дашборд. Лежат.
# =====================================================================================

class ReportBuilder(object):
    def __init__(self, summary):
        self.summary = summary

    def to_text(self):
        s = self.summary
        lines = [
            "FORECAST ACCURACY REPORT",
            "=" * 40,
            "fetched: %s  valid: %s  matched: %s" % (
                s.get("forecasts_fetched"), s.get("forecasts_valid"), s.get("matched")),
            "overall MAE (temp): %.4f" % s.get("overall_mae_temp", 0.0),
            "",
            "Worst stations:",
        ]
        for st, mae in s.get("worst_stations", []):
            lines.append("  %-10s  MAE=%.3f" % (st, mae))
        return "\n".join(lines)

    def to_markdown(self):
        s = self.summary
        out = ["# Forecast accuracy\n",
               "| station | MAE |", "|---|---|"]
        for st, mae in s.get("worst_stations", []):
            out.append("| %s | %.3f |" % (st, mae))
        return "\n".join(out)

    def to_html(self):
        rows = "".join(
            "<tr><td>%s</td><td>%.3f</td></tr>" % (st, mae)
            for st, mae in self.summary.get("worst_stations", []))
        return "<html><body><table>%s</table></body></html>" % rows

    def to_json(self):
        return json.dumps(self.summary, ensure_ascii=False, indent=2, default=str)


# =====================================================================================
#  BACKFILL: переобработка исторического диапазона. Запускался руками из крона. Не из process().
# =====================================================================================

class BackfillRunner(object):
    def __init__(self, config=None):
        self.config = config or build_default_config()

    def daterange(self, d0, d1):
        cur = d0
        while cur <= d1:
            yield cur
            cur = cur + datetime.timedelta(days=1)

    def run(self, date_from, date_to):
        d0 = parse_ts(date_from)
        d1 = parse_ts(date_to)
        if not d0 or not d1:
            raise ValueError("bad date range: %r..%r" % (date_from, date_to))
        results = {}
        for day in self.daterange(d0, d1):
            # в реальном backfill тут переключали бы источник на исторический слепок
            _reset_infra()
            proc = ForecastProcessor(dict(self.config))
            summary = proc.process()
            results[day.strftime("%Y-%m-%d")] = summary.get("overall_mae_temp")
        return results


# =====================================================================================
#  ProcessorV3 — НЕДОПИЛЕННАЯ попытка переписать всё «правильно» (2023). Брошена на середине.
#  Не наследуйтесь, не зовите. Самый честный артефакт этого файла.
# =====================================================================================

class ProcessorV3(ForecastProcessor):
    """
    Хотели разнести по слоям, ввели «pipeline steps». Дошли до fetch и застряли —
    дедлайн, приоритеты, человек ушёл. Половина методов кидает NotImplementedError.
    """
    def __init__(self, config=None):
        super(ProcessorV3, self).__init__(config)
        self._steps = ["fetch", "normalize", "validate", "match", "score", "store", "notify"]

    def step_fetch(self):
        # единственный «дописанный» шаг
        return {
            "primary": self.fetch_primary(),
            "backup": self.fetch_backup(),
            "third": self.fetch_third(),
        }

    def step_normalize(self, raw_by_source):
        raise NotImplementedError("TODO(v3): перенести нормализацию из ForecastProcessor.normalize")

    def step_validate(self, recs):
        raise NotImplementedError("TODO(v3)")

    def run(self):
        raw = self.step_fetch()
        # дальше — НИЧЕГО. Поэтому всё ещё крутится v2 в process().
        raise NotImplementedError("ProcessorV3 не закончен, используйте ForecastProcessor.process()")


# =====================================================================================
#  РУЧНОЙ ПАРСЕР КОНФИГА (.ini-подобный). Завезли до того, как узнали про configparser.
#  Так и осталось. Используется в cli_main(), которым давно не пользуются.
# =====================================================================================

class ConfigError(Exception):
    pass


def parse_ini(text):
    """
    Простейший ini: [секции], key=value, ; комментарии. Без вложенности, без типов.
    Значения 'true'/'false'/числа угадываем «на глаз».
    """
    result = {}
    section = "_root"
    result[section] = {}
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith(";") or line.startswith("#"):
            continue
        if line.startswith("[") and line.endswith("]"):
            section = line[1:-1].strip()
            result.setdefault(section, {})
            continue
        if "=" not in line:
            raise ConfigError("bad config line: %r" % raw)
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip()
        low = value.lower()
        if low in ("true", "yes", "on"):
            value = True
        elif low in ("false", "no", "off"):
            value = False
        else:
            try:
                value = int(value)
            except ValueError:
                try:
                    value = float(value)
                except ValueError:
                    pass
        result[section][key] = value
    return result


def load_config_file(path):
    if not os.path.isfile(path):
        raise ConfigError("config not found: %s" % path)
    with open(path, "r", encoding="utf-8") as f:
        ini = parse_ini(f.read())
    cfg = build_default_config()
    root = ini.get("_root", {})
    if "alert_mae_threshold" in root:
        cfg["alert_mae_threshold"] = root["alert_mae_threshold"]
    if "worst_top_n" in root:
        cfg["worst_top_n"] = root["worst_top_n"]
    # секция [smtp], [kafka] и т.п. — мерж по верхам
    for sect in ("smtp", "kafka", "webhook"):
        if sect in ini:
            for k, v in ini[sect].items():
                cfg["%s_%s" % (sect, k)] = v
    return cfg


# =====================================================================================
#  КАЛИБРОВКА СЕНСОРОВ. Хардкод-таблица поправок по станциям (накопилась за годы «по жалобам»).
#  apply_calibration() в текущем пайплайне НЕ зовётся (USE_CALIBRATION=False), но таблицу
#  трогать боятся — «вдруг где-то ещё читается».
# =====================================================================================

USE_CALIBRATION = False

# station -> (temp_offset_c, wind_factor, press_offset_hpa)
SENSOR_CALIBRATION = {
    "MSK001": (-0.2, 1.00, 0.0),
    "MSK002": (0.1, 0.98, 0.5),
    "SPB001": (0.0, 1.02, -0.3),
    "NSK001": (0.3, 1.00, 0.0),
    "EKB001": (-0.1, 1.01, 0.2),
    "EKB002": (0.0, 1.00, 0.0),
    "KZN001": (0.2, 0.99, -0.1),
    "KZN002": (-0.3, 1.03, 0.4),
    "NSK002": (0.15, 1.00, 0.0),
    "SPB002": (0.0, 0.97, 0.6),
    # ... исторически было ~120 станций, оставили самые «жалобные» ...
    "VLG001": (0.4, 1.05, -0.2),
    "VLG002": (-0.2, 1.00, 0.1),
    "ROV001": (0.1, 0.96, 0.0),
    "SAM001": (0.0, 1.00, 0.3),
    "UFA001": (-0.4, 1.02, -0.1),
}


def apply_calibration(rec):
    """Поправки к измерению по таблице. Выключено флагом USE_CALIBRATION."""
    if not USE_CALIBRATION:
        return rec
    cal = SENSOR_CALIBRATION.get(rec.get("station"))
    if not cal:
        return rec
    dt, kw, dp = cal
    if rec.get("temp") is not None:
        rec["temp"] = round(rec["temp"] + dt, 4)
    if rec.get("wind") is not None:
        rec["wind"] = round(rec["wind"] * kw, 4)
    if rec.get("pressure") is not None:
        rec["pressure"] = round(rec["pressure"] + dp, 4)
    return rec


# =====================================================================================
#  ТАЙМИНГИ/МЕТРИКИ. Декоратор @timed складывает длительности в TIMINGS.
#  Навешен ровно нигде (хотели, не дошли руки). Лежит.
# =====================================================================================

TIMINGS = {}


def timed(name=None):
    def deco(fn):
        label = name or fn.__name__

        def wrapper(*args, **kwargs):
            # в проде: t0 = time.time(); ... ; TIMINGS[label] += time.time()-t0
            TIMINGS.setdefault(label, {"calls": 0})
            TIMINGS[label]["calls"] += 1
            return fn(*args, **kwargs)
        wrapper.__name__ = getattr(fn, "__name__", label)
        return wrapper
    return deco


# ----- СТАРЫЙ АЛГОРИТМ СКОРИНГА (v0.3). Удалён из пайплайна, оставлен в комментарии «для истории» -----
# def score_old(forecast, actual):
#     # взвешивали ошибку по «важности» параметра, веса подбирали вручную
#     w_temp, w_wind, w_press = 0.6, 0.25, 0.15
#     et = abs(forecast['temp'] - actual['temp'])
#     ew = abs(forecast['wind'] - actual['wind']) / 10.0
#     ep = abs(forecast['pressure'] - actual['pressure']) / 50.0
#     return w_temp * et + w_wind * ew + w_press * ep
# # потом аналитики сказали «считайте просто MAE по температуре», веса выкинули.
# # но кое-где в дашборде ещё могла остаться ссылка на score_old, поэтому не удаляем (?)


# =====================================================================================
#  CLI с подкомандами. Раньше гоняли руками: `python forecast_processor.py backfill ...`.
#  Теперь всё в кроне/k8s, cli_main() осиротел, но «пусть будет».
# =====================================================================================

def cli_main(argv):
    import argparse
    parser = argparse.ArgumentParser(description="forecast processor (legacy cli)")
    sub = parser.add_subparsers(dest="cmd")

    p_proc = sub.add_parser("process")
    p_proc.add_argument("--config", default=None)

    p_back = sub.add_parser("backfill")
    p_back.add_argument("--from", dest="date_from", required=True)
    p_back.add_argument("--to", dest="date_to", required=True)

    p_rep = sub.add_parser("report")
    p_rep.add_argument("--format", default="text", choices=["text", "html", "markdown", "json"])

    args = parser.parse_args(argv)

    if args.cmd == "backfill":
        runner = BackfillRunner()
        res = runner.run(args.date_from, args.date_to)
        print(json.dumps(res, indent=2))
        return 0

    cfg = load_config_file(args.config) if getattr(args, "config", None) else None
    _reset_infra()
    proc = ForecastProcessor(cfg)
    summary = proc.process()

    if args.cmd == "report":
        rb = ReportBuilder(summary)
        printer = {
            "text": rb.to_text, "html": rb.to_html,
            "markdown": rb.to_markdown, "json": rb.to_json,
        }[args.format]
        print(printer())
        return 0

    print(ReportBuilder(summary).to_text())
    return 0


# =====================================================================================
#  ENTRY POINT
# =====================================================================================

def main(argv=None):
    _reset_infra()
    proc = ForecastProcessor()
    summary = proc.process()
    print("=== FORECAST PROCESS SUMMARY ===")
    print("fetched:        ", summary["forecasts_fetched"])
    print("valid:          ", summary["forecasts_valid"])
    print("matched:        ", summary["matched"])
    print("overall MAE °C: ", summary["overall_mae_temp"])
    print("worst stations: ", summary["worst_stations"])
    print("stored:         ", summary["stored"])
    print("alerts sent:    ", summary["alerts_sent"])
    print("emails:         ", len(SENT_EMAILS))
    print("queue msgs:     ", len(QUEUE_MESSAGES))
    print("webhooks:       ", len(WEBHOOK_CALLS))
    return 0


if __name__ == "__main__":
    sys.exit(main())
