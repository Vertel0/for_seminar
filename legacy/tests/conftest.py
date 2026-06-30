import os
import sys

# legacy-модуль лежит уровнем выше — добавим в путь, чтобы импортировался
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest  # noqa: E402
import forecast_processor  # noqa: E402


@pytest.fixture(autouse=True)
def _clean_infra():
    """Между тестами чистим модульную фейк-инфру (sqlite, очереди, почту)."""
    forecast_processor._reset_infra()
    yield
    forecast_processor._reset_infra()
