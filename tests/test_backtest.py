from datetime import date
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from api import main


class FakeResult:
    def __init__(self, rows):
        self.result_rows = rows


class FakeClickHouse:
    def __init__(self, data):
        self.data = {symbol: list(rows) for symbol, rows in data.items()}

    def query(self, sql, parameters=None):
        parameters = parameters or {}
        normalized_sql = " ".join(sql.split())

        if "SELECT DISTINCT symbol" in normalized_sql:
            rows = [(sym,) for sym in sorted(self.data.keys())]
            return FakeResult(rows)

        if "WHERE symbol = %(sym)s AND date >= %(dt)s" in normalized_sql:
            symbol = parameters.get("sym")
            start = parameters.get("dt")
            if isinstance(start, date):
                start_str = start.isoformat()
            else:
                start_str = str(start)
            rows = [
                (ds, close)
                for (ds, close) in self.data.get(symbol, [])
                if ds >= start_str
            ]
            return FakeResult(rows)

        if "WHERE symbol = %(sym)s" in normalized_sql:
            symbol = parameters.get("sym")
            rows = self.data.get(symbol, [])
            return FakeResult(rows)

        raise AssertionError(f"Unexpected query: {sql}")


def test_rank_symbols_with_multiple_components():
    data = {
        "AAA": [
            ("2023-01-01", 100.0),
            ("2023-01-02", 105.0),
            ("2023-01-03", 110.0),
            ("2023-01-04", 120.0),
            ("2023-01-05", 130.0),
        ],
        "BBB": [
            ("2023-01-01", 100.0),
            ("2023-01-02", 101.0),
            ("2023-01-03", 102.0),
            ("2023-01-04", 103.0),
            ("2023-01-05", 104.0),
        ],
        "CCC": [
            ("2023-01-01", 50.0),
            ("2023-01-02", 55.0),
            ("2023-01-03", 70.0),
            ("2023-01-04", 90.0),
            ("2023-01-05", 120.0),
        ],
    }
    fake = FakeClickHouse(data)

    components = [
        main.ScoreComponent(lookback_days=2, metric="total_return", weight=4),
        main.ScoreComponent(lookback_days=4, metric="total_return", weight=6),
    ]

    ranked = main._rank_symbols_by_score(fake, list(data.keys()), components)
    ordered = [sym for sym, _ in ranked]
    assert ordered[:3] == ["CCC", "AAA", "BBB"]


def test_backtest_portfolio_auto_handles_missing_history(monkeypatch):
    data = {
        "AAA": [
            ("2023-01-01", 100.0),
            ("2023-01-02", 102.0),
            ("2023-01-03", 105.0),
            ("2023-01-04", 110.0),
            ("2023-01-05", 130.0),
        ],
        "DDD": [
            ("2023-01-04", 50.0),
            ("2023-01-05", 52.0),
        ],
    }
    fake = FakeClickHouse(data)
    monkeypatch.setattr(main, "get_ch", lambda: fake)

    request = main.BacktestPortfolioRequest(
        start=date(2023, 1, 1),
        rebalance="none",
        auto=main.AutoSelectionConfig(
            top_n=2,
            components=[
                main.ScoreComponent(lookback_days=4, metric="total_return", weight=5)
            ],
            filters=main.UniverseFilters(include=["AAA", "DDD"]),
            weighting="equal",
        ),
    )

    result = main.backtest_portfolio(request)

    assert result.stats.last_value == pytest.approx(1.3, rel=1e-5)
    assert len(result.equity) == len(data["AAA"])
