# api/main.py
from __future__ import annotations

import io
import os
from datetime import date, datetime
from typing import Dict, Iterable, List, Optional, Tuple

import clickhouse_connect
from urllib.parse import urlparse
from fastapi import FastAPI, File, HTTPException, Query, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# =========================
# Konfiguracja / połączenie
# =========================

TABLE_OHLC = os.getenv("TABLE_OHLC", "ohlc")

# Wariant 1 – ClickHouse Cloud (jedno pole URL) np. https://abc123.us-east-1.aws.clickhouse.cloud:8443
CLICKHOUSE_URL = os.getenv("CLICKHOUSE_URL", "")      # np. https://abc123.eu-west-1.aws.clickhouse.cloud:8443
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")
CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE", "default")


# Wariant 2 – Self-hosted
CH_HOST = os.getenv("CH_HOST")
CH_PORT = int(os.getenv("CH_PORT", "8123"))
CH_USER = os.getenv("CH_USER")
CH_PASSWORD = os.getenv("CH_PASSWORD")

# Prosty cache klienta
_CH_CLIENT = None


def get_ch():
    global _CH_CLIENT
    if _CH_CLIENT is not None:
        return _CH_CLIENT

    if not CLICKHOUSE_URL:
        raise RuntimeError("Env CLICKHOUSE_URL is empty")

    u = urlparse(CLICKHOUSE_URL.strip())
    if u.scheme not in ("http", "https"):
        raise RuntimeError(f"CLICKHOUSE_URL must start with http(s)://, got: {CLICKHOUSE_URL}")

    host = u.hostname
    port = u.port or (8443 if u.scheme == "https" else 8123)
    interface = "https" if u.scheme == "https" else "http"

    _CH_CLIENT = clickhouse_connect.get_client(
        host=host,
        port=port,
        interface=interface,           # KLUCZOWE — zamiast url
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DATABASE,
        verify=True                    # w CH Cloud po https
    )
    return _CH_CLIENT


# =========================
# FastAPI + CORS
# =========================

app = FastAPI(title="GPW Analytics API", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # dopasuj wg potrzeb
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/ping")
def ping() -> str:
    return "pong"


# =========================
# Aliasy RAW -> .WA
# =========================

# Dodawaj wg potrzeb.
ALIASES_RAW_TO_WA: Dict[str, str] = {
    "CDPROJEKT": "CDR.WA",
    "PKNORLEN": "PKN.WA",
    "PEKAO": "PEO.WA",
    "KGHM": "KGH.WA",
    "PGE": "PGE.WA",
    "ALLEGRO": "ALE.WA",
    "DINOPL": "DNP.WA",
    "LPP": "LPP.WA",
    "ORANGEPL": "OPL.WA",
    "MERCATOR": "MRC.WA",
    # ...
}

# odwrotna mapa .WA -> RAW (wygodna do normalizacji wejścia)
ALIASES_WA_TO_RAW: Dict[str, str] = {wa.lower(): raw for raw, wa in ALIASES_RAW_TO_WA.items()}


def pretty_symbol(raw: str) -> str:
    """
    Zwraca 'ładny' ticker z sufiksem .WA jeśli znamy alias; w p.p. zwraca raw.
    """
    return ALIASES_RAW_TO_WA.get(raw, raw)


def normalize_input_symbol(s: str) -> str:
    """
    Dla wejścia użytkownika zwraca surowy symbol (RAW) używany w bazie.
    Obsługuje zarówno 'CDR.WA' jak i 'CDPROJEKT'.
    """
    if "." in s:
        # Użytkownik podał ładny ticker – zamieniamy na RAW jeśli znamy alias
        maybe = ALIASES_WA_TO_RAW.get(s.lower())
        return maybe or s
    return s


# =========================
# MODELE
# =========================

class QuoteRow(BaseModel):
    date: str
    open: float
    high: float
    low: float
    close: float
    volume: float


class PortfolioPoint(BaseModel):
    date: str
    value: float


class PortfolioStats(BaseModel):
    cagr: float
    max_drawdown: float
    volatility: float
    sharpe: float
    last_value: float


class PortfolioResp(BaseModel):
    equity: List[PortfolioPoint]
    stats: PortfolioStats


# =========================
# /symbols – lista tickerów
# =========================

@app.get("/symbols")
def symbols(
    q: Optional[str] = Query(default=None, description="fragment symbolu"),
    limit: int = Query(default=200, ge=1, le=2000),
):
    """
    Zwraca listę symboli:
    - symbol: ładny ticker (np. CDR.WA)
    - raw: surowy symbol w bazie (np. CDPROJEKT)
    """
    ch = get_ch()
    if q:
        rows = ch.query(
            f"""
            SELECT DISTINCT symbol
            FROM {TABLE_OHLC}
            WHERE positionCaseInsensitive(symbol, %(q)s) > 0
            ORDER BY symbol
            LIMIT %(limit)s
            """,
            params={"q": q, "limit": limit},
        ).result_rows
    else:
        rows = ch.query(
            f"""
            SELECT DISTINCT symbol
            FROM {TABLE_OHLC}
            ORDER BY symbol
            LIMIT %(limit)s
            """,
            params={"limit": limit},
        ).result_rows

    out = []
    for r in rows:
        raw = str(r[0])
        out.append({"symbol": pretty_symbol(raw), "raw": raw})
    return out


# =========================
# /quotes – notowania OHLC
# =========================

@app.get("/quotes", response_model=List[QuoteRow])
def quotes(symbol: str, start: Optional[str] = None):
    """
    Zwraca notowania OHLC dla symbolu od wskazanej daty.
    Obsługuje zarówno 'CDR.WA' jak i 'CDPROJEKT'.
    """
    raw_symbol = normalize_input_symbol(symbol)

    try:
        dt = date.fromisoformat(start) if start else date(2015, 1, 1)
    except Exception:
        raise HTTPException(400, "start must be in format YYYY-MM-DD")

    ch = get_ch()
    rows = ch.query(
        f"""
        SELECT toString(date) as date, open, high, low, close, volume
        FROM {TABLE_OHLC}
        WHERE symbol = %(sym)s AND date >= %(dt)s
        ORDER BY date
        """,
        params={"sym": raw_symbol, "dt": dt},
    ).named_results()

    out: List[QuoteRow] = []
    for r in rows:
        out.append(
            QuoteRow(
                date=str(r["date"]),
                open=float(r["open"]),
                high=float(r["high"]),
                low=float(r["low"]),
                close=float(r["close"]),
                volume=float(r["volume"]),
            )
        )
    return out


# =========================
# /backtest/portfolio
# =========================

def _fetch_close_series(ch_client, raw_symbol: str, start: date) -> List[Tuple[str, float]]:
    """
    Pobiera (date, close) dla symbolu od daty start.
    """
    rows = ch_client.query(
        f"""
        SELECT toString(date) AS date, close
        FROM {TABLE_OHLC}
        WHERE symbol = %(sym)s AND date >= %(dt)s
        ORDER BY date
        """,
        params={"sym": raw_symbol, "dt": start},
    ).result_rows
    return [(str(d), float(c)) for (d, c) in rows]


def _rebalance_dates(dates: List[str], freq: str) -> List[str]:
    """
    Zwraca listę dat rebalansingu (YYYY-MM-DD) dla equity kroczonej dziennie.
    freq: 'none' | 'monthly' | 'quarterly' | 'yearly'
    """
    if freq == "none":
        return []

    result: List[str] = []
    last_key = None

    for ds in dates:
        y, m, _ = ds.split("-")
        key = None
        if freq == "monthly":
            key = f"{y}-{m}"
        elif freq == "quarterly":
            q = (int(m) - 1) // 3 + 1
            key = f"{y}-Q{q}"
        elif freq == "yearly":
            key = y
        else:
            break

        if key != last_key:
            result.append(ds)
            last_key = key

    return result


def _compute_backtest(
    closes_map: Dict[str, List[Tuple[str, float]]],
    weights_pct: List[float],
    start: date,
    rebalance: str,
) -> Tuple[List[PortfolioPoint], PortfolioStats]:
    """
    Prosty backtest na dziennych close'ach z rebalancingiem.
    """
    # unia dat
    all_dates = sorted({d for series in closes_map.values() for (d, _) in series})
    if not all_dates:
        raise HTTPException(404, "Brak wspólnych notowań")

    # zbuduj słowniki {date: close} per symbol
    close_dicts: Dict[str, Dict[str, float]] = {
        sym: {d: c for (d, c) in series} for sym, series in closes_map.items()
    }

    # filtr: tylko daty obecne dla wszystkich
    common_dates = []
    for ds in all_dates:
        if all(ds in close_dicts[s] for s in close_dicts.keys()):
            common_dates.append(ds)

    if not common_dates:
        raise HTTPException(404, "Brak wspólnych notowań dla wszystkich spółek")

    # normalizacja wag
    tot = sum(weights_pct) or 1.0
    w = [x / tot for x in weights_pct]

    equity: List[PortfolioPoint] = []
    value = 1.0  # start equity
    shares: Dict[str, float] = {}

    # daty rebalansingu
    rebal_dates = set(_rebalance_dates(common_dates, rebalance))

    first_date = common_dates[0]
    # inicjalny zakup
    for sym, wi in zip(close_dicts.keys(), w):
        px = close_dicts[sym][first_date]
        shares[sym] = (value * wi) / px

    equity.append(PortfolioPoint(date=first_date, value=value))

    # kolejne dni
    for ds in common_dates[1:]:
        # aktualizacja wyceny
        value = sum(shares[s] * close_dicts[s][ds] for s in shares)
        # ewentualny rebalans na początku okresu
        if ds in rebal_dates:
            for i, sym in enumerate(close_dicts.keys()):
                px = close_dicts[sym][ds]
                shares[sym] = (value * w[i]) / px

        equity.append(PortfolioPoint(date=ds, value=value))

    # stats
    if len(equity) >= 2:
        first_v = equity[0].value
        last_v = equity[-1].value
        days = max(1, (datetime.fromisoformat(equity[-1].date) - datetime.fromisoformat(equity[0].date)).days)
        years = days / 365.25
        cagr = (last_v / first_v) ** (1 / years) - 1 if years > 0 else 0.0

        # max drawdown
        peak = -1e9
        max_dd = 0.0
        values = [pt.value for pt in equity]
        for v in values:
            peak = max(peak, v)
            dd = (v / peak) - 1.0
            if dd < max_dd:
                max_dd = dd

        # dzienna zmienność (bardzo prosto, od equity)
        import statistics

        rets: List[float] = []
        for a, b in zip(values, values[1:]):
            if a > 0:
                rets.append(b / a - 1.0)
        vol_daily = statistics.pstdev(rets) if len(rets) > 1 else 0.0
        vol_annual = vol_daily * (252 ** 0.5)

        sharpe = (cagr - 0.0) / vol_annual if vol_annual > 1e-12 else 0.0

        stats = PortfolioStats(
            cagr=cagr,
            max_drawdown=max_dd,
            volatility=vol_annual,
            sharpe=sharpe,
            last_value=last_v,
        )
    else:
        stats = PortfolioStats(
            cagr=0.0, max_drawdown=0.0, volatility=0.0, sharpe=0.0, last_value=equity[-1].value
        )

    return equity, stats


@app.get("/backtest/portfolio", response_model=PortfolioResp)
def backtest_portfolio(
    symbols: str = Query(..., description="Lista symboli rozdzielona przecinkami (np. CDR.WA,PKN.WA)"),
    weights: str = Query(..., description="Lista wag w % (np. 40,30,30)"),
    start: str = Query("2015-01-01"),
    rebalance: str = Query("monthly", pattern="^(none|monthly|quarterly|yearly)$"),
):
    """
    Prosty backtest portfela po close'ach z rebalancingiem.
    Obsługuje symbole w formacie RAW i .WA (mieszane).
    """
    # parse wejścia
    syms_in: List[str] = [s.strip() for s in symbols.split(",") if s.strip()]
    if not syms_in:
        raise HTTPException(400, "Podaj co najmniej jeden symbol")

    raw_syms: List[str] = [normalize_input_symbol(s) for s in syms_in]

    try:
        dt_start = date.fromisoformat(start)
    except Exception:
        raise HTTPException(400, "start must be in format YYYY-MM-DD")

    weights_list: List[float] = []
    for w in weights.split(","):
        w = w.strip()
        if not w:
            continue
        try:
            weights_list.append(float(w))
        except Exception:
            raise HTTPException(400, f"Nieprawidłowa waga: {w}")

    if len(weights_list) != len(raw_syms):
        raise HTTPException(400, "Liczba wag musi odpowiadać liczbie symboli")

    ch = get_ch()

    # pobierz serie close dla każdego symbolu
    closes_map: Dict[str, List[Tuple[str, float]]] = {}
    for rs in raw_syms:
        series = _fetch_close_series(ch, rs, dt_start)
        closes_map[rs] = series

    # policz backtest
    equity, stats = _compute_backtest(closes_map, weights_list, dt_start, rebalance)
    return PortfolioResp(equity=equity, stats=stats)
