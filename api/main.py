# api/main.py
from __future__ import annotations

import io
import json
import os
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Tuple

from urllib.parse import parse_qs, urlparse

import clickhouse_connect
from fastapi import Depends, FastAPI, File, HTTPException, Query, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, ValidationError, field_validator, model_validator

# =========================
# Konfiguracja / połączenie
# =========================

TABLE_OHLC = os.getenv("TABLE_OHLC", "ohlc")


def _env_bool(name: str, default: bool = False) -> bool:
    """Pomocniczo odczytuje wartości bool z env."""

    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "t", "yes", "y", "on"}


# Wariant 1 – pełny URL (np. https://abc123.eu-west-1.aws.clickhouse.cloud:8443)
CLICKHOUSE_URL = os.getenv("CLICKHOUSE_URL", "").strip()

# Wariant 2 – oddzielne pola. Działają także razem z URL, ale
# mogą nadpisywać wartości (np. inny user/hasło niż w URL).
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "").strip()
CLICKHOUSE_PORT = os.getenv("CLICKHOUSE_PORT", "").strip()
CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE", "default").strip()
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default").strip()
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "").strip()

# Flagi TLS/SSL – przydają się na Render/Cloud.
CLICKHOUSE_SECURE = _env_bool("CLICKHOUSE_SECURE", default=True)
CLICKHOUSE_VERIFY = _env_bool("CLICKHOUSE_VERIFY", default=True)
CLICKHOUSE_CA = os.getenv("CLICKHOUSE_CA", "").strip()  # ścieżka do dodatkowego certyfikatu, opcjonalna

# CORS – domyślnie pozwalamy wszystkim, ale można podać np. domenę z Vercel.
_cors_origins = os.getenv("CORS_ALLOW_ORIGINS", "*").strip()
if _cors_origins == "*":
    CORS_ALLOW_ORIGINS: List[str] = ["*"]
else:
    CORS_ALLOW_ORIGINS = [origin.strip() for origin in _cors_origins.split(",") if origin.strip()]


# Prosty cache klienta
_CH_CLIENT = None


def _str_to_bool(value: str, default: bool) -> bool:
    low = value.strip().lower()
    if low in {"1", "true", "t", "yes", "y", "on"}:
        return True
    if low in {"0", "false", "f", "no", "n", "off"}:
        return False
    return default


def _parse_clickhouse_url():
    """Zwraca słownik parametrów wyciągniętych z CLICKHOUSE_URL."""

    if not CLICKHOUSE_URL:
        return None

    u = urlparse(CLICKHOUSE_URL)
    query = parse_qs(u.query)

    def _query_last(*names: str) -> Optional[str]:
        for n in names:
            if n in query and query[n]:
                return query[n][-1]
        return None

    if u.scheme in {"http", "https"}:
        secure_default = u.scheme == "https"
    elif u.scheme in {"clickhouse", "clickhouses"}:
        secure_default = u.scheme == "clickhouses"
    else:
        raise RuntimeError(
            "CLICKHOUSE_URL must start with http(s):// or clickhouse(s)://, got: "
            f"{CLICKHOUSE_URL}"
        )

    host = u.hostname or ""
    if not host:
        raise RuntimeError("CLICKHOUSE_URL musi zawierać hosta")

    port_default = 8443 if secure_default else 8123
    port = u.port or port_default

    secure = secure_default
    secure_q = _query_last("secure", "ssl")
    if secure_q is not None:
        secure = _str_to_bool(secure_q, secure_default)

    verify = None
    verify_q = _query_last("verify", "check")
    if verify_q is not None:
        verify = _str_to_bool(verify_q, secure)

    username = u.username or _query_last("username", "user")
    password = u.password or _query_last("password", "pass")

    database = None
    if u.path and u.path != "/":
        database = u.path.lstrip("/")
    database_q = _query_last("database", "db")
    if database_q:
        database = database_q

    return {
        "host": host,
        "port": port,
        "secure": secure,
        "verify": verify,
        "username": username,
        "password": password,
        "database": database,
    }


def get_ch():
    global _CH_CLIENT
    if _CH_CLIENT is not None:
        return _CH_CLIENT

    parsed = _parse_clickhouse_url()

    if parsed:
        host = parsed["host"]
        port = parsed["port"]
        secure = parsed["secure"]
        username = parsed.get("username") or CLICKHOUSE_USER
        password = parsed.get("password") or CLICKHOUSE_PASSWORD
        database = parsed.get("database") or CLICKHOUSE_DATABASE
        verify = (
            parsed["verify"]
            if parsed.get("verify") is not None
            else (CLICKHOUSE_VERIFY if secure else False)
        )
    else:
        host = CLICKHOUSE_HOST
        if not host:
            raise RuntimeError(
                "Brak konfiguracji ClickHouse. Ustaw CLICKHOUSE_URL lub CLICKHOUSE_HOST"
            )
        try:
            port = int(CLICKHOUSE_PORT or (8443 if CLICKHOUSE_SECURE else 8123))
        except ValueError as exc:
            raise RuntimeError("CLICKHOUSE_PORT musi być liczbą całkowitą") from exc
        secure = CLICKHOUSE_SECURE
        username = CLICKHOUSE_USER
        password = CLICKHOUSE_PASSWORD
        database = CLICKHOUSE_DATABASE
        verify = CLICKHOUSE_VERIFY if secure else False

    interface = "https" if secure else "http"

    client_kwargs = {
        "host": host,
        "port": port,
        "username": username,
        "password": password,
        "database": database,
        "interface": interface,
        "secure": secure,
        "verify": verify,
    }

    if CLICKHOUSE_CA:
        client_kwargs["ca_cert"] = CLICKHOUSE_CA

    _CH_CLIENT = clickhouse_connect.get_client(**client_kwargs)
    return _CH_CLIENT


# =========================
# FastAPI + CORS
# =========================

app = FastAPI(title="GPW Analytics API", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://gpw-frontend.vercel.app",
        "http://localhost:3000",
        "*",  # opcjonalnie na czas testów
    ],
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

    W praktyce użytkownicy często wpisują tickery małymi literami albo z
    sufiksem .WA dla spółek z GPW.  Funkcja stara się więc:
    - przywrócić RAW z mapy aliasów, jeśli go znamy,
    - w przeciwnym razie, gdy ticker wygląda jak "XYZ.WA", uciąć sufiks i
      zwrócić bazowy symbol,
    - w ostateczności zwrócić wejście spójne wielkościowo (UPPER).
    """

    cleaned = s.strip()
    if not cleaned:
        return ""

    maybe = ALIASES_WA_TO_RAW.get(cleaned.lower())
    if maybe:
        return maybe

    if "." in cleaned:
        base = cleaned.split(".", 1)[0].strip()
        if base:
            return base.upper()

    return cleaned.upper()


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


class ScoreComponent(BaseModel):
    lookback_days: int = Field(..., ge=1, le=3650)
    metric: str = Field(..., description="Typ metryki score'u (np. total_return)")
    weight: int = Field(..., ge=1, le=10)

    @field_validator("metric")
    @classmethod
    def _validate_metric(cls, value: str) -> str:
        allowed = {"total_return"}
        if value not in allowed:
            raise ValueError(f"metric must be one of {sorted(allowed)}")
        return value


class UniverseFilters(BaseModel):
    include: Optional[List[str]] = None
    exclude: Optional[List[str]] = None
    prefixes: Optional[List[str]] = None

    @field_validator("include", "exclude", "prefixes", mode="before")
    @classmethod
    def _ensure_list(cls, value):
        if value is None:
            return value
        if isinstance(value, str):
            return [value]
        return list(value)

    @field_validator("include", "exclude", "prefixes")
    @classmethod
    def _cleanup(cls, value):
        if value is None:
            return value
        cleaned: List[str] = []
        for item in value:
            cleaned_item = item.strip()
            if not cleaned_item:
                raise ValueError("filter values must not be empty")
            cleaned.append(cleaned_item)
        return cleaned


class ManualPortfolioConfig(BaseModel):
    symbols: List[str] = Field(..., min_length=1)
    weights: Optional[List[float]] = None

    @model_validator(mode="after")
    def _validate_weights(self):
        if self.weights is not None and len(self.weights) != len(self.symbols):
            raise ValueError("Liczba wag musi odpowiadać liczbie symboli")
        return self


class AutoSelectionConfig(BaseModel):
    top_n: int = Field(..., ge=1, le=100)
    components: List[ScoreComponent] = Field(..., min_length=1)
    filters: Optional[UniverseFilters] = None
    weighting: str = Field("equal", pattern="^(equal|score)$")


class BacktestPortfolioRequest(BaseModel):
    start: date = Field(default=date(2015, 1, 1))
    rebalance: str = Field("monthly", pattern="^(none|monthly|quarterly|yearly)$")
    manual: Optional[ManualPortfolioConfig] = None
    auto: Optional[AutoSelectionConfig] = None

    @model_validator(mode="after")
    def _validate_mode(self):
        if self.manual and self.auto:
            raise ValueError("Wybierz tylko jeden tryb: manual lub auto")
        if not self.manual and not self.auto:
            raise ValueError("Wymagany jest tryb manual lub auto")
        return self


class RangeDescriptor(BaseModel):
    min: float
    max: float
    step: Optional[float] = None
    default: Optional[float] = None


class ComponentDescriptor(BaseModel):
    metric: str
    label: str
    description: str
    lookback_days: RangeDescriptor
    weight: RangeDescriptor


class AutoSelectionDescriptor(BaseModel):
    top_n: RangeDescriptor
    weighting_modes: List[str]
    components: List[ComponentDescriptor]
    filters: Dict[str, str]


class ManualSelectionDescriptor(BaseModel):
    description: str
    weights: str


class BacktestPortfolioTooling(BaseModel):
    start: str
    rebalance_modes: List[str]
    manual: ManualSelectionDescriptor
    auto: AutoSelectionDescriptor


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
            parameters={"q": q, "limit": limit},
        ).result_rows
    else:
        rows = ch.query(
            f"""
            SELECT DISTINCT symbol
            FROM {TABLE_OHLC}
            ORDER BY symbol
            LIMIT %(limit)s
            """,
            parameters={"limit": limit},
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
    if not raw_symbol:
        raise HTTPException(400, "symbol must not be empty")

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
        parameters={"sym": raw_symbol, "dt": dt},
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
        parameters={"sym": raw_symbol, "dt": start},
    ).result_rows
    return [(str(d), float(c)) for (d, c) in rows]


def _fetch_close_history(ch_client, raw_symbol: str) -> List[Tuple[str, float]]:
    rows = ch_client.query(
        f"""
        SELECT toString(date) AS date, close
        FROM {TABLE_OHLC}
        WHERE symbol = %(sym)s
        ORDER BY date
        """,
        parameters={"sym": raw_symbol},
    ).result_rows
    return [(str(d), float(c)) for (d, c) in rows]


def _normalize_return(value: float) -> float:
    return max(0.0, min(2.0, 1.0 + value))


def _compute_component_return(
    closes: List[Tuple[str, float]], lookback_days: int
) -> Optional[float]:
    if not closes:
        return None

    last_date_str, last_close = closes[-1]
    if last_close <= 0:
        return None

    last_dt = datetime.fromisoformat(last_date_str).date()
    target_date = last_dt - timedelta(days=lookback_days)

    base_close = None
    for date_str, close in reversed(closes):
        dt = datetime.fromisoformat(date_str).date()
        if dt <= target_date:
            if close > 0:
                base_close = close
            break

    if base_close is None or base_close <= 0:
        return None

    return (last_close / base_close) - 1.0


def _calculate_symbol_score(
    ch_client, raw_symbol: str, components: List[ScoreComponent]
) -> Optional[float]:
    closes = _fetch_close_history(ch_client, raw_symbol)
    if not closes:
        return None

    total = 0.0
    for comp in components:
        if comp.metric != "total_return":
            continue
        comp_ret = _compute_component_return(closes, comp.lookback_days)
        if comp_ret is None:
            return None
        total += comp.weight * _normalize_return(comp_ret)

    return total


def _list_candidate_symbols(ch_client, filters: Optional[UniverseFilters]) -> List[str]:
    rows = ch_client.query(
        f"""
        SELECT DISTINCT symbol
        FROM {TABLE_OHLC}
        ORDER BY symbol
        """
    ).result_rows
    symbols = [str(r[0]) for r in rows]

    if not filters:
        return symbols

    includes = None
    if filters.include:
        includes = {normalize_input_symbol(sym) for sym in filters.include}
        includes = {sym for sym in includes if sym}
        if not includes:
            raise HTTPException(400, "Lista include nie zawiera poprawnych symboli")

    excludes = set()
    if filters.exclude:
        excludes = {normalize_input_symbol(sym) for sym in filters.exclude}
        excludes = {sym for sym in excludes if sym}

    prefixes = None
    if filters.prefixes:
        prefixes = [p.strip().upper() for p in filters.prefixes if p.strip()]

    filtered: List[str] = []
    for sym in symbols:
        if includes and sym not in includes:
            continue
        if sym in excludes:
            continue
        if prefixes and not any(sym.startswith(pref) for pref in prefixes):
            continue
        filtered.append(sym)

    return filtered


def _rank_symbols_by_score(
    ch_client, candidates: List[str], components: List[ScoreComponent]
) -> List[Tuple[str, float]]:
    ranked: List[Tuple[str, float]] = []
    for sym in candidates:
        score = _calculate_symbol_score(ch_client, sym, components)
        if score is None:
            continue
        ranked.append((sym, score))

    ranked.sort(key=lambda item: item[1], reverse=True)
    return ranked


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


def _run_backtest(req: BacktestPortfolioRequest) -> PortfolioResp:
    dt_start = req.start
    ch = get_ch()

    if req.manual:
        raw_syms: List[str] = []
        for s in req.manual.symbols:
            raw = normalize_input_symbol(s)
            if not raw:
                raise HTTPException(400, "Symbol nie może być pusty")
            raw_syms.append(raw)

        weights_list = list(req.manual.weights) if req.manual.weights else [1.0] * len(raw_syms)
    else:
        assert req.auto is not None
        candidates = _list_candidate_symbols(ch, req.auto.filters)
        if not candidates:
            raise HTTPException(404, "Brak symboli do oceny")

        ranked = _rank_symbols_by_score(ch, candidates, req.auto.components)
        if not ranked:
            raise HTTPException(404, "Brak symboli ze wszystkimi wymaganymi danymi")

        top = ranked[: req.auto.top_n]
        raw_syms = [sym for sym, _ in top]
        if not raw_syms:
            raise HTTPException(404, "Brak symboli po filtrach")

        if req.auto.weighting == "score":
            weights_list = [score for _, score in top]
            if not any(weights_list):
                weights_list = [1.0] * len(top)
        else:
            weights_list = [1.0] * len(top)

    closes_map: Dict[str, List[Tuple[str, float]]] = {}
    for rs in raw_syms:
        series = _fetch_close_series(ch, rs, dt_start)
        if not series:
            raise HTTPException(404, f"Brak danych historycznych dla {rs}")
        closes_map[rs] = series

    equity, stats = _compute_backtest(closes_map, weights_list, dt_start, req.rebalance)
    return PortfolioResp(equity=equity, stats=stats)


def _parse_backtest_get(
    mode: str = Query(
        default="manual",
        description="Wybierz tryb budowy portfela: manual lub auto.",
    ),
    start: str = Query(
        default=date(2015, 1, 1).isoformat(),
        description="Początek backtestu w formacie YYYY-MM-DD.",
    ),
    rebalance: str = Query(
        default="monthly",
        description="Częstotliwość rebalancingu (none, monthly, quarterly, yearly).",
    ),
    symbols: Optional[List[str]] = Query(
        default=None,
        description="Lista symboli GPW (powtarzalny parametr) dla trybu manual.",
    ),
    weights: Optional[List[float]] = Query(
        default=None,
        description="Lista wag odpowiadająca kolejności symboli (powtarzalny parametr).",
    ),
    top_n: Optional[int] = Query(
        default=None,
        description="Liczba spółek do wyboru w trybie auto.",
    ),
    weighting: str = Query(
        default="equal",
        description="Strategia wag w trybie auto: equal lub score.",
    ),
    components: Optional[List[str]] = Query(
        default=None,
        description=(
            "Lista komponentów score'u. Każdy element może być JSON-em lub zapisem "
            "lookback:metric:weight (np. 252:total_return:5)."
        ),
    ),
    filters_include: Optional[List[str]] = Query(
        default=None,
        description="Filtr: bierz pod uwagę tylko wskazane symbole.",
    ),
    filters_exclude: Optional[List[str]] = Query(
        default=None,
        description="Filtr: pomiń wskazane symbole.",
    ),
    filters_prefixes: Optional[List[str]] = Query(
        default=None,
        description="Filtr: ogranicz do symboli zaczynających się od prefiksów.",
    ),
) -> BacktestPortfolioRequest:
    try:
        start_dt = date.fromisoformat(start)
    except ValueError as exc:  # pragma: no cover - defensywne
        raise HTTPException(400, "Parametr start musi być w formacie YYYY-MM-DD") from exc

    payload: Dict[str, object] = {"start": start_dt, "rebalance": rebalance}

    mode_normalized = mode.strip().lower()
    if mode_normalized == "manual":
        if not symbols:
            raise HTTPException(400, "Tryb manual wymaga co najmniej jednego symbolu")
        manual_payload: Dict[str, object] = {"symbols": list(symbols)}
        if weights:
            manual_payload["weights"] = list(weights)
        payload["manual"] = manual_payload
    elif mode_normalized == "auto":
        if top_n is None:
            raise HTTPException(400, "Tryb auto wymaga parametru top_n")
        if not components:
            raise HTTPException(400, "Tryb auto wymaga przynajmniej jednego komponentu")

        parsed_components: List[Dict[str, object]] = []
        for raw in components:
            raw_value = raw.strip()
            if not raw_value:
                continue
            comp_data: Dict[str, object]
            try:
                loaded = json.loads(raw_value)
            except json.JSONDecodeError:
                parts = raw_value.split(":")
                if len(parts) != 3:
                    raise HTTPException(
                        400,
                        "Komponent musi być JSON-em lub mieć format lookback:metric:weight",
                    )
                lookback_str, metric, weight_str = parts
                try:
                    lookback_days = int(lookback_str)
                    weight_val = int(weight_str)
                except ValueError as exc:
                    raise HTTPException(
                        400, "Lookback i weight muszą być liczbami całkowitymi"
                    ) from exc
                comp_data = {
                    "lookback_days": lookback_days,
                    "metric": metric,
                    "weight": weight_val,
                }
            else:
                if not isinstance(loaded, dict):
                    raise HTTPException(400, "JSON komponentu musi być obiektem")
                comp_data = loaded
            parsed_components.append(comp_data)

        if not parsed_components:
            raise HTTPException(400, "Lista komponentów nie może być pusta")

        auto_payload: Dict[str, object] = {
            "top_n": top_n,
            "weighting": weighting,
            "components": parsed_components,
        }

        if filters_include or filters_exclude or filters_prefixes:
            filters_payload: Dict[str, List[str]] = {}
            if filters_include:
                filters_payload["include"] = list(filters_include)
            if filters_exclude:
                filters_payload["exclude"] = list(filters_exclude)
            if filters_prefixes:
                filters_payload["prefixes"] = list(filters_prefixes)
            auto_payload["filters"] = filters_payload

        payload["auto"] = auto_payload
    else:
        raise HTTPException(400, "Parametr mode musi przyjmować wartości manual lub auto")

    try:
        return BacktestPortfolioRequest.model_validate(payload)
    except ValidationError as exc:
        raise HTTPException(422, exc.errors()) from exc


@app.get("/backtest/portfolio", response_model=PortfolioResp)
def backtest_portfolio_get(req: BacktestPortfolioRequest = Depends(_parse_backtest_get)):
    """GET-owy wariant backtestu portfela.

    Umożliwia szybkie testy z poziomu przeglądarki, np.:

    ``/backtest/portfolio?mode=manual&symbols=CDR.WA&symbols=PKN.WA&start=2023-01-01``

    ``/backtest/portfolio?mode=auto&top_n=3&components=252:total_return:5``
    """

    return _run_backtest(req)


@app.post("/backtest/portfolio", response_model=PortfolioResp)
def backtest_portfolio(req: BacktestPortfolioRequest):
    """Backtest portfela na bazie kursów zamknięcia.

    Endpoint obsługuje zarówno klasyczny POST (JSON), jak i wariant GET opisany
    w dokumentacji wyżej. Tryb ``manual`` przyjmuje listę symboli oraz opcjonalne
    wagi, a tryb ``auto`` – konfigurację komponentów score'u wraz z filtrami
    wszechświata i sposobem ważenia (``equal`` lub ``score``).
    """

    return _run_backtest(req)


@app.get("/backtest/portfolio/tooling", response_model=BacktestPortfolioTooling)
def backtest_portfolio_tooling():
    """Zwraca metadane pomagające zbudować formularz do backtestów.

    Ułatwia frontendom przygotowanie list rozwijanych i opisów pól, tak aby
    użytkownicy mogli szybciej złożyć poprawny request ``/backtest/portfolio``.
    """

    return BacktestPortfolioTooling(
        start=date(2015, 1, 1).isoformat(),
        rebalance_modes=["none", "monthly", "quarterly", "yearly"],
        manual=ManualSelectionDescriptor(
            description="Podaj listę symboli w formacie GPW (np. CDR.WA).",
            weights=(
                "Opcjonalna lista wag – musi odpowiadać kolejności symboli. "
                "Brak oznacza równy podział."
            ),
        ),
        auto=AutoSelectionDescriptor(
            top_n=RangeDescriptor(min=1, max=100, step=1, default=5),
            weighting_modes=["equal", "score"],
            components=[
                ComponentDescriptor(
                    metric="total_return",
                    label="Skumulowana stopa zwrotu",
                    description=(
                        "Porównuje cenę końcową z wartością sprzed okresu "
                        "lookback i normalizuje wynik (0-200%)."
                    ),
                    lookback_days=RangeDescriptor(min=1, max=3650, step=1, default=252),
                    weight=RangeDescriptor(min=1, max=10, step=1, default=5),
                )
            ],
            filters={
                "include": "Lista symboli do rozważenia (priorytet nad prefixami).",
                "exclude": "Symbole, które zostaną pominięte (po normalizacji).",
                "prefixes": "Rozważaj tylko tickery zaczynające się od podanych prefiksów.",
            },
        ),
    )
