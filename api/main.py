# api/main.py
from __future__ import annotations

import io
import os
import zipfile
from datetime import date, datetime, timedelta
from typing import List, Literal, Optional

import pandas as pd
from fastapi import FastAPI, File, HTTPException, Query, UploadFile
from fastapi.middleware.cors import CORSMiddleware

# -------- ClickHouse --------
# Wymagane: pip install clickhouse-connect pandas fastapi uvicorn
import clickhouse_connect

# ===========================
# Konfiguracja / utils
# ===========================

APP_TITLE = "GPW Analytics API"
APP_VERSION = "0.1.0"

CH_HOST = os.getenv("CH_HOST", os.getenv("CLICKHOUSE_HOST", "clickhouse"))
CH_PORT = int(os.getenv("CH_PORT", os.getenv("CLICKHOUSE_PORT", "8123")))
CH_USER = os.getenv("CH_USER", os.getenv("CLICKHOUSE_USER", "default"))
CH_PASSWORD = os.getenv("CH_PASSWORD", os.getenv("CLICKHOUSE_PASSWORD", ""))

TABLE_OHLC = "ohlc"

def get_ch():
    """
    Zwraca klienta ClickHouse (połączenie HTTP).
    """
    return clickhouse_connect.get_client(
        host=CH_HOST,
        port=CH_PORT,
        username=CH_USER,
        password=CH_PASSWORD,
        connect_timeout=10,
    )

def ensure_tables():
    """
    Tworzy tabelę OHLC, jeśli nie istnieje.
    """
    ch = get_ch()
    ch.command(
        f"""
        CREATE TABLE IF NOT EXISTS {TABLE_OHLC} (
            symbol String,
            date   Date,
            open   Float64,
            high   Float64,
            low    Float64,
            close  Float64,
            volume UInt64
        )
        ENGINE = MergeTree()
        ORDER BY (symbol, date)
        SETTINGS index_granularity = 8192
        """
    )

# ===========================
# FastAPI
# ===========================

app = FastAPI(title=APP_TITLE, version=APP_VERSION)

# CORS – pozwól frontendowi (Vercel, localhost)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("startup")
def _startup():
    ensure_tables()

@app.get("/ping")
def ping():
    return {"ok": True, "ts": datetime.utcnow().isoformat()}

# ===========================
# Ingest: MetaStock ZIP
# ===========================

ACCEPTED_MSTA_EXT = (".mst", ".MST", ".txt", ".TXT", ".csv", ".CSV")

def _clean_symbol_from_filename(name: str) -> str:
    # np. "WIG20.mst" -> "WIG20"
    base = name.rsplit("/", 1)[-1].rsplit("\\", 1)[-1]
    sym = base.rsplit(".", 1)[0]
    return sym.strip().upper()

def _read_mst_like(path_in_zip: str, raw_bytes: bytes) -> Optional[pd.DataFrame]:
    """
    Parser „tolerancyjny” dla plików w stylu MetaStock (często z nagłówkiem:
    <DTYYYYMMDD>,<OPEN>,<HIGH>,<LOW>,<CLOSE>,<VOL>).
    Próbuje różne separatory i mapuje kolumny na: date, open, high, low, close, volume.
    """
    text = raw_bytes.decode("utf-8", errors="ignore")
    text = text.replace("\r\n", "\n").strip()

    seps = [",", ";", r"\s+"]
    df = None

    def looks_like_placeholder(cell) -> bool:
        if not isinstance(cell, str):
            return False
        s = cell.strip()
        return s.startswith("<") and s.endswith(">")

    for sep in seps:
        try:
            tmp = pd.read_csv(io.StringIO(text), sep=sep, engine="python", header=None)
        except Exception:
            continue

        if tmp.shape[1] >= 5:
            first_row = tmp.iloc[0].tolist()
            if any(looks_like_placeholder(x) for x in first_row):
                tmp.columns = [str(c).strip("<>").lower() for c in first_row]
                tmp = tmp.iloc[1:].reset_index(drop=True)
            else:
                tmp.columns = [f"col{i}" for i in range(tmp.shape[1])]
            df = tmp
            break

    if df is None or df.empty:
        return None

    cols = {c.lower(): c for c in df.columns}

    def pick(*aliases):
        for a in aliases:
            if a in cols:
                return cols[a]
        return None

    c_date  = pick("date", "dt", "dtyyyymmdd", "col0")
    c_open  = pick("open", "o", "col1")
    c_high  = pick("high", "h", "col2")
    c_low   = pick("low", "l", "col3")
    c_close = pick("close", "c", "last", "price", "col4")
    c_vol   = pick("vol", "volume", "v", "col5")

    needed = [c_date, c_open, c_high, c_low, c_close]
    if any(x is None for x in needed):
        return None

    out = df[[c_date, c_open, c_high, c_low, c_close] + ([c_vol] if c_vol else [])].copy()
    out.columns = ["date", "open", "high", "low", "close"] + (["volume"] if c_vol else [])

    # Usuń placeholdery w kolumnie daty
    out = out[~out["date"].astype(str).str.contains(r"^<.*>$", regex=True)]

    # Daty – najpierw spróbuj %Y%m%d, potem fallback
    try:
        out["date"] = pd.to_datetime(out["date"].astype(str), format="%Y%m%d", errors="coerce")
    except Exception:
        out["date"] = pd.to_datetime(out["date"], errors="coerce")
    out = out.dropna(subset=["date"])

    for c in ["open", "high", "low", "close"]:
        out[c] = pd.to_numeric(out[c], errors="coerce")

    if "volume" in out.columns:
        out["volume"] = pd.to_numeric(out["volume"], errors="coerce").fillna(0).astype("Int64")
    else:
        out["volume"] = 0

    out = out.dropna(subset=["open", "high", "low", "close"])
    out["date"] = out["date"].dt.date
    out = out.sort_values("date").reset_index(drop=True)
    out["symbol"] = _clean_symbol_from_filename(path_in_zip)
    return out[["symbol", "date", "open", "high", "low", "close", "volume"]]

def _insert_df_clickhouse(df: pd.DataFrame) -> int:
    if df is None or df.empty:
        return 0
    ch = get_ch()
    ch.insert_df(
        table=TABLE_OHLC,
        df=df,
        column_names=["symbol", "date", "open", "high", "low", "close", "volume"],
    )
    return len(df)

@app.post("/ingest/metastock-zip")
async def ingest_metastock_zip(file: UploadFile = File(...)):
    """
    Przyjmuje ZIP z plikami MetaStock (*.mst/*.txt/*.csv) i ładuje do ClickHouse (tabela ohlc).
    """
    try:
        data = await file.read()
        zf = zipfile.ZipFile(io.BytesIO(data))
    except Exception as e:
        raise HTTPException(400, f"Nieprawidłowy ZIP: {e}")

    files_seen = 0
    files_loaded = 0
    rows_inserted = 0
    errors: List[str] = []

    for info in zf.infolist():
        name = info.filename
        if info.is_dir():
            continue
        files_seen += 1

        # akceptujemy tylko rozszerzenia z listy
        if not name.endswith(ACCEPTED_MSTA_EXT):
            continue

        try:
            raw = zf.read(info.filename)
            df = _read_mst_like(name, raw)
            if df is None or df.empty:
                continue
            n = _insert_df_clickhouse(df)
            if n > 0:
                files_loaded += 1
                rows_inserted += n
        except Exception as e:
            errors.append(f"{name}: {e}")

    return {
        "files_seen": files_seen,
        "files_loaded": files_loaded,
        "rows_inserted": rows_inserted,
        "errors": errors[:10],  # max 10 zgłoszeń na podgląd
    }

# ===========================
# Dane /symbols /quotes
# ===========================

@app.get("/symbols")
def symbols(q: Optional[str] = Query(default=None, description="fragment symbolu")):
    """
    Zwraca listę symboli (unikalne) z tabeli OHLC.
    """
    ch = get_ch()
    if q:
        rows = ch.query(
            f"SELECT DISTINCT symbol FROM {TABLE_OHLC} WHERE positionCaseInsensitive(symbol, %(q)s) > 0 ORDER BY symbol LIMIT 200",
            parameters={"q": q},
        ).result_rows
    else:
        rows = ch.query(
            f"SELECT DISTINCT symbol FROM {TABLE_OHLC} ORDER BY symbol LIMIT 500"
        ).result_rows
    return [{"symbol": r[0], "name": r[0]} for r in rows]

@app.get("/quotes")
def quotes(symbol: str, start: Optional[str] = None):
    """
    Zwraca notowania OHLC dla symbolu od wskazanej daty.
    """
    try:
        dt = date.fromisoformat(start) if start else date(2015, 1, 1)
    except Exception:
        raise HTTPException(400, "start musi być w formacie YYYY-MM-DD")

    ch = get_ch()
    rs = ch.query(
        f"""
        SELECT date, open, high, low, close, volume
        FROM {TABLE_OHLC}
        WHERE symbol = %(s)s AND date >= %(d)s
        ORDER BY date
        """,
        parameters={"s": symbol.upper(), "d": dt},
    ).result_rows

    return [
        {
            "date": r[0].isoformat() if isinstance(r[0], (date, datetime)) else str(r[0]),
            "open": float(r[1]),
            "high": float(r[2]),
            "low": float(r[3]),
            "close": float(r[4]),
            "volume": int(r[5]),
        }
        for r in rs
    ]

# ===========================
# Backtest portfela
# ===========================

RebFreq = Literal["none", "monthly", "quarterly", "yearly"]

def _add_months(d: date, months: int) -> date:
    year = d.year + (d.month - 1 + months) // 12
    month = (d.month - 1 + months) % 12 + 1
    day = min(d.day, [31, 29 if year % 4 == 0 and (year % 100 != 0 or year % 400 == 0) else 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31][month-1])
    return date(year, month, day)

def _next_rebalance(d: date, freq: RebFreq) -> date:
    if freq == "none":
        return date(9999, 12, 31)
    if freq == "monthly":
        return _add_months(d, 1)
    if freq == "quarterly":
        return _add_months(d, 3)
    if freq == "yearly":
        return date(d.year + 1, d.month, d.day)
    return date(9999, 12, 31)

@app.get("/backtest/portfolio")
def backtest_portfolio(
    symbols: str = Query(..., description="CSV symboli"),
    weights: str = Query(..., description="CSV wag w %"),
    start: str = Query(..., description="YYYY-MM-DD"),
    rebalance: RebFreq = Query("monthly"),
):
    """
    Prosty backtest portfela (close-to-close), rebalans wg częstotliwości.
    Zwraca equity curve i podstawowe statystyki.
    """
    syms = [s.strip().upper() for s in symbols.split(",") if s.strip()]
    w = [float(x) for x in weights.split(",")]
    if not syms or len(syms) != len(w):
        raise HTTPException(400, "Nieprawidłowe symbole / wagi")
    w = [max(0.0, x) for x in w]
    sw = sum(w)
    if sw <= 0:
        raise HTTPException(400, "Suma wag musi być > 0")
    w = [x / sw for x in w]  # do 1.0

    try:
        dt0 = date.fromisoformat(start)
    except Exception:
        raise HTTPException(400, "start musi być YYYY-MM-DD")

    # Pobierz close dla każdego symbolu i złącz po wspólnych datach
    ch = get_ch()
    frames = []
    for s in syms:
        rs = ch.query(
            f"""
            SELECT date, close
            FROM {TABLE_OHLC}
            WHERE symbol = %(s)s AND date >= %(d)s
            ORDER BY date
            """,
            parameters={"s": s, "d": dt0},
        ).result_rows
        if not rs:
            continue
        df = pd.DataFrame(rs, columns=["date", s])
        df["date"] = pd.to_datetime(df["date"]).dt.date
        frames.append(df)

    if not frames or len(frames) != len(syms):
        raise HTTPException(404, "Brak wspólnych notowań dla wszystkich symboli")

    df = frames[0]
    for k in range(1, len(frames)):
        df = df.merge(frames[k], on="date", how="inner")

    if df.empty or df.shape[1] < len(syms) + 1:
        raise HTTPException(404, "Brak wspólnych notowań dla wszystkich spółek")

    df = df.sort_values("date").reset_index(drop=True)

    # stopy zwrotu dzienne
    prices = df.set_index("date")
    rets = prices.pct_change().fillna(0.0)

    # symulacja portfela z rebalansem
    equity = []
    val = 1.0
    current_w = pd.Series(w, index=syms)  # target wagi
    alloc = current_w * val
    last_date = df["date"].iloc[0]
    next_reb = _next_rebalance(last_date, rebalance)

    equity.append({"date": last_date.isoformat(), "value": float(val)})

    for d, row in rets.iloc[1:].iterrows():
        # aprecjacja
        alloc = alloc * (1.0 + row[syms])
        val = float(alloc.sum())

        # rebalans na początku dnia 'd' po aprecjacji poprzedniego – realistycznie można robić różnie,
        # tu przyjmujemy prosty wariant kalendarzowy
        if d >= next_reb:
            alloc = current_w * val
            next_reb = _next_rebalance(d, rebalance)

        equity.append({"date": d.isoformat(), "value": float(val)})

    # statystyki
    eq = pd.DataFrame(equity)
    eq["date"] = pd.to_datetime(eq["date"])
    eq = eq.set_index("date").sort_index()
    total_ret = eq["value"].iloc[-1] / eq["value"].iloc[0] - 1.0
    days = (eq.index[-1] - eq.index[0]).days or 1
    years = days / 365.25
    cagr = (eq["value"].iloc[-1]) ** (1 / years) - 1.0 if years > 0 else total_ret

    # max drawdown
    roll_max = eq["value"].cummax()
    dd = eq["value"] / roll_max - 1.0
    max_dd = dd.min() if len(dd) else 0.0

    # zmienność (roczna) i Sharpe (risk-free ~ 0)
    daily_ret = eq["value"].pct_change().dropna()
    vol = float(daily_ret.std() * (252 ** 0.5)) if not daily_ret.empty else 0.0
    sharpe = float((daily_ret.mean() * 252) / vol) if vol > 0 else 0.0

    return {
        "equity": equity,
        "stats": {
            "cagr": float(cagr),
            "max_drawdown": float(max_dd),
            "volatility": float(vol),
            "sharpe": float(sharpe),
            "last_value": float(eq["value"].iloc[-1]),
        },
    }
