import io
import os
import zipfile
from datetime import date, datetime
from typing import List, Literal, Optional, Tuple

import duckdb
import numpy as np
import pandas as pd
from fastapi import FastAPI, File, HTTPException, Query, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# ──────────────────────────────────────────────────────────────────────────────
# Konfiguracja bazy (DuckDB na dysku). Render ma RW w /opt/render/project/src/
# ──────────────────────────────────────────────────────────────────────────────
DATA_DIR = os.environ.get("DATA_DIR", "./data")
os.makedirs(DATA_DIR, exist_ok=True)
DUCK_PATH = os.path.join(DATA_DIR, "duck.db")

def get_duck() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(DUCK_PATH, read_only=False)
    con.execute("""
        CREATE TABLE IF NOT EXISTS candles (
            symbol VARCHAR,
            d DATE,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            volume BIGINT
        );
    """)
    # Dla szybkich zapytań
    con.execute("CREATE INDEX IF NOT EXISTS idx_candles_symbol_date ON candles(symbol, d);")
    return con

# ──────────────────────────────────────────────────────────────────────────────
# FastAPI + CORS
# ──────────────────────────────────────────────────────────────────────────────
app = FastAPI(title="GPW Analytics API", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # w razie czego zawęź do swojego frontu
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ──────────────────────────────────────────────────────────────────────────────
# Pomocnicze: parser plików .mst ze Stooq (YYYYMMDD;O;H;L;C;V)
# ──────────────────────────────────────────────────────────────────────────────
def parse_mst_bytes(sym: str, raw: bytes) -> pd.DataFrame:
    """
    Minimalny parser formatu *.mst (Stooq).
    Zakładamy kolumny w kolejności:
      DATE ; OPEN ; HIGH ; LOW ; CLOSE ; VOLUME
    gdzie DATE to YYYYMMDD (int/str).
    Czasem pierwszy wiersz bywa nagłówkiem — filtrujemy wszystko,
    co nie wygląda na 8-znakową datę.
    """
    text = raw.decode("utf-8", errors="ignore").replace("\r", "")
    lines = [ln for ln in text.split("\n") if ln.strip()]
    rows: List[Tuple[str, float, float, float, float, int]] = []
    for ln in lines:
        parts = [p.strip() for p in ln.split(";")]
        if len(parts) < 6:
            continue
        d_raw = parts[0]
        # tylko daty 8-znakowe
        if not (len(d_raw) == 8 and d_raw.isdigit()):
            continue
        try:
            d = datetime.strptime(d_raw, "%Y%m%d").date()
            o = float(parts[1]); h = float(parts[2]); l = float(parts[3]); c = float(parts[4])
            v = int(float(parts[5]))
            rows.append((sym, d, o, h, l, c, v))
        except Exception:
            # ignoruj rzędy z błędami
            continue
    if not rows:
        return pd.DataFrame(columns=["symbol", "d", "open", "high", "low", "close", "volume"])
    df = pd.DataFrame(rows, columns=["symbol", "d", "open", "high", "low", "close", "volume"])
    # usuwamy duplikaty tej samej daty
    df = df.sort_values("d").drop_duplicates(subset=["symbol", "d"], keep="last")
    return df

# ──────────────────────────────────────────────────────────────────────────────
# Upload ZIP z plikami *.mst i ingest do DuckDB
# ──────────────────────────────────────────────────────────────────────────────
class IngestResult(BaseModel):
    files_seen: int
    files_loaded: int
    rows_inserted: int

@app.post("/ingest/stooq-zip", response_model=IngestResult, summary="Wgraj ZIP z plikami *.mst")
async def ingest_stooq_zip(file: UploadFile = File(...)):
    if not file.filename.lower().endswith(".zip"):
        raise HTTPException(status_code=400, detail="Podaj plik ZIP.")

    data = await file.read()
    try:
        zf = zipfile.ZipFile(io.BytesIO(data))
    except zipfile.BadZipFile:
        raise HTTPException(status_code=400, detail="Uszkodzony ZIP.")

    con = get_duck()
    files_seen = 0
    files_loaded = 0
    rows_inserted = 0

    for name in zf.namelist():
        if not name.lower().endswith(".mst"):
            continue
        files_seen += 1
        sym = os.path.basename(name).split(".")[0].upper()

        try:
            raw = zf.read(name)
            df = parse_mst_bytes(sym, raw)
            if df.empty:
                continue

            # Insert — aby uniknąć duplikatów, kasujemy istniejące rzędy dla (symbol, d) które wstawiamy
            # (DuckDB nie ma INSERT OR REPLACE – robimy MERGE przez temp table)
            con.execute("CREATE TEMP TABLE tmp_load AS SELECT * FROM df", {"df": df})
            con.execute("""
                DELETE FROM candles
                USING tmp_load
                WHERE candles.symbol = tmp_load.symbol AND candles.d = tmp_load.d
            """)
            con.execute("INSERT INTO candles SELECT * FROM tmp_load")
            ins = con.execute("SELECT COUNT(*) FROM tmp_load").fetchone()[0]
            rows_inserted += int(ins)
            con.execute("DROP TABLE tmp_load")
            files_loaded += 1
        except Exception:
            # błąd konkretnego pliku – ignorujemy, lecimy dalej
            continue

    return IngestResult(files_seen=files_seen, files_loaded=files_loaded, rows_inserted=rows_inserted)

# ──────────────────────────────────────────────────────────────────────────────
# Endpointy dla frontu
# ──────────────────────────────────────────────────────────────────────────────
class QuoteRow(BaseModel):
    date: date
    open: float
    high: float
    low: float
    close: float
    volume: int

@app.get("/quotes", response_model=List[QuoteRow], summary="Notowania 1D dla spółki")
def quotes(
    symbol: str = Query(..., description="Ticker, np. CDR.WA"),
    start: Optional[date] = Query(None, description="Początek zakresu (YYYY-MM-DD)")
):
    con = get_duck()
    if start:
        q = """
            SELECT d AS date, open, high, low, close, volume
            FROM candles
            WHERE symbol = ? AND d >= ?
            ORDER BY d
        """
        df = con.execute(q, [symbol.upper(), start]).df()
    else:
        q = """
            SELECT d AS date, open, high, low, close, volume
            FROM candles
            WHERE symbol = ?
            ORDER BY d
        """
        df = con.execute(q, [symbol.upper()]).df()

    # Konwersje typów pod Pydantic
    out = [
        QuoteRow(
            date=pd.to_datetime(r["date"]).date(),
            open=float(r["open"]),
            high=float(r["high"]),
            low=float(r["low"]),
            close=float(r["close"]),
            volume=int(r["volume"]),
        )
        for _, r in df.iterrows()
    ]
    return out

class SymbolRow(BaseModel):
    symbol: str
    name: str

@app.get("/symbols", response_model=List[SymbolRow], summary="Lista tickerów (z filtem q=)")
def symbols(q: Optional[str] = Query(None, description="Prefiks lub fragment, np. 'PK'")):
    con = get_duck()
    if q:
        df = con.execute(
            "SELECT DISTINCT symbol FROM candles WHERE symbol ILIKE ? ORDER BY symbol",
            [f"%{q.upper()}%"],
        ).df()
    else:
        df = con.execute("SELECT DISTINCT symbol FROM candles ORDER BY symbol").df()

    return [SymbolRow(symbol=s, name=s) for s in df["symbol"].tolist()]

# ──────────────────────────────────────────────────────────────────────────────
# Backtest portfela (equity + podstawowe statystyki)
# ──────────────────────────────────────────────────────────────────────────────
class PortfolioPoint(BaseModel):
    date: date
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

def _rebal_dates(dates: pd.DatetimeIndex, freq: Literal["none","monthly","quarterly","yearly"]) -> pd.Index:
    if freq == "none":
        return pd.Index([])
    if freq == "monthly":
        return dates.to_period("M").drop_duplicates().to_timestamp(how="end")
    if freq == "quarterly":
        return dates.to_period("Q").drop_duplicates().to_timestamp(how="end")
    if freq == "yearly":
        return dates.to_period("Y").drop_duplicates().to_timestamp(how="end")
    return pd.Index([])

@app.get("/backtest/portfolio", response_model=PortfolioResp, summary="Backtest portfela")
def backtest_portfolio(
    symbols: str = Query(..., description="CSV tickerów, np. CDR.WA,PKO.WA"),
    weights: str = Query(..., description="CSV wag w %, np. 40,30,30"),
    start: date = Query(..., description="Start (YYYY-MM-DD)"),
    rebalance: Literal["none", "monthly", "quarterly", "yearly"] = Query("monthly")
):
    syms = [s.strip().upper() for s in symbols.split(",") if s.strip()]
    w_raw = [float(x) for x in weights.split(",")]
    if len(syms) == 0 or len(syms) != len(w_raw):
        raise HTTPException(status_code=400, detail="Liczba symboli musi odpowiadać liczbie wag.")
    w = np.array(w_raw, dtype=float)
    if w.sum() <= 0:
        raise HTTPException(status_code=400, detail="Wagi muszą być dodatnie.")
    w = w / w.sum()  # normalizacja do 1.0

    # pobierz zamknięcia, utnij do wspólnych dat
    con = get_duck()
    frames = []
    for s in syms:
        df = con.execute(
            "SELECT d AS date, close FROM candles WHERE symbol=? AND d>=? ORDER BY d",
            [s, start],
        ).df()
        if df.empty:
            raise HTTPException(status_code=404, detail=f"Brak danych dla {s}")
        df["date"] = pd.to_datetime(df["date"])
        df = df.set_index("date").rename(columns={"close": s})
        frames.append(df)

    # join po wspólnym indeksie
    mat = pd.concat(frames, axis=1, join="inner").sort_index()
    if mat.empty or len(mat.columns) != len(syms):
        raise HTTPException(status_code=404, detail="Brak wspólnych notowań dla wszystkich spółek.")

    # dzienne stopy zwrotu
    rets = mat.pct_change().fillna(0.0)
    dates = rets.index

    # rebalancing
    rebal_on = set(_rebal_dates(dates, rebalance))
    port_val = [1.0]  # start = 1.0
    cur_w = w.copy()

    for i in range(1, len(rets)):
        r_vec = rets.iloc[i].values  # dzienny zwrot każdej spółki
        pv_prev = port_val[-1]
        pv_next = pv_prev * float(1.0 + np.dot(cur_w, r_vec))
        port_val.append(pv_next)

        # rebalansujemy na koniec okresu (po naliczeniu)
        dt = dates[i]
        if dt.normalize().to_pydatetime() in rebal_on:
            cur_w = w.copy()

        else:
            # dryf wag (po wzroście/spadku)
            # nowe "udziały" = w_t * (1+r_i) i normalizacja
            grown = cur_w * (1.0 + r_vec)
            s = grown.sum()
            if s > 0:
                cur_w = grown / s
            else:
                cur_w = w.copy()

    equity = pd.Series(port_val, index=dates)

    # statystyki
    yrs = (equity.index[-1] - equity.index[0]).days / 365.25
    last_val = float(equity.iloc[-1])
    cagr = (last_val ** (1 / yrs) - 1.0) if yrs > 0 else 0.0
    dd = (equity / equity.cummax() - 1.0).min()
    daily_std = rets.dot(w).std()
    vol = float(daily_std * np.sqrt(252))
    sharpe = float((cagr - 0.0) / vol) if vol > 1e-9 else 0.0

    resp = PortfolioResp(
        equity=[PortfolioPoint(date=d.to_pydatetime().date(), value=float(v)) for d, v in equity.items()],
        stats=PortfolioStats(
            cagr=float(cagr),
            max_drawdown=float(abs(dd)),
            volatility=float(vol),
            sharpe=float(sharpe),
            last_value=last_val,
        ),
    )
    return resp

# ──────────────────────────────────────────────────────────────────────────────
# Prosty ping
# ──────────────────────────────────────────────────────────────────────────────
@app.get("/ping")
def ping():
    return {"status": "ok", "storage": "duckdb", "db_path": DUCK_PATH}
