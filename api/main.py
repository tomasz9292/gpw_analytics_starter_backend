from fastapi import FastAPI, UploadFile, File, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Iterable, List, Optional
import os, io, csv, zipfile, re
from datetime import date, datetime
import clickhouse_connect

# ---------- Konfiguracja ----------
CH_HOST = os.environ.get("CH_HOST", "clickhouse")
CH_PORT = int(os.environ.get("CH_PORT", "8123"))
CH_USER = os.environ.get("CH_USER", "default")
CH_PASSWORD = os.environ.get("CH_PASSWORD", "")
CH_DB = os.environ.get("CH_DB", "default")

def get_ch():
    client = clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT, username=CH_USER, password=CH_PASSWORD, database=CH_DB
    )
    return client

def ensure_quotes_table():
    ch = get_ch()
    ch.command("""
    CREATE TABLE IF NOT EXISTS quotes
    (
      symbol LowCardinality(String),
      date Date,
      ts DateTime DEFAULT toDateTime(date),
      open Float64,
      high Float64,
      low  Float64,
      close Float64,
      volume UInt64,
      source LowCardinality(String) DEFAULT 'stooq'
    )
    ENGINE = MergeTree
    PARTITION BY toYYYYMM(date)
    ORDER BY (symbol, date)
    SETTINGS index_granularity = 8192
    """)
    ch.close()

ensure_quotes_table()

app = FastAPI(title="GPW Market API (ZIP .mst ingest)")

# ---------- Modele ----------
class Candle(BaseModel):
    date: date
    open: float
    high: float
    low: float
    close: float
    volume: int

# ---------- Utils parsujące .mst ----------
DATE_PAT_YYYYMMDD = re.compile(r"^\d{8}$")
DATE_PAT_ISO = re.compile(r"^\d{4}-\d{2}-\d{2}$")

def parse_date_token(tok: str) -> date:
    tok = tok.strip()
    if DATE_PAT_YYYYMMDD.match(tok):
        # YYYYMMDD
        return date(int(tok[0:4]), int(tok[4:6]), int(tok[6:8]))
    if DATE_PAT_ISO.match(tok):
        return date.fromisoformat(tok)
    # spróbuj automatycznie
    try:
        return datetime.strptime(tok, "%Y%m%d").date()
    except:
        try:
            return datetime.strptime(tok, "%Y-%m-%d").date()
        except:
            raise ValueError(f"Nieznany format daty: {tok}")

def detect_delimiter(sample: str) -> str:
    # jeśli w próbce więcej średników niż przecinków – użyj ';'
    semi = sample.count(';')
    comma = sample.count(',')
    return ';' if semi > comma else ','

def iter_mst_rows(fh: io.TextIOBase, delimiter: str) -> Iterable[list]:
    """Zwraca wiersze (listy) z pliku .mst (pomija puste i komentarze)."""
    rdr = csv.reader(fh, delimiter=delimiter)
    for row in rdr:
        if not row: 
            continue
        # stooq często ma trailing commas – oczyść z pustych końcówek
        while len(row) and row[-1] == '':
            row.pop()
        # linie komentowane (rzadko) – pomiń
        if row and str(row[0]).startswith('#'):
            continue
        yield row

def parse_single_mst_bytes(data: bytes, symbol_from_name: str) -> List[tuple]:
    """
    Czyta body .mst -> list[tuple(symbol, date, open, high, low, close, volume, source)]
    Zakłada kolumny: date, open, high, low, close, volume, (opcjonalnie openint)
    """
    # wykryj delimiter na podstawie nagłówka/próbki
    head = data[:4096].decode("utf-8", "ignore")
    delim = detect_delimiter(head)

    # niektóre paczki mają nagłówek – spróbuj go wykryć
    has_header = False
    first_line = head.splitlines()[0] if head.splitlines() else ""
    header_like = ["date", "open", "high", "low", "close", "volume"]
    if any(h in first_line.lower() for h in header_like):
        has_header = True

    fh = io.StringIO(data.decode("utf-8", "ignore"))
    rdr = iter_mst_rows(fh, delimiter=delim)

    rows: List[tuple] = []
    # jeśli jest nagłówek, zrzuć pierwszy rekord
    if has_header:
        try:
            next(rdr)
        except StopIteration:
            return rows

    for cols in rdr:
        # akceptuj 6 lub 7 kolumn (ostatnia open interest ignorowana)
        if len(cols) < 6:
            # jeśli kolumn mniej – spróbuj jeszcze raz (np. spacje)
            cols = [c for c in cols if c != ""]
            if len(cols) < 6:
                continue
        dt = parse_date_token(cols[0])
        o = float(cols[1]); h = float(cols[2]); l = float(cols[3]); c = float(cols[4])
        v = int(float(cols[5])) if cols[5] != '' else 0
        rows.append((symbol_from_name, dt, o, h, l, c, v, "stooq"))
    return rows

# ---------- Ingest ZIP .mst ----------
@app.post("/ingest/stooq-zip")
async def ingest_stooq_zip(file: UploadFile = File(...), batch_size: int = 50000):
    """
    Przyjmij ZIP z plikami .mst (Stooq/MetaStock), wyodrębnij i załaduj do ClickHouse.
    Nazwa pliku .mst = symbol, np. CDR.WA.mst -> symbol=C SDR.WA
    """
    if not file.filename.lower().endswith(".zip"):
        raise HTTPException(400, detail="Wyślij plik ZIP (z wieloma *.mst)")

    raw = await file.read()
    zf = zipfile.ZipFile(io.BytesIO(raw))
    names = [n for n in zf.namelist() if n.lower().endswith(".mst")]

    if not names:
        raise HTTPException(400, detail="Brak *.mst w przesłanym ZIP.")

    ch = get_ch()
    inserted_total = 0
    files_ok = 0
    files_skipped = 0
    errors: list[dict] = []

    for name in names:
        # symbol = nazwa pliku bez ścieżki i rozszerzenia
        base = os.path.basename(name)
        sym = re.sub(r"\.mst$", "", base, flags=re.IGNORECASE)

        try:
            data = zf.read(name)
            rows = parse_single_mst_bytes(data, sym)
            if not rows:
                files_skipped += 1
                continue

            # batch insert
            for i in range(0, len(rows), batch_size):
                chunk = rows[i:i + batch_size]
                ch.insert(
                    "quotes",
                    chunk,
                    column_names=["symbol", "date", "open", "high", "low", "close", "volume", "source"],
                )
            inserted_total += len(rows)
            files_ok += 1
        except Exception as e:
            errors.append({"file": name, "error": str(e)})
            files_skipped += 1

    ch.close()
    return {
        "files_total": len(names),
        "files_ok": files_ok,
        "files_skipped": files_skipped,
        "rows_inserted": inserted_total,
        "errors": errors[:10],  # pokaż pierwsze 10 błędów, jeśli są
    }

# ---------- API do frontu ----------
@app.get("/")
def root():
    return {"ok": True, "msg": "GPW backend. Użyj /ingest/stooq-zip, /quotes, /symbols, /health."}

@app.get("/health")
def health():
    try:
        ch = get_ch(); ch.query("SELECT 1"); ch.close()
        return {"status": "ok"}
    except Exception as e:
        raise HTTPException(500, detail=f"ClickHouse error: {e}")

@app.get('/quotes')
def quotes(symbol: str, start: str = '2000-01-01', end: Optional[str] = None):
    if end is None:
        end = date.today().isoformat()
    ch = get_ch()
    rows = ch.query("""
        SELECT date, open, high, low, close, volume
        FROM quotes
        WHERE symbol = %(s)s AND date BETWEEN %(a)s AND %(b)s
        ORDER BY date
    """, {"s": symbol, "a": start, "b": end}).result_rows
    ch.close()
    return [{"date": r[0].isoformat(), "open": r[1], "high": r[2], "low": r[3], "close": r[4], "volume": int(r[5])} for r in rows]

# Jeżeli używasz Postgresa na symbole – zastąp to zapytaniem do PG.
# Tymczasowo zwrócimy listę rozpoznanych symboli (z ClickHouse) z tabeli quotes:
@app.get('/symbols')
def symbols(q: Optional[str] = None, limit: int = 50):
    ch = get_ch()
    if q:
        rows = ch.query("""
            SELECT DISTINCT symbol
            FROM quotes
            WHERE like(symbol, %(p)s)
            ORDER BY symbol
            LIMIT %(lim)s
        """, {"p": f"%{q.upper()}%", "lim": limit}).result_rows
    else:
        rows = ch.query("""
            SELECT DISTINCT symbol
            FROM quotes
            ORDER BY symbol
            LIMIT %(lim)s
        """, {"lim": limit}).result_rows
    ch.close()
    return [{"symbol": r[0], "name": r[0]} for r in rows]
