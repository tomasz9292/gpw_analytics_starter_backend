from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import zipfile, io, os, pandas as pd
from datetime import datetime

# --- Try ClickHouse; if not configured, fallback to DuckDB ---
USE_CLICKHOUSE = all(os.getenv(k) for k in ["CH_HOST", "CH_PORT", "CH_USER"])
ch_client = None
duck = None

if USE_CLICKHOUSE:
    import clickhouse_connect
else:
    import duckdb

app = FastAPI(title="GPW Analytics API", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True,
    allow_methods=["*"], allow_headers=["*"],
)

@app.get("/ping")
def ping():
    return {"status": "ok", "storage": "clickhouse" if USE_CLICKHOUSE else "duckdb"}

def get_clickhouse():
    global ch_client
    if ch_client is None:
        ch_client = clickhouse_connect.get_client(
            host=os.getenv("CH_HOST"),
            port=int(os.getenv("CH_PORT", "8123")),
            username=os.getenv("CH_USER"),
            password=os.getenv("CH_PASSWORD", ""),
            database=os.getenv("CH_DATABASE", "default"),
        )
        # tabela jeśli brak
        ch_client.command("""
        CREATE TABLE IF NOT EXISTS quotes (
            symbol String,
            date Date,
            open Float64, high Float64, low Float64, close Float64, volume UInt64
        ) ENGINE = MergeTree()
        ORDER BY (symbol, date)
        """)
    return ch_client

def get_duck():
    global duck
    if duck is None:
        # Render ma fs efemeryczny, ale wystarczy do testów; zmień ścieżkę jeśli chcesz
        path = os.getenv("DUCKDB_PATH", "/var/tmp/gpw.duckdb")
        duck = duckdb.connect(path)
        duck.execute("""
            CREATE TABLE IF NOT EXISTS quotes(
                symbol TEXT,
                date DATE,
                open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE, volume BIGINT
            )
        """)
    return duck

def ensure_storage():
    if USE_CLICKHOUSE:
        return "clickhouse", get_clickhouse()
    return "duckdb", get_duck()

def parse_mst(content: bytes) -> pd.DataFrame:
    """
    Parser plików Stooq .mst (z mstall.zip).
    Obsługuje nagłówek w formacie: <TICKER>,<PER>,<DATE>,<TIME>,<OPEN>,<HIGH>,<LOW>,<CLOSE>,<VOL>
    oraz różne delimitery: ',', ';', tab.
    Zwraca kolumny: date, open, high, low, close, volume
    """
    txt = content.decode("utf-8", errors="ignore").lstrip("\ufeff")
    # wybierz delimiter
    if ";" in txt.splitlines()[0]:
        sep = ";"
    elif "\t" in txt.splitlines()[0]:
        sep = "\t"
    else:
        sep = ","

    # Spróbuj z nagłówkiem Stooq
    cols_stooq = ["<TICKER>", "<PER>", "<DATE>", "<TIME>", "<OPEN>", "<HIGH>", "<LOW>", "<CLOSE>", "<VOL>"]
    first_line = txt.splitlines()[0].strip()

    if all(c in first_line for c in ["<DATE>", "<OPEN>", "<CLOSE>"]):
        df = pd.read_csv(io.StringIO(txt), sep=sep)
        # odfiltruj ewentualne puste i wiersze nagłówka powtórzone w środku
        df = df.loc[~df["<DATE>"].astype(str).str.startswith("<")]

        # konwersje
        df["date"] = pd.to_datetime(df["<DATE>"].astype(str), format="%Y%m%d", errors="coerce").dt.date
        df["open"] = pd.to_numeric(df["<OPEN>"], errors="coerce")
        df["high"] = pd.to_numeric(df["<HIGH>"], errors="coerce")
        df["low"]  = pd.to_numeric(df["<LOW>"], errors="coerce")
        df["close"]= pd.to_numeric(df["<CLOSE>"], errors="coerce")
        df["volume"]=pd.to_numeric(df.get("<VOL>", 0), errors="coerce").fillna(0).astype("Int64")

        df = df[["date","open","high","low","close","volume"]].dropna(subset=["date","close"])
        return df.reset_index(drop=True)

    # Fallback: brak nagłówka – 6 kolumn: date, open, high, low, close, volume
    df = pd.read_csv(
        io.StringIO(txt), sep=sep, header=None,
        names=["date","open","high","low","close","volume"],
        engine="python"
    )
    # niektóre pliki mogą mieć w pierwszym wierszu znaczniki w <> — odfiltruj
    df = df.loc[~df["date"].astype(str).str.startswith("<")]

    # data jako %Y%m%d
    df["date"] = pd.to_datetime(df["date"].astype(str), format="%Y%m%d", errors="coerce").dt.date
    for c in ["open","high","low","close","volume"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["volume"] = df["volume"].fillna(0).astype("Int64")

    df = df.dropna(subset=["date","close"])
    return df[["date","open","high","low","close","volume"]].reset_index(drop=True)


@app.post("/ingest/stooq-zip")
async def ingest_stooq_zip(file: UploadFile = File(...)):
    if not file.filename.lower().endswith(".zip"):
        raise HTTPException(400, "Prześlij plik ZIP")

    storage, conn = ensure_storage()

    data = await file.read()
    zf = zipfile.ZipFile(io.BytesIO(data))

    files_total = 0
    files_ok = 0
    files_skipped = 0
    rows_inserted = 0
    errors = []

    for name in zf.namelist():
        if not name.lower().endswith(".mst"):
            continue
        files_total += 1
        try:
            content = zf.read(name)
            df = parse_mst(content)
            if df.empty:
                files_skipped += 1
                continue
            symbol = os.path.splitext(os.path.basename(name))[0].upper()
            df["symbol"] = symbol

            if storage == "clickhouse":
                recs = df[["symbol","date","open","high","low","close","volume"]].values.tolist()
                conn.insert(
                    "quotes", recs,
                    column_names=["symbol","date","open","high","low","close","volume"]
                )
            else:
                # DuckDB – wstawka przez pandas
                conn.execute("BEGIN")
                conn.register("tmp_df", df)
                conn.execute("""
                    INSERT INTO quotes
                    SELECT symbol, date, open, high, low, close, CAST(volume AS BIGINT)
                    FROM tmp_df
                """)
                conn.execute("COMMIT")
                conn.unregister("tmp_df")

            rows_inserted += len(df)
            files_ok += 1
        except Exception as e:
            errors.append(f"{name}: {e}")

    return {
        "storage": storage,
        "files_total": files_total,
        "files_ok": files_ok,
        "files_skipped": files_skipped,
        "rows_inserted": rows_inserted,
        "errors": errors,
        "timestamp": datetime.utcnow().isoformat()+"Z",
    }
