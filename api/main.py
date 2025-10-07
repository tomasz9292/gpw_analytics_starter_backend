from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import zipfile, io, pandas as pd, os, clickhouse_connect
from datetime import datetime

app = FastAPI(title="GPW Analytics API")

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True,
    allow_methods=["*"], allow_headers=["*"],
)

@app.get("/ping")
def ping():
    return {"status": "ok"}

# Konfiguracja ClickHouse
CH_HOST = os.getenv("CH_HOST", "localhost")
CH_PORT = int(os.getenv("CH_PORT", "8123"))
CH_USER = os.getenv("CH_USER", "default")
CH_PASSWORD = os.getenv("CH_PASSWORD", "")

def get_ch():
    return clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT, username=CH_USER, password=CH_PASSWORD
    )

# 📦 Endpoint do uploadu i rozpakowania ZIP-a
@app.post("/ingest/stooq-zip")
async def ingest_stooq_zip(file: UploadFile = File(...)):
    if not file.filename.lower().endswith(".zip"):
        raise HTTPException(400, "Prześlij plik ZIP")

    data = await file.read()
    zf = zipfile.ZipFile(io.BytesIO(data))

    ch = get_ch()
    files_total = 0
    files_ok = 0
    files_skipped = 0
    rows_inserted = 0
    errors = []

    def parse_mst(content: bytes):
        try:
            df = pd.read_csv(io.BytesIO(content))
        except Exception:
            df = pd.read_csv(io.BytesIO(content), header=None,
                             names=["date","open","high","low","close","volume"])
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"]).dt.date
        return df[["date","open","high","low","close","volume"]]

    for name in zf.namelist():
        if not name.lower().endswith(".mst"):
            files_skipped += 1
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
            recs = df[["symbol","date","open","high","low","close","volume"]].values.tolist()
            ch.insert("quotes", recs, column_names=["symbol","date","open","high","low","close","volume"])
            rows_inserted += len(recs)
            files_ok += 1
        except Exception as e:
            errors.append(f"{name}: {e}")

    return {
        "files_total": files_total,
        "files_ok": files_ok,
        "files_skipped": files_skipped,
        "rows_inserted": rows_inserted,
        "errors": errors,
        "timestamp": datetime.utcnow().isoformat()+"Z",
    }
