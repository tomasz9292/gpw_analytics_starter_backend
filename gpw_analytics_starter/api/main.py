from fastapi import FastAPI

app = FastAPI(title="GPW Analytics API")

@app.get("/ping")
def ping():
    return {"status": "ok"}
