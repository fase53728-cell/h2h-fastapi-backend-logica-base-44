from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from supabase import create_client, Client
import pandas as pd
import io
import os

app = FastAPI()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

BUCKET = "h2h-data"

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# === LISTAR LIGAS (PASTAS DIRETAS NO BUCKET) ===
@app.get("/api/leagues")
async def list_leagues():
    try:
        result = supabase.storage.from_(BUCKET).list("")
        leagues = [item["name"] for item in result if item.get("metadata") is None]
        return {"leagues": leagues}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# === LISTAR TIMES (ARQUIVOS DENTRO DA LIGA) ===
@app.get("/api/teams/{league}")
async def list_teams(league: str):
    try:
        result = supabase.storage.from_(BUCKET).list(f"{league}/")

        teams = [
            item["name"].replace(".csv", "")
            for item in result
            if item["name"].endswith(".csv")
        ]

        return {"league": league, "teams": teams}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# === CARREGAR CSV DO TIME ===
def load_team_csv(league: str, team: str):
    path = f"{league}/{team}.csv"
    data = supabase.storage.from_(BUCKET).download(path)
    if not data:
        raise HTTPException(status_code=404, detail=f"CSV não encontrado: {path}")

    df = pd.read_csv(io.BytesIO(data), sep=";")
    return df

@app.get("/api/status")
def status():
    return {"status": "online"}
