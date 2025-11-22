from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from supabase import create_client, Client
import pandas as pd
import io
import os

app = FastAPI()

# ======================================
# CONFIG SUPABASE
# ======================================
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise Exception("Variáveis SUPABASE_URL e SUPABASE_KEY não foram definidas.")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

BUCKET = "h2h-data"

# ======================================
# CORS
# ======================================
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ======================================
# LISTAR LIGAS (VERSÃO BLINDADA)
# ======================================
@app.get("/api/leagues")
async def list_leagues():
    try:
        # Lista TUDO no bucket
        folders = supabase.storage.from_(BUCKET).list("")

        leagues = []
        for f in folders:
            name = f.get("name", "")

            # se NÃO tiver ponto → é pasta → é liga
            if "." not in name:
                leagues.append(name)

        return {"leagues": leagues}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ======================================
# LISTAR TIMES DE UMA LIGA
# ======================================
@app.get("/api/teams/{league}")
async def list_teams(league: str):
    try:
        path = f"{league}/"
        files = supabase.storage.from_(BUCKET).list(path)

        teams = []
        for f in files:
            name = f.get("name", "")
            if name.endswith(".csv"):
                teams.append(name.replace(".csv", ""))

        return {"league": league, "teams": teams}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ======================================
# CARREGAR CSV DE UM TIME
# ======================================
def load_team_csv(league: str, team: str):
    path = f"{league}/{team}.csv"
    data = supabase.storage.from_(BUCKET).download(path)

    if not data:
        raise HTTPException(status_code=404, detail=f"CSV não encontrado: {path}")

    df = pd.read_csv(io.BytesIO(data), sep=";")
    return df


# ======================================
# ENDPOINT H2H
# ======================================
@app.get("/api/h2h/{league}/{home}/{away}")
async def h2h(league: str, home: str, away: str):
    try:
        df_home = load_team_csv(league, home)
        df_away = load_team_csv(league, away)

        stats = {
            "home": {
                "team": home,
                "avg_goals_ft": float(df_home["avg_goals_ft"].iloc[0]),
                "avg_goals_ht": float(df_home["avg_goals_ht"].iloc[0]),
            },
            "away": {
                "team": away,
                "avg_goals_ft": float(df_away["avg_goals_ft"].iloc[0]),
                "avg_goals_ht": float(df_away["avg_goals_ht"].iloc[0]),
            }
        }

        return stats

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ======================================
# DEBUG STORAGE (MOSTRA O QUE O SUPABASE ESTÁ DEVOLVENDO)
# ======================================
@app.get("/api/debug/storage")
def debug_storage():
    try:
        folders = supabase.storage.from_(BUCKET).list("")
        return folders
    except Exception as e:
        return {"error": str(e)}


# ======================================
# STATUS
# ======================================
@app.get("/api/status")
def status():
    return {"status": "online", "message": "Backend Base44 conectado"}
