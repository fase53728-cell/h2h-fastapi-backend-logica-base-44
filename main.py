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
# FUNÇÃO: LISTAR LIGAS
# ======================================
@app.get("/api/leagues")
async def list_leagues():
    try:
        leagues = supabase.storage.from_(BUCKET).list("leagues/")
        return {"leagues": [l["name"] for l in leagues]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ======================================
# FUNÇÃO: LISTAR TIMES DE UMA LIGA
# ======================================
@app.get("/api/teams/{league}")
async def list_teams(league: str):
    try:
        path = f"leagues/{league}/"
        files = supabase.storage.from_(BUCKET).list(path)

        teams = []
        for f in files:
            name = f["name"].replace(".csv", "")
            teams.append(name)

        return {"league": league, "teams": teams}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ======================================
# FUNÇÃO INTERNA — CARREGAR CSV
# ======================================
def load_team_csv(league: str, team: str):
    path = f"leagues/{league}/{team}.csv"

    data = supabase.storage.from_(BUCKET).download(path)
    if not data:
        raise HTTPException(status_code=404, detail="CSV não encontrado")

    df = pd.read_csv(io.BytesIO(data), sep=";")
    return df


# ======================================
# ROTA H2H
# ======================================
@app.get("/api/h2h/{league}/{home}/{away}")
async def h2h(league: str, home: str, away: str):
    try:
        df_home = load_team_csv(league, home)
        df_away = load_team_csv(league, away)

        # Exemplo básico para o Base44
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
# DIAGNÓSTICO
# ======================================
@app.get("/api/status")
def status():
    return {"status": "online", "message": "Backend Base44 conectado"}
