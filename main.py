from fastapi import FastAPI, HTTPException, UploadFile, File, Form
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


# ============================================================
# 1) CRIAR LIGA AUTOMATICAMENTE (painel Base44)
# ============================================================
@app.post("/api/leagues/create")
async def create_league(
    league_name: str = Form(...),
    display_name: str = Form(...)
):
    folder = league_name.lower().replace(" ", "-")

    # Criar registro na tabela
    resp = supabase.table("leagues").insert({
        "league_name": league_name,
        "folder_name": folder,
        "display_name": display_name
    }).execute()

    if "error" in resp:
        raise HTTPException(status_code=400, detail=resp["error"]["message"])

    league_id = resp.data[0]["id"]

    # Criar pasta no Storage
    supabase.storage.from_(BUCKET).upload(
        f"leagues/{folder}/.init",
        b"",
        {"content-type": "text/plain"}
    )

    return {
        "message": "Liga criada com sucesso!",
        "league_id": league_id,
        "folder": folder
    }


# ============================================================
# 2) UPLOAD CSV → cria o time automaticamente
# ============================================================
@app.post("/api/teams/upload")
async def upload_team_csv(
    league_folder: str = Form(...),
    league_id: str = Form(...),
    file: UploadFile = File(...)
):
    file_bytes = await file.read()
    file_name = file.filename
    team_name = file_name.replace(".csv", "")

    # Salvar arquivo no storage
    path = f"leagues/{league_folder}/{file_name}"

    supabase.storage.from_(BUCKET).upload(
        path,
        file_bytes,
        {"content-type": "text/csv"}
    )

    # Criar registro do time na tabela
    supabase.table("teams").insert({
        "league_id": league_id,
        "team_name": team_name,
        "file_name": file_name
    }).execute()

    return {
        "message": "Time criado com sucesso!",
        "team": team_name,
        "file": file_name
    }


# ============================================================
# 3) LISTAR LIGAS (para o Base44 carregar)
# ============================================================
@app.get("/api/leagues")
async def list_leagues():
    resp = supabase.table("leagues").select("*").execute()
    return resp.data


# ============================================================
# 4) LISTAR TIMES DA LIGA
# ============================================================
@app.get("/api/teams/by-league/{league_id}")
async def list_teams(league_id: str):
    resp = supabase.table("teams").select("*").eq("league_id", league_id).execute()
    return resp.data


# ============================================================
# Função interna — carregar CSV do Storage
# ============================================================
def load_csv(league_folder: str, file_name: str):
    path = f"leagues/{league_folder}/{file_name}"
    file_bytes = supabase.storage.from_(BUCKET).download(path)

    if not file_bytes:
        raise HTTPException(status_code=404, detail="CSV não encontrado")

    df = pd.read_csv(io.BytesIO(file_bytes), sep=";")
    return df


# ============================================================
# 5) ROTA H2H (carrega CSV individual)
# ============================================================
@app.get("/api/h2h/{league_id}/{home}/{away}")
async def h2h(league_id: str, home: str, away: str):

    league = supabase.table("leagues").select("*").eq("id", league_id).single().execute()
    folder = league.data["folder_name"]

    home_data = supabase.table("teams").select("*").eq("team_name", home).single().execute()
    away_data = supabase.table("teams").select("*").eq("team_name", away).single().execute()

    df_home = load_csv(folder, home_data.data["file_name"])
    df_away = load_csv(folder, away_data.data["file_name"])

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


# ============================================================
# 6) STATUS
# ============================================================
@app.get("/api/status")
def status():
    return {"status": "online", "message": "Backend conectado (automação total)"}
