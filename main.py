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


# ===============================================
# 1) CRIAR LIGA AUTOMATICAMENTE
# ===============================================
@app.post("/api/leagues/create")
async def create_league(league_name: str = Form(...), display_name: str = Form(...)):
    folder = league_name.lower().replace(" ", "-")

    # 1) Criar registro na tabela 'leagues'
    data = {
        "league_name": league_name,
        "folder_name": folder,
        "display_name": display_name,
    }

    resp = supabase.table("leagues").insert(data).execute()
    if resp.get("status_code") == 400:
        raise HTTPException(status_code=400, detail=resp["msg"])

    # 2) Criar pasta no Storage
    supabase.storage.from_(BUCKET).upload(
        f"leagues/{folder}/.init", b"", {"content-type": "text/plain"}
    )

    return {
        "message": "Liga criada com sucesso",
        "league_id": resp.data[0]["id"],
        "folder": folder
    }


# ===============================================
# 2) UPLOAD DE CSV → CRIA TIME AUTOMATICAMENTE
# ===============================================
@app.post("/api/teams/upload")
async def upload_team_csv(
    league_folder: str = Form(...),
    league_id: str = Form(...),
    file: UploadFile = File(...)
):

    file_content = await file.read()

    # Nome do time baseado no nome do arquivo
    file_name = file.filename
    team_name = file_name.replace(".csv", "")

    # 1) SALVAR CSV no Supabase Storage
    storage_path = f"leagues/{league_folder}/{file_name}"

    supabase.storage.from_(BUCKET).upload(
        storage_path,
        file_content,
        {"content-type": "text/csv"}
    )

    # 2) CRIAR TIME NA TABELA
    data = {
        "league_id": league_id,
        "team_name": team_name,
        "file_name": file_name
    }

    supabase.table("teams").insert(data).execute()

    return {
        "message": "Time criado automaticamente",
        "team": team_name,
        "file": file_name,
        "storage_path": storage_path
    }


# ======================================
# LISTAR LIGAS
# ======================================
@app.get("/api/leagues")
async def list_leagues():
    resp = supabase.table("leagues").select("*").execute()
    return resp.data


# ======================================
# LISTAR TIMES DE UMA LIGA
# ======================================
@app.get("/api/teams/by-league/{league_id}")
async def list_teams(league_id: str):
    resp = supabase.table("teams").select("*").eq("league_id", league_id).execute()
    return resp.data


# ======================================
# FUNÇÃO INTERNA — CARREGAR CSV
# ======================================
def load_team_csv(league_folder: str, file_name: str):
    path = f"leagues/{league_folder}/{file_name}"

    data = supabase.storage.from_(BUCKET).download(path)
    if not data:
        raise HTTPException(status_code=404, detail="CSV não encontrado")

    df = pd.read_csv(io.BytesIO(data), sep=";")
    return df


# ======================================
# H2H
# ======================================
@app.get("/api/h2h/{league_id}/{home}/{away}")
async def h2h(league_id: str, home: str, away: str):

    # Buscar liga
    league = supabase.table("leagues").select("*").eq("id", league_id).single().execute()
    folder = league.data["folder_name"]

    # Buscar times
    t1 = supabase.table("teams").select("*").eq("team_name", home).single().execute()
    t2 = supabase.table("teams").select("*").eq("team_name", away).single().execute()

    df_home = load_team_csv(folder, t1.data["file_name"])
    df_away = load_team_csv(folder, t2.data["file_name"])

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


# ======================================
# STATUS
# ======================================
@app.get("/api/status")
def status():
    return {"status": "online", "message": "Backend Base44 conectado com automação total"}
