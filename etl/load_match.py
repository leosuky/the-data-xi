import os
import json
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Load env variables
load_dotenv()

PG_HOST = os.getenv("POSTGRES_HOST")
PG_PORT = os.getenv("POSTGRES_PORT")
PG_USER = os.getenv("POSTGRES_USER")
PG_PASS = os.getenv("POSTGRES_PASSWORD")
PG_DB   = os.getenv("POSTGRES_DB")

# Connect to Postgres
engine = create_engine(f"postgresql://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}")

# === Step 1: Read raw JSON (replace with one of your files) ===
with open("data/raw/sample_match.json", "r") as f:
    match_data = json.load(f)

# Example: parse players
players = []
for side in ["home", "away"]:
    for p in match_data["lineups"][side]["players"]:
        players.append({
            "player_id": p["player"]["id"],
            "name": p["player"]["name"],
            "position": p["position"],
            "nationality": p["player"]["country"]["name"],
            "birth_date": pd.to_datetime(p["player"]["dateOfBirthTimestamp"], unit="s")
        })

players_df = pd.DataFrame(players)

# === Step 2: Write to Postgres ===
players_df.to_sql("players", engine, if_exists="append", index=False)

print("✅ Loaded players into Postgres!")
