import os
from src.config import RAW_DIR, BRONZE_DIR, SILVER_DIR, GOLD_DIR

def ensure_dirs():
    for p in (BRONZE_DIR, SILVER_DIR, GOLD_DIR):
        os.makedirs(p, exist_ok=True)
