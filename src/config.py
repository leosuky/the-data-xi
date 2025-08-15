import os
from dotenv import load_dotenv
load_dotenv()

POSTGRES = {
    "user": os.getenv("POSTGRES_USER", "dataxi"),
    "password": os.getenv("POSTGRES_PASSWORD", "dataxi"),
    "db": os.getenv("POSTGRES_DB", "dataxi"),
    "host": os.getenv("POSTGRES_HOST", "db"),
    "port": os.getenv("POSTGRES_PORT", "5432"),
}

RAW_DIR    = os.getenv("RAW_DIR", "./Done")
BRONZE_DIR = os.getenv("BRONZE_DIR", "./warehouse/bronze")
SILVER_DIR = os.getenv("SILVER_DIR", "./warehouse/silver")
GOLD_DIR   = os.getenv("GOLD_DIR", "./warehouse/gold")

POSTGRES_JDBC_URL = f"jdbc:postgresql://{POSTGRES['host']}:{POSTGRES['port']}/{POSTGRES['db']}"
