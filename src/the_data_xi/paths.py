import os

RAW_DIR   = os.getenv("RAW_DIR", "./Done")
BRONZE_DIR = os.getenv("BRONZE_DIR", "./warehouse/bronze")
SILVER_DIR = os.getenv("SILVER_DIR", "./warehouse/silver")
GOLD_DIR   = os.getenv("GOLD_DIR", "./warehouse/gold")
