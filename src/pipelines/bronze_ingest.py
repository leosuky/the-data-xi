import os
from pyspark.sql import functions as F
from src.config import RAW_DIR, BRONZE_DIR
from src.the_data_xi.spark_session import get_spark
from src.the_data_xi.paths import ensure_dirs
from src.the_data_xi.utils import add_path_partitions

"""
Bronze ingest strategy:
- Sweep raw folder for known file patterns
- Read JSON/CSV as semi-structured
- Add partition columns from path (tournament/season/gameweek/combo_id)
- Write to Parquet datasets per 'domain' (statistics, lineups, heatmaps, misc)
- Append-only (idempotent via overwrite by partition on dev runs if needed)
"""

def _write(df, domain: str):
    if df is None: 
        return
    if not df.take(1):
        return
    (
        df.withColumn("ingested_at", F.current_timestamp())
          .write.mode("append")
          .partitionBy("tournament", "season", "gameweek")
          .parquet(os.path.join(BRONZE_DIR, domain))
    )
    print(f"✔ {domain} → bronze")


def main():
    ensure_dirs()
    spark = get_spark("TDXI_BronzeIngest")

    # 1) Match statistics
    stats_glob = os.path.join(RAW_DIR, "*", "*", "*", "*", "event-*-statistics.json")
    stats = spark.read.option("multiLine", "true").json(stats_glob)
    _write(add_path_partitions(stats), "statistics")

    # 2) Lineups
    lineups_glob = os.path.join(RAW_DIR, "*", "*", "*", "*", "event-*-lineups.json")
    lineups = spark.read.option("multiLine", "true").json(lineups_glob)
    _write(add_path_partitions(lineups), "lineups")

    # 3) Heatmaps
    heat_glob = os.path.join(RAW_DIR, "*", "*", "*", "*", "event-*-heatmap-*.json")
    heat = spark.read.option("multiLine", "true").json(heat_glob)
    _write(add_path_partitions(heat), "heatmaps")

    # 4) CSV (fbref misc)
    csv_glob = os.path.join(RAW_DIR, "*", "*", "*", "*", "*.csv")
    csv = spark.read.option("header", "true").csv(csv_glob)
    _write(add_path_partitions(csv), "csv_misc")

    print("✅ Bronze ingest complete")

if __name__ == "__main__":
    main()