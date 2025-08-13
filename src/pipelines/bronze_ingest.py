import os
from pyspark.sql import functions as F
from the_data_xi.spark_session import get_spark
from the_data_xi.paths import RAW_DIR, BRONZE_DIR
from the_data_xi.utils import add_path_partitions

"""
Bronze ingest strategy:
- Sweep raw folder for known file patterns
- Read JSON/CSV as semi-structured
- Add partition columns from path (tournament/season/gameweek/combo_id)
- Write to Parquet datasets per 'domain' (statistics, lineups, heatmaps, misc)
- Append-only (idempotent via overwrite by partition on dev runs if needed)
"""

def main():
    spark = get_spark("TDXI_BronzeIngest")

    # === 1) Match statistics JSON ===
    stats_glob = os.path.join(RAW_DIR, "*", "*", "*", "*", "event-*-statistics.json")
    stats = spark.read.option("multiLine", "true").json(stats_glob)
    if stats.head(1):
        stats = add_path_partitions(stats)
        (stats
         .withColumn("ingested_at", F.current_timestamp())
         .write
         .mode("append")
         .partitionBy("tournament", "season", "gameweek")
         .parquet(os.path.join(BRONZE_DIR, "statistics")))
        print("✔ statistics → bronze")

    # === 2) Lineups JSON ===
    lineups_glob = os.path.join(RAW_DIR, "*", "*", "*", "*", "event-*-lineups.json")
    lineups = spark.read.option("multiLine", "true").json(lineups_glob)
    if lineups.head(1):
        lineups = add_path_partitions(lineups)
        (lineups
         .withColumn("ingested_at", F.current_timestamp())
         .write
         .mode("append")
         .partitionBy("tournament", "season", "gameweek")
         .parquet(os.path.join(BRONZE_DIR, "lineups")))
        print("✔ lineups → bronze")

    # === 3) Heatmaps JSON (per-team/per-player heatmaps) ===
    heat_glob = os.path.join(RAW_DIR, "*", "*", "*", "*", "event-*-heatmap-*.json")
    heat = spark.read.option("multiLine", "true").json(heat_glob)
    if heat.head(1):
        heat = add_path_partitions(heat)
        (heat
         .withColumn("ingested_at", F.current_timestamp())
         .write
         .mode("append")
         .partitionBy("tournament", "season", "gameweek")
         .parquet(os.path.join(BRONZE_DIR, "heatmaps")))
        print("✔ heatmaps → bronze")

    # === 4) CSVs (fbref or others) — catch-all ===
    csv_glob = os.path.join(RAW_DIR, "*", "*", "*", "*", "*.csv")
    csv = spark.read.option("header", "true").csv(csv_glob)
    if csv.head(1):
        csv = add_path_partitions(csv)
        (csv
         .withColumn("ingested_at", F.current_timestamp())
         .write
         .mode("append")
         .partitionBy("tournament", "season", "gameweek")
         .parquet(os.path.join(BRONZE_DIR, "csv_misc")))
        print("✔ csv_misc → bronze")

    print("✅ Bronze ingest completed")

if __name__ == "__main__":
    main()
