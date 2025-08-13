from pyspark.sql import functions as F

def add_path_partitions(df, col="input_file_name"):
    df = df.withColumn(col, F.input_file_name())
    # expected path: .../Done/{tournament}/{season}/{gameweek}/{combo_id}/file.json
    rx = r".*/Done/([^/]+)/([^/]+)/([^/]+)/([^/]+)/[^/]+$"
    return (
        df
        .withColumn("tournament", F.regexp_extract(F.col(col), rx, 1))
        .withColumn("season",     F.regexp_extract(F.col(col), rx, 2))
        .withColumn("gameweek",   F.regexp_extract(F.col(col), rx, 3))
        .withColumn("combo_id",   F.regexp_extract(F.col(col), rx, 4))
        .drop(col)
    )
