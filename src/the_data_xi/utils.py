from pyspark.sql import functions as F

def add_path_partitions(df, input_col_name="__inputfile"):
    df = df.withColumn(input_col_name, F.input_file_name())
    # .../Done/{tournament}/{season}/{gameweek}/{combo_id}/filename.json
    rx = r".*[\\/](?:Done)[\\/]([^\\/]+)[\\/]([^\\/]+)[\\/]([^\\/]+)[\\/]([^\\/]+)[\\/][^\\/]+$"
    return (
        df
        .withColumn("tournament", F.regexp_extract(F.col(input_col_name), rx, 1))
        .withColumn("season",     F.regexp_extract(F.col(input_col_name), rx, 2))
        .withColumn("gameweek",   F.regexp_extract(F.col(input_col_name), rx, 3))
        .withColumn("combo_id",   F.regexp_extract(F.col(input_col_name), rx, 4))
        .drop(input_col_name)
    )
