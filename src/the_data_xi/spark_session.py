from pyspark.sql import SparkSession

def get_spark(app_name: str = "TheDataXI"):
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")                 # use all cores in Codespaces
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.shuffle.partitions", "8")   # sane default for dev
        .getOrCreate()
    )
