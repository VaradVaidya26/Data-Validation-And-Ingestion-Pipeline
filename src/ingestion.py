from pyspark.sql import SparkSession

def get_file_path(file_name, mount_point="/mnt/landing/"):
    return mount_point + file_name


def read_csv(spark, file_path):
    df = spark.read.csv(file_path, header=True, inferSchema=True)
    return df