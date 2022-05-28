from pyspark.sql import SparkSession

jar_dependencies = [
    "org.apache.iceberg:iceberg-spark3-runtime:0.13.1",
    "com.amazonaws:aws-java-sdk-bundle:1.11.920",
    "org.apache.hadoop:hadoop-aws:3.2.0",
    "mysql:mysql-connector-java:8.0.27"
]

packages = ",".join(jar_dependencies)
print("packages: {}".format(packages))


def get_spark_session():
    spark = SparkSession.builder \
        .appName("JDBC migration") \
        .master("local") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
        .config("spark.sql.catalog.spark_catalog.type", "hadoop") \
        .config("spark.sql.catalog.spark_catalog.warehouse", "lakehouse") \
        .config("spark.jars.packages", packages) \
        .config("spark.sql.legacy.createHiveTableByDefault", "false") \
        .config("spark.sql.sources.default", "iceberg") \
        .getOrCreate()

    # spark.sparkContext.setLogLevel("ERROR")
    return spark
