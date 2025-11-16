from pyspark.sql import SparkSession
from data_science.config import (
    SPARK_MASTER_HOST, DRIVER_MEMORY,
    HTTP_PROXY_HOST, HTTP_PROXY_PORT,
    HTTPS_PROXY_HOST, HTTPS_PROXY_PORT
)

spark: SparkSession = None

def getNewSparkSession(num_workers: int = 1, mem_per_worker: int = 10, cores_per_worker: int = 4):
    """
    Creates a new Spark session with dynamic configuration.
    
    Args:
        num_workers: Number of worker nodes
        mem_per_worker: Memory per worker in GB
        cores_per_worker: Number of cores per worker
    
    Returns:
        SparkSession: Configured Spark session
    """
    global spark
    
    if spark:
        spark.stop()
    
    if SPARK_MASTER_HOST.startswith("local[*]"):
        master_url = SPARK_MASTER_HOST
    else:
        master_url = f"spark://{SPARK_MASTER_HOST}:7077"
    
    builder = SparkSession.builder.appName("DEAS-Project-1").master(master_url)
    # cores_per_worker = max(1, cores_per_worker - 1)
    
    java_options = []
    if HTTP_PROXY_HOST and HTTP_PROXY_PORT:
        java_options.append(f"-Dhttp.proxyHost={HTTP_PROXY_HOST}")
        java_options.append(f"-Dhttp.proxyPort={HTTP_PROXY_PORT}")
    if HTTPS_PROXY_HOST and HTTPS_PROXY_PORT:
        java_options.append(f"-Dhttps.proxyHost={HTTPS_PROXY_HOST}")
        java_options.append(f"-Dhttps.proxyPort={HTTPS_PROXY_PORT}")
    
    extra_java_options = " ".join(java_options)

    if not master_url.startswith("local[*]"):
        builder = builder \
            .config("spark.executor.cores", str(cores_per_worker)) \
            .config("spark.executor.memory", f"{mem_per_worker}g") \
            .config("spark.executor.instances", str(num_workers)) \
            .config("spark.cores.max", str(num_workers * cores_per_worker))
    else:
        builder = builder.config("spark.executor.memory", f"{mem_per_worker}g")
    
    builder = builder \
        .config("spark.driver.memory", DRIVER_MEMORY) \
        .config("spark.driver.maxResultSize", "2g") \
        .config("spark.jars.packages", "ch.cern.sparkmeasure:spark-measure_2.13:0.27")

    if extra_java_options:
        builder = builder \
            .config("spark.driver.extraJavaOptions", extra_java_options) \
            .config("spark.executor.extraJavaOptions", extra_java_options)
    
    spark = builder.getOrCreate()
    # print("\nNew Spark session created:", spark, "\n\n")
    return spark
