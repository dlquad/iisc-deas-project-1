from pyspark.sql import SparkSession
from config import SPARK_MASTER_HOST, DRIVER_MEMORY

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
    
    print("spark before creating new session:", spark)
    if spark:
        print("Stopping existing Spark session...")
        spark.stop()
    print("Creating new Spark session...", spark)
    
    executor_memory = int(mem_per_worker * 0.8)
    
    if SPARK_MASTER_HOST.startswith("local"):
        master_url = SPARK_MASTER_HOST
    else:
        master_url = f"spark://{SPARK_MASTER_HOST}:7077"
    
    builder = SparkSession.builder.appName("DEAS-Project-1").master(master_url)
    
    if not master_url.startswith("local"):
        builder = builder \
            .config("spark.executor.instances", str(num_workers)) \
            .config("spark.executor.cores", str(cores_per_worker)) \
            .config("spark.executor.memory", f"{executor_memory}g") \
            .config("spark.cores.max", str(num_workers * cores_per_worker))
    else:
        builder = builder.config("spark.executor.memory", f"{executor_memory}g")
    
    builder = builder \
        .config("spark.driver.memory", DRIVER_MEMORY) \
        .config("spark.memory.offHeap.enabled", "true") \
        .config("spark.memory.offHeap.size", f"{int(mem_per_worker * 0.2)}g") \
        .config("spark.jars.packages", "ch.cern.sparkmeasure:spark-measure_2.13:0.27")
    
    spark = builder.getOrCreate()
    print("New Spark session created:", spark)
    return spark